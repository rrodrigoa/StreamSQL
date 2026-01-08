using ChronosQL.Engine;
using ChronosQL.Engine.Sql;
using StreamSql.Cli;
using StreamSql.Input;
using StreamSql.Output;
using StreamSql.Planning;
using System.Threading.Channels;
using System.Threading;
using System.Text.Json;

namespace StreamSql.Execution;

public sealed class StreamExecutionEngine
{
    public const int DefaultChannelCapacity = 8_000_000;
    private readonly ChronosQLEngine _engine;
    private readonly bool _follow;
    private readonly int _channelCapacity;

    public StreamExecutionEngine(ChronosQLEngine engine, bool follow, int channelCapacity = DefaultChannelCapacity)
    {
        _engine = engine;
        _follow = follow;
        _channelCapacity = channelCapacity;
    }

    public async Task ExecuteAsync(
        StreamGraphPlan graphPlan,
        IReadOnlyDictionary<string, InputSource> inputs,
        IReadOnlyDictionary<string, OutputDestination> outputs,
        CancellationToken cancellationToken)
    {
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var runtime = BuildRuntime(graphPlan);

        var outputStreams = new Dictionary<string, Stream>(StringComparer.OrdinalIgnoreCase);
        var outputWriters = new Dictionary<string, JsonLineWriter>(StringComparer.OrdinalIgnoreCase);
        Stream? stdinStream = null;

        try
        {
            var tasks = new List<Task>();

            foreach (var source in graphPlan.Sources)
            {
                tasks.Add(RunSourceAsync(
                    source,
                    runtime[source],
                    inputs,
                    () => stdinStream ??= StreamReaderFactory.OpenInput(source.Source),
                    linkedCts.Token));
            }

            foreach (var withNode in graphPlan.WithNodes)
            {
                tasks.Add(RunWithAsync(withNode, runtime[withNode], linkedCts.Token));
            }

            foreach (var joinNode in graphPlan.JoinNodes)
            {
                tasks.Add(RunJoinAsync(joinNode, runtime[joinNode], linkedCts.Token));
            }

            foreach (var selectNode in graphPlan.SelectNodes)
            {
                tasks.Add(RunSelectAsync(selectNode, runtime[selectNode], linkedCts.Token));
            }

            foreach (var unionNode in graphPlan.UnionNodes)
            {
                tasks.Add(RunUnionAsync(unionNode, runtime[unionNode], linkedCts.Token));
            }

            foreach (var outputNode in graphPlan.OutputNodes)
            {
                if (!outputWriters.TryGetValue(outputNode.Name, out var writer))
                {
                    var destination = outputs[outputNode.Name];
                    var stream = StreamReaderFactory.OpenOutput(destination);
                    outputStreams[outputNode.Name] = stream;
                    writer = new JsonLineWriter(stream);
                    outputWriters[outputNode.Name] = writer;
                }

                tasks.Add(RunOutputAsync(outputNode, runtime[outputNode], writer, linkedCts.Token));
            }

            await Task.WhenAll(tasks);
        }
        catch (Exception)
        {
            linkedCts.Cancel();
            throw;
        }
        finally
        {
            foreach (var node in graphPlan.Nodes)
            {
                runtime[node].Hub?.Complete();
            }

            foreach (var stream in outputStreams.Values)
            {
                await stream.FlushAsync();
                await stream.DisposeAsync();
            }

            if (stdinStream is not null)
            {
                await stdinStream.FlushAsync();
                await stdinStream.DisposeAsync();
            }
        }
    }

    private Dictionary<StreamNodePlan, NodeRuntime> BuildRuntime(StreamGraphPlan graphPlan)
    {
        var runtime = graphPlan.Nodes.ToDictionary(node => node, node => new NodeRuntime());

        foreach (var node in graphPlan.Nodes)
        {
            if (node.Downstream.Count == 0)
            {
                continue;
            }

            if (node is SourceNodePlan or WithNodePlan or JoinNodePlan)
            {
                runtime[node].Hub = new BroadcastHub<InputEvent>(_channelCapacity);
                foreach (var downstream in node.Downstream)
                {
                    if (downstream is OutputNodePlan or UnionNodePlan)
                    {
                        continue;
                    }

                    var input = runtime[node].Hub!.AddConsumer();
                    if (downstream is JoinNodePlan joinNode)
                    {
                        var joinRuntime = runtime[joinNode];
                        if (MatchesJoinInput(node.Name, joinNode.Join.LeftSource.Name))
                        {
                            joinRuntime.LeftInput = input;
                        }
                        else if (MatchesJoinInput(node.Name, joinNode.Join.RightSource.Name))
                        {
                            joinRuntime.RightInput = input;
                        }
                        else
                        {
                            throw new InvalidOperationException($"JOIN node '{joinNode.Name}' has an unknown input '{node.Name}'.");
                        }
                    }
                    else
                    {
                        runtime[downstream].Input = input;
                    }
                }
            }
        }

        var outputLookup = graphPlan.OutputNodes.ToDictionary(node => node.Name, StringComparer.OrdinalIgnoreCase);
        foreach (var outputNode in graphPlan.OutputNodes)
        {
            runtime[outputNode].Output = Channel.CreateBounded<JsonElement>(new BoundedChannelOptions(_channelCapacity)
            {
                SingleReader = true,
                SingleWriter = false,
                FullMode = BoundedChannelFullMode.Wait
            });
            runtime[outputNode].PendingWriters = outputNode.Upstreams.Count;
        }

        foreach (var unionNode in graphPlan.UnionNodes)
        {
            runtime[unionNode].JsonInput = Channel.CreateBounded<JsonElement>(new BoundedChannelOptions(_channelCapacity)
            {
                SingleReader = true,
                SingleWriter = false,
                FullMode = BoundedChannelFullMode.Wait
            });
            runtime[unionNode].PendingWriters = unionNode.Upstreams.Count;

            if (unionNode.Downstream.Count != 1 || unionNode.Downstream[0] is not OutputNodePlan unionOutput)
            {
                throw new InvalidOperationException($"UNION node '{unionNode.Name}' has an invalid downstream configuration.");
            }

            runtime[unionNode].OutputWriter = runtime[unionOutput].Output!.Writer;
            runtime[unionNode].OutputOwner = runtime[unionOutput];
        }

        foreach (var selectNode in graphPlan.SelectNodes)
        {
            if (selectNode.Downstream.Count != 1)
            {
                throw new InvalidOperationException($"SELECT {selectNode.Index} has an invalid downstream configuration.");
            }

            var downstream = selectNode.Downstream[0];
            if (downstream is OutputNodePlan outputNode)
            {
                runtime[selectNode].OutputWriter = runtime[outputNode].Output!.Writer;
                runtime[selectNode].OutputOwner = runtime[outputNode];
            }
            else if (downstream is UnionNodePlan unionNode)
            {
                runtime[selectNode].OutputWriter = runtime[unionNode].JsonInput!.Writer;
                runtime[selectNode].OutputOwner = runtime[unionNode];
            }
            else
            {
                throw new InvalidOperationException($"SELECT {selectNode.Index} has an unsupported downstream node.");
            }
        }

        return runtime;
    }

    private async Task RunSourceAsync(
        SourceNodePlan source,
        NodeRuntime runtime,
        IReadOnlyDictionary<string, InputSource> inputs,
        Func<Stream> stdinFactory,
        CancellationToken cancellationToken)
    {
        if (runtime.Hub is null)
        {
            return;
        }

        var inputSource = inputs[source.Name];
        var inputStream = inputSource.Kind == InputSourceKind.Stdin
            ? stdinFactory()
            : StreamReaderFactory.OpenInput(inputSource);

        try
        {
            var reader = new JsonLineReader(inputStream, _follow, inputSource.Path);
            await foreach (var inputEvent in reader.ReadAllAsync(cancellationToken))
            {
                await runtime.Hub.BroadcastAsync(inputEvent, cancellationToken);
            }

            runtime.Hub.Complete();
        }
        catch (Exception ex)
        {
            runtime.Hub.Complete(ex);
            throw new InvalidOperationException($"Input '{source.Name}' failed: {ex.Message}", ex);
        }
        finally
        {
            if (inputSource.Kind == InputSourceKind.File)
            {
                await inputStream.DisposeAsync();
            }
        }
    }

    private async Task RunWithAsync(WithNodePlan node, NodeRuntime runtime, CancellationToken cancellationToken)
    {
        if (runtime.Input is null || runtime.Hub is null)
        {
            return;
        }

        await using var query = _engine.CreateStreamingQuery(node.Plan);
        var lastTimestamp = 0L;
        var hasTimestamp = false;

        var outputTask = Task.Run(async () =>
        {
            await foreach (var element in query.Results.WithCancellation(cancellationToken))
            {
                var timestamp = hasTimestamp ? lastTimestamp : 0L;
                await runtime.Hub.BroadcastAsync(new InputEvent(element, timestamp), cancellationToken);
            }
        }, cancellationToken);

        try
        {
            await foreach (var inputEvent in runtime.Input.ReadAllAsync(cancellationToken))
            {
                lastTimestamp = TimestampResolver.ResolveTimestamp(
                    inputEvent.Payload,
                    inputEvent.ArrivalTime,
                    node.Plan.TimestampBy);
                hasTimestamp = true;
                await query.EnqueueAsync(inputEvent.Payload, inputEvent.ArrivalTime, cancellationToken);
            }

            query.Complete();
            await outputTask;
            runtime.Hub.Complete();
        }
        catch (Exception ex)
        {
            runtime.Hub.Complete(ex);
            throw new InvalidOperationException($"WITH '{node.Name}' failed: {ex.Message}", ex);
        }
    }

    private async Task RunJoinAsync(
        JoinNodePlan node,
        NodeRuntime runtime,
        CancellationToken cancellationToken)
    {
        if (runtime.Hub is null || runtime.LeftInput is null || runtime.RightInput is null)
        {
            return;
        }

        var joinChannel = Channel.CreateUnbounded<(JoinSide Side, InputEvent Event)>();
        var leftBuffer = new Dictionary<string, List<InputEvent>>(StringComparer.Ordinal);
        var rightBuffer = new Dictionary<string, List<InputEvent>>(StringComparer.Ordinal);
        var temporalConstraint = node.Join.TemporalConstraint;
        var leftTimestampBy = node.Join.LeftSource.TimestampBy;
        var rightTimestampBy = node.Join.RightSource.TimestampBy;
        var minDeltaMs = 0L;
        var maxDeltaMs = 0L;
        var maxSpanMs = 0L;
        var leftMaxTimestamp = 0L;
        var rightMaxTimestamp = 0L;
        var hasLeftMax = false;
        var hasRightMax = false;

        if (temporalConstraint is not null)
        {
            if (leftTimestampBy is null || rightTimestampBy is null)
            {
                throw new InvalidOperationException($"JOIN '{node.Name}' is missing TIMESTAMP BY definitions.");
            }

            var unitMs = temporalConstraint.Unit.Ticks / TimeSpan.TicksPerMillisecond;
            minDeltaMs = checked(unitMs * temporalConstraint.MinDelta);
            maxDeltaMs = checked(unitMs * temporalConstraint.MaxDelta);
            maxSpanMs = Math.Max(Math.Abs(minDeltaMs), Math.Abs(maxDeltaMs));
        }

        async Task PumpAsync(ChannelReader<InputEvent> reader, JoinSide side)
        {
            await foreach (var input in reader.ReadAllAsync(cancellationToken))
            {
                await joinChannel.Writer.WriteAsync((side, input), cancellationToken);
            }
        }

        var pumpLeft = Task.Run(() => PumpAsync(runtime.LeftInput, JoinSide.Left), cancellationToken);
        var pumpRight = Task.Run(() => PumpAsync(runtime.RightInput, JoinSide.Right), cancellationToken);

        _ = Task.WhenAll(pumpLeft, pumpRight).ContinueWith(
            _ => joinChannel.Writer.TryComplete(),
            cancellationToken);

        try
        {
            await foreach (var item in joinChannel.Reader.ReadAllAsync(cancellationToken))
            {
                var side = item.Side;
                var inputEvent = item.Event;
                var keyReference = side == JoinSide.Left ? node.Join.LeftKey : node.Join.RightKey;
                if (!TryGetJoinKey(inputEvent.Payload, keyReference, out var joinKey))
                {
                    continue;
                }

                if (temporalConstraint is not null)
                {
                    var timestampBy = side == JoinSide.Left ? leftTimestampBy : rightTimestampBy;
                    var resolvedTimestamp = TimestampResolver.ResolveTimestamp(
                        inputEvent.Payload,
                        inputEvent.ArrivalTime,
                        timestampBy);
                    inputEvent = new InputEvent(inputEvent.Payload, resolvedTimestamp);
                }

                var buffer = side == JoinSide.Left ? leftBuffer : rightBuffer;
                if (!buffer.TryGetValue(joinKey, out var events))
                {
                    events = new List<InputEvent>();
                    buffer[joinKey] = events;
                }

                events.Add(inputEvent);

                if (temporalConstraint is not null)
                {
                    if (side == JoinSide.Left)
                    {
                        leftMaxTimestamp = hasLeftMax ? Math.Max(leftMaxTimestamp, inputEvent.ArrivalTime) : inputEvent.ArrivalTime;
                        hasLeftMax = true;
                        ExpireOldEvents(leftBuffer, leftMaxTimestamp - maxSpanMs);
                    }
                    else
                    {
                        rightMaxTimestamp = hasRightMax ? Math.Max(rightMaxTimestamp, inputEvent.ArrivalTime) : inputEvent.ArrivalTime;
                        hasRightMax = true;
                        ExpireOldEvents(rightBuffer, rightMaxTimestamp - maxSpanMs);
                    }
                }

                var probe = side == JoinSide.Left ? rightBuffer : leftBuffer;
                if (!probe.TryGetValue(joinKey, out var matches))
                {
                    continue;
                }

                foreach (var match in matches)
                {
                    var leftEvent = side == JoinSide.Left ? inputEvent : match;
                    var rightEvent = side == JoinSide.Left ? match : inputEvent;
                    if (temporalConstraint is not null)
                    {
                        var deltaMs = rightEvent.ArrivalTime - leftEvent.ArrivalTime;
                        if (deltaMs < minDeltaMs || deltaMs > maxDeltaMs)
                        {
                            continue;
                        }
                    }

                    var payload = BuildJoinPayload(node.Join, leftEvent.Payload, rightEvent.Payload);
                    var timestamp = Math.Max(leftEvent.ArrivalTime, rightEvent.ArrivalTime);
                    await runtime.Hub.BroadcastAsync(new InputEvent(payload, timestamp), cancellationToken);
                }
            }

            runtime.Hub.Complete();
        }
        catch (Exception ex)
        {
            runtime.Hub.Complete(ex);
            throw new InvalidOperationException($"JOIN '{node.Name}' failed: {ex.Message}", ex);
        }
    }

    private async Task RunUnionAsync(
        UnionNodePlan node,
        NodeRuntime runtime,
        CancellationToken cancellationToken)
    {
        if (runtime.JsonInput is null || runtime.OutputWriter is null)
        {
            return;
        }

        var seen = node.Distinct ? new HashSet<string>(StringComparer.Ordinal) : null;
        Exception? outputError = null;

        try
        {
            await foreach (var element in runtime.JsonInput.Reader.ReadAllAsync(cancellationToken))
            {
                if (seen is not null)
                {
                    var key = element.GetRawText();
                    if (!seen.Add(key))
                    {
                        continue;
                    }
                }

                await runtime.OutputWriter.WriteAsync(element, cancellationToken);
            }
        }
        catch (Exception ex)
        {
            outputError = ex;
            TryCompleteOutputOwner(runtime.OutputOwner, ex);
            throw new InvalidOperationException($"UNION '{node.Name}' failed: {ex.Message}", ex);
        }
        finally
        {
            CompleteOutput(runtime.OutputOwner, outputError);
        }
    }

    private async Task RunSelectAsync(
        SelectNodePlan node,
        NodeRuntime runtime,
        CancellationToken cancellationToken)
    {
        if (runtime.Input is null || runtime.OutputWriter is null)
        {
            return;
        }

        await using var query = _engine.CreateStreamingQuery(node.Plan);

        Exception? outputError = null;
        try
        {
            var outputTask = Task.Run(async () =>
            {
                await foreach (var element in query.Results.WithCancellation(cancellationToken))
                {
                    await runtime.OutputWriter.WriteAsync(element, cancellationToken);
                }
            }, cancellationToken);

            await foreach (var inputEvent in runtime.Input.ReadAllAsync(cancellationToken))
            {
                await query.EnqueueAsync(inputEvent.Payload, inputEvent.ArrivalTime, cancellationToken);
            }

            query.Complete();
            await outputTask;
        }
        catch (Exception ex)
        {
            outputError = ex;
            TryCompleteOutputOwner(runtime.OutputOwner, ex);
            throw new InvalidOperationException($"SELECT {node.Index} failed: {ex.Message}", ex);
        }
        finally
        {
            CompleteOutput(runtime.OutputOwner, outputError);
        }
    }

    private async Task RunOutputAsync(
        OutputNodePlan node,
        NodeRuntime runtime,
        JsonLineWriter writer,
        CancellationToken cancellationToken)
    {
        if (runtime.Output is null)
        {
            return;
        }

        try
        {
            await writer.WriteAllAsync(runtime.Output.Reader.ReadAllAsync(cancellationToken), cancellationToken);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Output '{node.Name}' failed: {ex.Message}", ex);
        }
    }

    private sealed class NodeRuntime
    {
        public BroadcastHub<InputEvent>? Hub { get; set; }
        public ChannelReader<InputEvent>? Input { get; set; }
        public ChannelReader<InputEvent>? LeftInput { get; set; }
        public ChannelReader<InputEvent>? RightInput { get; set; }
        public Channel<JsonElement>? JsonInput { get; set; }
        public Channel<JsonElement>? Output { get; set; }
        public ChannelWriter<JsonElement>? OutputWriter { get; set; }
        public NodeRuntime? OutputOwner { get; set; }
        public int PendingWriters
        {
            get
            {
                return _pendingWriters;
            }

            set
            {
                _pendingWriters = value;
            }
        }

        private int _pendingWriters;
        public int DecrementPendingWriters()
        {
            return Interlocked.Decrement(ref _pendingWriters);
        }
    }

    private static void TryCompleteOutputOwner(NodeRuntime? outputOwner, Exception error)
    {
        if (outputOwner is null)
        {
            return;
        }

        if (outputOwner.Output is not null)
        {
            outputOwner.Output.Writer.TryComplete(error);
            return;
        }

        outputOwner.JsonInput?.Writer.TryComplete(error);
    }

    private static void CompleteOutput(NodeRuntime? outputOwner, Exception? error)
    {
        if (outputOwner is null)
        {
            return;
        }

        var remaining = outputOwner.DecrementPendingWriters();
        if (remaining == 0)
        {
            if (outputOwner.Output is not null)
            {
                outputOwner.Output.Writer.TryComplete(error);
                return;
            }

            outputOwner.JsonInput?.Writer.TryComplete(error);
        }
    }

    private static bool MatchesJoinInput(string upstreamName, string joinName) =>
        upstreamName.Equals(joinName, StringComparison.OrdinalIgnoreCase);

    private static void ExpireOldEvents(Dictionary<string, List<InputEvent>> buffer, long cutoffTimestamp)
    {
        var expiredKeys = new List<string>();

        foreach (var (key, events) in buffer)
        {
            events.RemoveAll(item => item.ArrivalTime < cutoffTimestamp);
            if (events.Count == 0)
            {
                expiredKeys.Add(key);
            }
        }

        foreach (var key in expiredKeys)
        {
            buffer.Remove(key);
        }
    }

    private static bool TryGetJoinKey(JsonElement payload, FieldReference key, out string joinKey)
    {
        joinKey = string.Empty;
        if (!TryGetProperty(payload, key.PathSegments, out var value))
        {
            return false;
        }

        joinKey = value.GetRawText();
        return true;
    }

    private static JsonElement BuildJoinPayload(
        JoinDefinition join,
        JsonElement leftPayload,
        JsonElement rightPayload)
    {
        var leftAlias = string.IsNullOrWhiteSpace(join.LeftSource.Alias)
            ? join.LeftSource.Name
            : join.LeftSource.Alias!;
        var rightAlias = string.IsNullOrWhiteSpace(join.RightSource.Alias)
            ? join.RightSource.Name
            : join.RightSource.Alias!;

        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream))
        {
            writer.WriteStartObject();
            writer.WritePropertyName(leftAlias);
            leftPayload.WriteTo(writer);
            writer.WritePropertyName(rightAlias);
            rightPayload.WriteTo(writer);
            writer.WriteEndObject();
        }

        stream.Position = 0;
        using var document = JsonDocument.Parse(stream);
        return document.RootElement.Clone();
    }

    private static bool TryGetProperty(JsonElement payload, IReadOnlyList<string> pathSegments, out JsonElement value)
    {
        value = payload;

        foreach (var segment in pathSegments)
        {
            if (value.ValueKind != JsonValueKind.Object || !value.TryGetProperty(segment, out var next))
            {
                return false;
            }

            value = next;
        }

        return true;
    }

    private enum JoinSide
    {
        Left,
        Right
    }
}

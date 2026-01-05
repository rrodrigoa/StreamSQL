using ChronosQL.Engine;
using StreamSql.Cli;
using StreamSql.Input;
using StreamSql.Output;
using StreamSql.Planning;
using System.Threading.Channels;
using System.Threading;

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
        IDictionary<string, InputSource> inputs,
        IDictionary<string, OutputDestination> outputs,
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

            foreach (var selectNode in graphPlan.SelectNodes)
            {
                tasks.Add(RunSelectAsync(selectNode, runtime[selectNode], linkedCts.Token));
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

            runtime[node].Hub = new BroadcastHub<InputEvent>(_channelCapacity);
            foreach (var downstream in node.Downstream)
            {
                if (downstream is OutputNodePlan)
                {
                    continue;
                }

                runtime[downstream].Input = runtime[node].Hub!.AddConsumer();
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
            runtime[outputNode].PendingWriters = outputNode.UpstreamCount;
        }

        foreach (var selectNode in graphPlan.SelectNodes)
        {
            var outputNode = outputLookup[selectNode.OutputName];
            runtime[selectNode].OutputWriter = runtime[outputNode].Output!.Writer;
            runtime[selectNode].OutputOwner = runtime[outputNode];
        }

        return runtime;
    }

    private async Task RunSourceAsync(
        SourceNodePlan source,
        NodeRuntime runtime,
        IDictionary<string, InputSource> inputs,
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
            runtime.OutputOwner?.Output?.Writer.TryComplete(ex);
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
        public Channel<JsonElement>? Output { get; set; }
        public ChannelWriter<JsonElement>? OutputWriter { get; set; }
        public NodeRuntime? OutputOwner { get; set; }
        public int PendingWriters { get; set; }
    }

    private static void CompleteOutput(NodeRuntime? outputOwner, Exception? error)
    {
        if (outputOwner is null)
        {
            return;
        }

        var remaining = Interlocked.Decrement(ref outputOwner.PendingWriters);
        if (remaining == 0)
        {
            outputOwner.Output?.Writer.TryComplete(error);
        }
    }
}

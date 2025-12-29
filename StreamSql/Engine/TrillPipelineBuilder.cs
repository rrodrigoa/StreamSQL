using Microsoft.StreamProcessing;
using StreamSql.Input;
using StreamSql.Sql;
using System.Text.Json;
using System.Reactive.Linq;

namespace StreamSql.Engine;

public sealed class TrillPipelineBuilder
{
    private const int BatchSize = 128;
    private readonly string _timestampField;
    private readonly bool _follow;
    private readonly SqlPlan _plan;
    private readonly TimeSpan? _tumblingWindow;

    public TrillPipelineBuilder(string timestampField, bool follow, SqlPlan plan, TimeSpan? tumblingWindow)
    {
        _timestampField = timestampField;
        _follow = follow;
        _plan = plan;
        _tumblingWindow = tumblingWindow;
    }

    public async IAsyncEnumerable<JsonElement> ExecuteAsync(IAsyncEnumerable<InputEvent> input, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (_tumblingWindow is not null)
        {
            await foreach (var output in ExecuteWindowedAsync(input, cancellationToken))
            {
                yield return output;
            }

            yield break;
        }

        if (_plan.Aggregate is not null)
        {
            await foreach (var output in ExecuteBatchSumAsync(input, cancellationToken))
            {
                yield return output;
            }

            yield break;
        }

        var buffer = new List<StreamEvent<JsonElement>>();
        await foreach (var inputEvent in input.WithCancellation(cancellationToken))
        {
            buffer.Add(CreateEvent(inputEvent));

            if (buffer.Count >= BatchSize)
            {
                foreach (var output in ExecuteBatch(buffer))
                {
                    yield return output;
                }

                buffer.Clear();
            }
        }

        if (buffer.Count > 0)
        {
            foreach (var output in ExecuteBatch(buffer))
            {
                yield return output;
            }
        }

        AppendInfinityPunctuation(buffer);
        foreach (var output in ExecuteBatch(buffer))
        {
            yield return output;
        }

        buffer.Clear();

        if (_follow)
        {
            // Follow mode uses batching to avoid unbounded memory. The loop ends only when input completes.
        }
    }

    private IEnumerable<JsonElement> ExecuteBatch(List<StreamEvent<JsonElement>> batch)
    {
        var streamable = batch.ToObservable().ToStreamable();
        var translated = SqlToTrillTranslator.ApplyPlan(streamable, _plan);

        foreach (var streamEvent in translated.ToEnumerable())
        {
            //if (streamEvent.IsData)
            //{
            yield return streamEvent;
            //}
        }
    }

    private StreamEvent<JsonElement> CreateEvent(InputEvent inputEvent)
    {
        var timestamp = ResolveTimestamp(inputEvent.Payload, inputEvent.ArrivalTime);
        var start = timestamp;
        var end = timestamp + 1;

        return StreamEvent.CreateInterval(start, end, inputEvent.Payload);
    }

    private async IAsyncEnumerable<JsonElement> ExecuteBatchSumAsync(
        IAsyncEnumerable<InputEvent> input,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var sum = 0.0;
        var aggregate = _plan.Aggregate!;

        await foreach (var inputEvent in input.WithCancellation(cancellationToken))
        {
            if (_plan.Filter is not null && !MatchesFilter(inputEvent.Payload, _plan.Filter))
            {
                continue;
            }

            if (!TryGetNumericValue(inputEvent.Payload, aggregate.Field.PathSegments, out var value))
            {
                continue;
            }

            sum += value;
        }

        yield return BuildSumPayload(sum, aggregate.OutputName);
    }

    private async IAsyncEnumerable<JsonElement> ExecuteWindowedAsync(
        IAsyncEnumerable<InputEvent> input,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        if (_plan.Aggregate is null)
        {
            await foreach (var output in ExecuteStreamingAsync(input, cancellationToken))
            {
                yield return output;
            }

            yield break;
        }

        var aggregate = _plan.Aggregate;
        var windowSize = (long)_tumblingWindow!.Value.TotalMilliseconds;
        long? currentWindowStart = null;
        var currentSum = 0.0;

        await foreach (var inputEvent in input.WithCancellation(cancellationToken))
        {
            if (_plan.Filter is not null && !MatchesFilter(inputEvent.Payload, _plan.Filter))
            {
                continue;
            }

            if (!TryGetNumericValue(inputEvent.Payload, aggregate.Field.PathSegments, out var value))
            {
                continue;
            }

            var timestamp = ResolveTimestamp(inputEvent.Payload, inputEvent.ArrivalTime);
            var windowStart = timestamp - (timestamp % windowSize);

            if (currentWindowStart is null)
            {
                currentWindowStart = windowStart;
            }
            else if (windowStart != currentWindowStart)
            {
                yield return BuildWindowedSumPayload(currentWindowStart.Value, currentWindowStart.Value + windowSize, currentSum, aggregate.OutputName);
                currentWindowStart = windowStart;
                currentSum = 0.0;
            }

            currentSum += value;
        }

        if (currentWindowStart is not null)
        {
            yield return BuildWindowedSumPayload(currentWindowStart.Value, currentWindowStart.Value + windowSize, currentSum, aggregate.OutputName);
        }
    }

    private async IAsyncEnumerable<JsonElement> ExecuteStreamingAsync(
        IAsyncEnumerable<InputEvent> input,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var buffer = new List<StreamEvent<JsonElement>>();
        await foreach (var inputEvent in input.WithCancellation(cancellationToken))
        {
            buffer.Add(CreateEvent(inputEvent));

            if (buffer.Count >= BatchSize)
            {
                foreach (var output in ExecuteBatch(buffer))
                {
                    yield return output;
                }

                buffer.Clear();
            }
        }

        if (buffer.Count > 0)
        {
            foreach (var output in ExecuteBatch(buffer))
            {
                yield return output;
            }
        }

        AppendInfinityPunctuation(buffer);
        foreach (var output in ExecuteBatch(buffer))
        {
            yield return output;
        }

        buffer.Clear();
    }

    private static void AppendInfinityPunctuation(List<StreamEvent<JsonElement>> buffer)
    {
        buffer.Add(StreamEvent.CreatePunctuation<JsonElement>(StreamEvent.InfinitySyncTime));
    }

    private long ResolveTimestamp(JsonElement element, long arrivalTime)
    {
        if (TryResolveTimestamp(element, _timestampField, out var resolved))
        {
            return resolved;
        }

        return arrivalTime;
    }

    private static bool TryResolveTimestamp(JsonElement element, string fieldPath, out long timestamp)
    {
        timestamp = 0;
        if (element.ValueKind != JsonValueKind.Object)
        {
            return false;
        }

        var current = element;
        var segments = fieldPath.Split('.', StringSplitOptions.RemoveEmptyEntries);
        foreach (var segment in segments)
        {
            if (current.ValueKind != JsonValueKind.Object || !current.TryGetProperty(segment, out var next))
            {
                return false;
            }

            current = next;
        }

        if (current.ValueKind == JsonValueKind.Number && current.TryGetInt64(out var numeric))
        {
            timestamp = numeric;
            return true;
        }

        if (current.ValueKind == JsonValueKind.String && DateTimeOffset.TryParse(current.GetString(), out var parsed))
        {
            timestamp = parsed.ToUnixTimeMilliseconds();
            return true;
        }

        return false;
    }

    private static bool MatchesFilter(JsonElement payload, FilterDefinition filter)
    {
        if (!TryGetProperty(payload, filter.Field.PathSegments, out var value))
        {
            return false;
        }

        if (value.ValueKind != JsonValueKind.Number || !value.TryGetDouble(out var numeric))
        {
            return false;
        }

        return filter.Operator switch
        {
            FilterOperator.GreaterThan => numeric > filter.Value,
            _ => false
        };
    }

    private static bool TryGetNumericValue(JsonElement payload, IReadOnlyList<string> pathSegments, out double value)
    {
        value = 0;

        if (!TryGetProperty(payload, pathSegments, out var property))
        {
            return false;
        }

        if (property.ValueKind != JsonValueKind.Number || !property.TryGetDouble(out var numeric))
        {
            return false;
        }

        value = numeric;
        return true;
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

    private static JsonElement BuildSumPayload(double sum, string outputName)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream))
        {
            writer.WriteStartObject();
            writer.WritePropertyName(outputName);
            writer.WriteNumberValue(sum);
            writer.WriteEndObject();
        }

        stream.Position = 0;
        using var document = JsonDocument.Parse(stream);
        return document.RootElement.Clone();
    }

    private static JsonElement BuildWindowedSumPayload(long windowStart, long windowEnd, double sum, string outputName)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream))
        {
            writer.WriteStartObject();
            writer.WritePropertyName("windowStart");
            writer.WriteNumberValue(windowStart);
            writer.WritePropertyName("windowEnd");
            writer.WriteNumberValue(windowEnd);
            writer.WritePropertyName(outputName);
            writer.WriteNumberValue(sum);
            writer.WriteEndObject();
        }

        stream.Position = 0;
        using var document = JsonDocument.Parse(stream);
        return document.RootElement.Clone();
    }
}

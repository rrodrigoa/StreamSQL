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

    public TrillPipelineBuilder(string timestampField, bool follow, SqlPlan plan)
    {
        _timestampField = timestampField;
        _follow = follow;
        _plan = plan;
    }

    public async IAsyncEnumerable<JsonElement> ExecuteAsync(IAsyncEnumerable<InputEvent> input, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
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
}

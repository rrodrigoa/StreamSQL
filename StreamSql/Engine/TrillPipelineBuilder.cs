using Microsoft.StreamProcessing;
using StreamSql.Input;
using StreamSql.Sql;
using System.Text.Json;

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

    private IEnumerable<JsonElement> ExecuteBatch(IReadOnlyCollection<StreamEvent<JsonElement>> batch)
    {
        var streamable = Streamable.Create(batch);
        var translated = SqlToTrillTranslator.ApplyPlan(streamable, _plan);

        foreach (var streamEvent in translated.ToEnumerable())
        {
            if (streamEvent.IsData)
            {
                yield return streamEvent.Payload;
            }
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
        if (element.ValueKind == JsonValueKind.Object && element.TryGetProperty(_timestampField, out var property))
        {
            if (property.ValueKind == JsonValueKind.Number && property.TryGetInt64(out var numeric))
            {
                return numeric;
            }

            if (property.ValueKind == JsonValueKind.String && DateTimeOffset.TryParse(property.GetString(), out var parsed))
            {
                return parsed.ToUnixTimeMilliseconds();
            }
        }

        return arrivalTime;
    }
}

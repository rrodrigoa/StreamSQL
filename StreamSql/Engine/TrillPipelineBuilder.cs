using Microsoft.StreamProcessing;
using StreamSql.Input;
using StreamSql.Sql;
using System.Text.Json;
using System.Reactive.Linq;
using StreamSql.Cli;

namespace StreamSql.Engine;

public sealed class TrillPipelineBuilder
{
    private const int BatchSize = 128;
    private readonly string _timestampField;
    private readonly bool _follow;
    private readonly SqlPlan _plan;
    private readonly WindowDefinition? _window;

    public TrillPipelineBuilder(string timestampField, bool follow, SqlPlan plan, WindowDefinition? window)
    {
        _timestampField = timestampField;
        _follow = follow;
        _plan = plan;
        _window = window;
    }

    public async IAsyncEnumerable<JsonElement> ExecuteAsync(IAsyncEnumerable<InputEvent> input, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (_window is not null)
        {
            await foreach (var output in ExecuteWindowedAsync(input, cancellationToken))
            {
                yield return output;
            }

            yield break;
        }

        if (_plan.SelectItems.Any(item => item.Kind == SelectItemKind.Aggregate))
        {
            await foreach (var output in ExecuteBatchAggregateAsync(input, cancellationToken))
            {
                yield return output;
            }

            yield break;
        }

        await foreach (var output in ExecuteStreamingAsync(input, cancellationToken))
        {
            yield return output;
        }
    }

    private IEnumerable<JsonElement> ExecuteBatch(List<StreamEvent<JsonElement>> batch)
    {
        var streamable = batch.ToObservable().ToStreamable(
            DisorderPolicy.Throw(),
            FlushPolicy.FlushOnPunctuation,
            PeriodicPunctuationPolicy.None());
        var translated = SqlToTrillTranslator.ApplyPlan(streamable, _plan);

        var outputArray = translated.ToStreamEventObservable().ToEnumerable();
        foreach (var streamEvent in outputArray)
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

        return StreamEvent.CreatePoint(timestamp, inputEvent.Payload);
    }

    private async IAsyncEnumerable<JsonElement> ExecuteBatchAggregateAsync(
        IAsyncEnumerable<InputEvent> input,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var aggregates = _plan.SelectItems.Where(item => item.Kind == SelectItemKind.Aggregate).Select(item => item.Aggregate!).ToList();
        var groups = new Dictionary<string, AggregateBucket>(StringComparer.Ordinal);

        await foreach (var inputEvent in input.WithCancellation(cancellationToken))
        {
            if (_plan.Filter is not null && !MatchesFilter(inputEvent.Payload, _plan.Filter))
            {
                continue;
            }

            if (!TryGetGroupValues(inputEvent.Payload, out var groupKey, out var groupValues))
            {
                continue;
            }

            if (!groups.TryGetValue(groupKey, out var bucket))
            {
                bucket = new AggregateBucket(groupValues, aggregates);
                groups.Add(groupKey, bucket);
            }

            bucket.Accumulate(inputEvent.Payload);
        }

        if (_plan.GroupBy.Count == 0 && groups.Count == 0)
        {
            groups.Add(string.Empty, new AggregateBucket(new List<JsonElement>(), aggregates));
        }

        foreach (var output in BuildAggregateOutputs(groups, includeWindow: false, windowStart: 0, windowEnd: 0))
        {
            yield return output;
        }
    }

    private async IAsyncEnumerable<JsonElement> ExecuteWindowedAsync(
        IAsyncEnumerable<InputEvent> input,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var aggregates = _plan.SelectItems.Where(item => item.Kind == SelectItemKind.Aggregate).Select(item => item.Aggregate!).ToList();
        if (aggregates.Count == 0)
        {
            await foreach (var output in ExecuteStreamingAsync(input, cancellationToken))
            {
                yield return output;
            }

            yield break;
        }

        var windowSize = (long)_window!.Size.TotalMilliseconds;
        var slideSize = (long)(_window.Slide ?? _window.Size).TotalMilliseconds;
        var windowBuckets = new Dictionary<(long WindowStart, long WindowEnd, string GroupKey), AggregateBucket>();

        await foreach (var inputEvent in input.WithCancellation(cancellationToken))
        {
            if (_plan.Filter is not null && !MatchesFilter(inputEvent.Payload, _plan.Filter))
            {
                continue;
            }

            if (!TryGetGroupValues(inputEvent.Payload, out var groupKey, out var groupValues))
            {
                continue;
            }

            var timestamp = ResolveTimestamp(inputEvent.Payload, inputEvent.ArrivalTime);
            foreach (var windowStart in GetWindowStarts(timestamp, windowSize, slideSize))
            {
                var windowEnd = windowStart + windowSize;
                var key = (windowStart, windowEnd, groupKey);
                if (!windowBuckets.TryGetValue(key, out var bucket))
                {
                    bucket = new AggregateBucket(groupValues, aggregates);
                    windowBuckets.Add(key, bucket);
                }

                bucket.Accumulate(inputEvent.Payload);
            }
        }

        foreach (var output in BuildWindowedOutputs(windowBuckets))
        {
            yield return output;
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

        AppendInfinityPunctuation(buffer);
        if (buffer.Count > 0)
        {
            foreach (var output in ExecuteBatch(buffer))
            {
                yield return output;
            }
        }

        buffer.Clear();

        if (_follow)
        {
            // Follow mode uses batching to avoid unbounded memory. The loop ends only when input completes.
        }
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
        foreach (var condition in filter.Conditions)
        {
            if (!TryGetProperty(payload, condition.Field.PathSegments, out var value))
            {
                return false;
            }

            if (condition.Value.Kind == FilterValueKind.Number)
            {
                if (value.ValueKind != JsonValueKind.Number || !value.TryGetDouble(out var numeric))
                {
                    return false;
                }

                var expected = condition.Value.Number;
                var match = condition.Operator switch
                {
                    FilterOperator.GreaterThan => numeric > expected,
                    FilterOperator.LessThan => numeric < expected,
                    FilterOperator.Equals => numeric.Equals(expected),
                    _ => false
                };

                if (!match)
                {
                    return false;
                }
            }
            else
            {
                if (value.ValueKind != JsonValueKind.True && value.ValueKind != JsonValueKind.False)
                {
                    return false;
                }

                var actual = value.ValueKind == JsonValueKind.True;
                var expected = condition.Value.Boolean;
                var match = condition.Operator == FilterOperator.Equals && actual == expected;
                if (!match)
                {
                    return false;
                }
            }
        }

        return true;
    }

    private bool TryGetGroupValues(JsonElement payload, out string groupKey, out List<JsonElement> groupValues)
    {
        groupKey = string.Empty;
        groupValues = new List<JsonElement>();
        if (_plan.GroupBy.Count == 0)
        {
            return true;
        }

        foreach (var groupField in _plan.GroupBy)
        {
            if (!TryGetProperty(payload, groupField.PathSegments, out var value))
            {
                return false;
            }

            groupValues.Add(value.Clone());
        }

        groupKey = string.Join("|", groupValues.Select(value => value.GetRawText()));
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

    private IEnumerable<JsonElement> BuildAggregateOutputs(
        Dictionary<string, AggregateBucket> buckets,
        bool includeWindow,
        long windowStart,
        long windowEnd)
    {
        foreach (var bucket in buckets.OrderBy(entry => entry.Key, StringComparer.Ordinal))
        {
            yield return BuildAggregatePayload(bucket.Value, includeWindow, windowStart, windowEnd);
        }
    }

    private IEnumerable<JsonElement> BuildWindowedOutputs(
        Dictionary<(long WindowStart, long WindowEnd, string GroupKey), AggregateBucket> buckets)
    {
        foreach (var entry in buckets
            .OrderBy(entry => entry.Key.WindowStart)
            .ThenBy(entry => entry.Key.GroupKey, StringComparer.Ordinal))
        {
            yield return BuildAggregatePayload(entry.Value, includeWindow: true, entry.Key.WindowStart, entry.Key.WindowEnd);
        }
    }

    private JsonElement BuildAggregatePayload(
        AggregateBucket bucket,
        bool includeWindow,
        long windowStart,
        long windowEnd)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream))
        {
            writer.WriteStartObject();

            if (includeWindow)
            {
                writer.WritePropertyName("windowStart");
                writer.WriteNumberValue(windowStart);
                writer.WritePropertyName("windowEnd");
                writer.WriteNumberValue(windowEnd);
            }

            foreach (var item in _plan.SelectItems)
            {
                if (item.Kind == SelectItemKind.Field)
                {
                    var index = GetGroupByIndex(item.Field!);
                    if (index < 0 || index >= bucket.GroupValues.Count)
                    {
                        continue;
                    }

                    writer.WritePropertyName(item.OutputName);
                    bucket.GroupValues[index].WriteTo(writer);
                    continue;
                }

                var aggregateIndex = bucket.AggregateOrder.IndexOf(item.Aggregate!);
                if (aggregateIndex < 0)
                {
                    continue;
                }

                writer.WritePropertyName(item.OutputName);
                bucket.Aggregates[aggregateIndex].WriteValue(writer);
            }

            writer.WriteEndObject();
        }

        stream.Position = 0;
        using var document = JsonDocument.Parse(stream);
        return document.RootElement.Clone();
    }

    private IEnumerable<long> GetWindowStarts(long timestamp, long windowSize, long slideSize)
    {
        if (_window?.Type == WindowType.Tumbling || slideSize == windowSize)
        {
            var start = timestamp - (timestamp % windowSize);
            yield return start;
            yield break;
        }

        var lastStart = timestamp - (timestamp % slideSize);
        for (var start = lastStart; start >= 0 && start + windowSize > timestamp; start -= slideSize)
        {
            yield return start;
        }
    }

    private static bool FieldEquals(FieldReference left, FieldReference right) =>
        left.PathSegments.SequenceEqual(right.PathSegments, StringComparer.OrdinalIgnoreCase);

    private int GetGroupByIndex(FieldReference field)
    {
        for (var i = 0; i < _plan.GroupBy.Count; i++)
        {
            if (FieldEquals(_plan.GroupBy[i], field))
            {
                return i;
            }
        }

        return -1;
    }

    private sealed class AggregateBucket
    {
        public AggregateBucket(List<JsonElement> groupValues, IReadOnlyList<AggregateDefinition> aggregateOrder)
        {
            GroupValues = groupValues;
            AggregateOrder = aggregateOrder.ToList();
            Aggregates = aggregateOrder.Select(definition => new AggregateAccumulator(definition)).ToList();
        }

        public List<JsonElement> GroupValues { get; }
        public List<AggregateDefinition> AggregateOrder { get; }
        public List<AggregateAccumulator> Aggregates { get; }

        public void Accumulate(JsonElement payload)
        {
            foreach (var accumulator in Aggregates)
            {
                accumulator.Accumulate(payload);
            }
        }
    }

    private sealed class AggregateAccumulator
    {
        private readonly AggregateDefinition _definition;
        private double _sum;
        private double _min;
        private double _max;
        private long _count;
        private bool _hasValue;

        public AggregateAccumulator(AggregateDefinition definition)
        {
            _definition = definition;
        }

        public void Accumulate(JsonElement payload)
        {
            if (_definition.Type == AggregateType.Count)
            {
                if (_definition.CountAll)
                {
                    _count++;
                    return;
                }

                if (_definition.Field is null)
                {
                    return;
                }

                if (TryGetProperty(payload, _definition.Field.PathSegments, out var property) &&
                    property.ValueKind != JsonValueKind.Null)
                {
                    _count++;
                }

                return;
            }

            if (_definition.Field is null)
            {
                return;
            }

            if (!TryGetNumericValue(payload, _definition.Field.PathSegments, out var numeric))
            {
                return;
            }

            _count++;
            _sum += numeric;
            if (!_hasValue)
            {
                _min = numeric;
                _max = numeric;
                _hasValue = true;
            }
            else
            {
                _min = Math.Min(_min, numeric);
                _max = Math.Max(_max, numeric);
            }
        }

        public void WriteValue(Utf8JsonWriter writer)
        {
            switch (_definition.Type)
            {
                case AggregateType.Count:
                    writer.WriteNumberValue(_count);
                    return;
                case AggregateType.Sum:
                    writer.WriteNumberValue(_sum);
                    return;
                case AggregateType.Avg:
                    if (_count == 0)
                    {
                        writer.WriteNullValue();
                    }
                    else
                    {
                        writer.WriteNumberValue(_sum / _count);
                    }
                    return;
                case AggregateType.Min:
                    if (!_hasValue)
                    {
                        writer.WriteNullValue();
                    }
                    else
                    {
                        writer.WriteNumberValue(_min);
                    }
                    return;
                case AggregateType.Max:
                    if (!_hasValue)
                    {
                        writer.WriteNullValue();
                    }
                    else
                    {
                        writer.WriteNumberValue(_max);
                    }
                    return;
            }
        }
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
}

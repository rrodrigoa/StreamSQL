using Microsoft.StreamProcessing;
using System.Reactive.Linq;
using System.Text.Json;
using ChronosQL.Engine.Sql;

namespace ChronosQL.Engine;

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

        if (_plan.Aggregates.Count > 0)
        {
            await foreach (var output in ExecuteBatchAggregateAsync(input, cancellationToken))
            {
                yield return output;
            }

            yield break;
        }

        if (_plan.OrderBy.Count > 0)
        {
            await foreach (var output in ExecuteOrderedProjectionAsync(input, cancellationToken))
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
        var aggregates = _plan.Aggregates.ToList();
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

        var outputs = BuildAggregateOutputs(groups, includeWindow: false, windowStart: 0, windowEnd: 0);
        foreach (var output in OrderOutputs(outputs))
        {
            yield return output;
        }
    }

    private async IAsyncEnumerable<JsonElement> ExecuteWindowedAsync(
        IAsyncEnumerable<InputEvent> input,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var aggregates = _plan.Aggregates.ToList();
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
        if (_window.Type == WindowType.Sliding)
        {
            await foreach (var output in ExecuteSlidingWindowAsync(input, windowSize, cancellationToken))
            {
                yield return output;
            }

            yield break;
        }

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

    private async IAsyncEnumerable<JsonElement> ExecuteSlidingWindowAsync(
        IAsyncEnumerable<InputEvent> input,
        long windowSize,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var aggregates = _plan.Aggregates.ToList();
        var events = new List<(long Timestamp, string GroupKey, List<JsonElement> GroupValues, JsonElement Payload)>();

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
            events.Add((timestamp, groupKey, groupValues, inputEvent.Payload));
        }

        var windowBuckets = new List<(long WindowStart, long WindowEnd, string GroupKey, AggregateBucket Bucket)>();
        foreach (var group in events.GroupBy(item => item.GroupKey))
        {
            var groupEvents = group.OrderBy(item => item.Timestamp).ToList();
            var groupValues = groupEvents.FirstOrDefault().GroupValues ?? new List<JsonElement>();
            var timestamps = groupEvents.Select(item => item.Timestamp).Distinct().ToList();

            foreach (var windowEnd in timestamps)
            {
                var windowStart = windowEnd - windowSize;
                var bucket = new AggregateBucket(groupValues, aggregates);
                foreach (var item in groupEvents)
                {
                    if (item.Timestamp < windowStart || item.Timestamp > windowEnd)
                    {
                        continue;
                    }

                    bucket.Accumulate(item.Payload);
                }

                if (bucket.Aggregates.Count == 0 && _plan.GroupBy.Count == 0)
                {
                    continue;
                }

                windowBuckets.Add((windowStart, windowEnd, group.Key, bucket));
            }
        }

        var outputs = BuildWindowedOutputs(windowBuckets);
        foreach (var output in OrderOutputs(outputs))
        {
            yield return output;
        }
    }

    private async IAsyncEnumerable<JsonElement> ExecuteOrderedProjectionAsync(
        IAsyncEnumerable<InputEvent> input,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var outputs = new List<JsonElement>();
        await foreach (var output in ExecuteStreamingAsync(input, cancellationToken))
        {
            outputs.Add(output);
        }

        foreach (var output in OrderOutputs(outputs))
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

            if (condition.Value.Kind != FilterValueKind.Number)
            {
                return false;
            }

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
            if (!MatchesHaving(bucket.Value))
            {
                continue;
            }

            yield return BuildAggregatePayload(bucket.Value, includeWindow, windowStart, windowEnd);
        }
    }

    private IEnumerable<JsonElement> BuildWindowedOutputs(
        Dictionary<(long WindowStart, long WindowEnd, string GroupKey), AggregateBucket> buckets)
    {
        var outputs = buckets
            .OrderBy(entry => entry.Key.WindowStart)
            .ThenBy(entry => entry.Key.GroupKey, StringComparer.Ordinal)
            .Where(entry => MatchesHaving(entry.Value))
            .Select(entry => BuildAggregatePayload(entry.Value, includeWindow: true, entry.Key.WindowStart, entry.Key.WindowEnd))
            .ToList();

        foreach (var output in OrderOutputs(outputs))
        {
            yield return output;
        }
    }

    private IEnumerable<JsonElement> BuildWindowedOutputs(
        List<(long WindowStart, long WindowEnd, string GroupKey, AggregateBucket Bucket)> buckets)
    {
        var outputs = buckets
            .OrderBy(entry => entry.WindowStart)
            .ThenBy(entry => entry.GroupKey, StringComparer.Ordinal)
            .Where(entry => MatchesHaving(entry.Bucket))
            .Select(entry => BuildAggregatePayload(entry.Bucket, includeWindow: true, entry.WindowStart, entry.WindowEnd))
            .ToList();

        return outputs;
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

        public FilterValue GetFilterValue()
        {
            switch (_definition.Type)
            {
                case AggregateType.Count:
                    return new FilterValue(FilterValueKind.Number, _count, string.Empty);
                case AggregateType.Sum:
                    return new FilterValue(FilterValueKind.Number, _sum, string.Empty);
                case AggregateType.Avg:
                    if (_count == 0)
                    {
                        return new FilterValue(FilterValueKind.Null, 0, string.Empty);
                    }
                    return new FilterValue(FilterValueKind.Number, _sum / _count, string.Empty);
                case AggregateType.Min:
                    if (!_hasValue)
                    {
                        return new FilterValue(FilterValueKind.Null, 0, string.Empty);
                    }
                    return new FilterValue(FilterValueKind.Number, _min, string.Empty);
                case AggregateType.Max:
                    if (!_hasValue)
                    {
                        return new FilterValue(FilterValueKind.Null, 0, string.Empty);
                    }
                    return new FilterValue(FilterValueKind.Number, _max, string.Empty);
            }

            return new FilterValue(FilterValueKind.Null, 0, string.Empty);
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

    private IEnumerable<JsonElement> OrderOutputs(IEnumerable<JsonElement> outputs)
    {
        if (_plan.OrderBy.Count == 0)
        {
            return outputs;
        }

        var ordered = outputs.ToList();
        ordered.Sort(new JsonElementOrderComparer(_plan.OrderBy));
        return ordered;
    }

    private sealed class JsonElementOrderComparer : IComparer<JsonElement>
    {
        private readonly IReadOnlyList<OrderByDefinition> _orderBy;

        public JsonElementOrderComparer(IReadOnlyList<OrderByDefinition> orderBy)
        {
            _orderBy = orderBy;
        }

        public int Compare(JsonElement left, JsonElement right)
        {
            foreach (var orderBy in _orderBy)
            {
                var leftValue = TryGetOrderValue(left, orderBy.OutputName, out var leftElement)
                    ? leftElement
                    : default;
                var rightValue = TryGetOrderValue(right, orderBy.OutputName, out var rightElement)
                    ? rightElement
                    : default;

                var comparison = CompareJsonValues(leftValue, rightValue);
                if (comparison == 0)
                {
                    continue;
                }

                return orderBy.Direction == SortDirection.Descending ? -comparison : comparison;
            }

            return 0;
        }

        private static int CompareJsonValues(JsonElement left, JsonElement right)
        {
            var leftNull = left.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined;
            var rightNull = right.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined;

            if (leftNull && rightNull)
            {
                return 0;
            }

            if (leftNull)
            {
                return -1;
            }

            if (rightNull)
            {
                return 1;
            }

            if (left.ValueKind == JsonValueKind.Number && right.ValueKind == JsonValueKind.Number)
            {
                left.TryGetDouble(out var leftNumber);
                right.TryGetDouble(out var rightNumber);
                return leftNumber.CompareTo(rightNumber);
            }

            if (left.ValueKind == JsonValueKind.String && right.ValueKind == JsonValueKind.String)
            {
                return string.CompareOrdinal(left.GetString(), right.GetString());
            }

            if (left.ValueKind is JsonValueKind.True or JsonValueKind.False &&
                right.ValueKind is JsonValueKind.True or JsonValueKind.False)
            {
                var leftBool = left.ValueKind == JsonValueKind.True;
                var rightBool = right.ValueKind == JsonValueKind.True;
                return leftBool.CompareTo(rightBool);
            }

            return GetSortRank(left.ValueKind).CompareTo(GetSortRank(right.ValueKind));
        }

        private static bool TryGetOrderValue(JsonElement element, string propertyName, out JsonElement value)
        {
            value = default;
            if (element.ValueKind != JsonValueKind.Object)
            {
                return false;
            }

            if (element.TryGetProperty(propertyName, out value))
            {
                return true;
            }

            foreach (var property in element.EnumerateObject())
            {
                if (property.NameEquals(propertyName) ||
                    property.Name.Equals(propertyName, StringComparison.OrdinalIgnoreCase))
                {
                    value = property.Value;
                    return true;
                }
        }

        return false;
    }

    private bool MatchesHaving(AggregateBucket bucket)
    {
        if (_plan.Having is null)
        {
            return true;
        }

        foreach (var condition in _plan.Having.Conditions)
        {
            if (!TryGetHavingValue(bucket, condition.Operand, out var actual))
            {
                return false;
            }

            if (!MatchesHavingCondition(actual, condition.Operator, condition.Value))
            {
                return false;
            }
        }

        return true;
    }

    private bool TryGetHavingValue(AggregateBucket bucket, HavingOperand operand, out FilterValue value)
    {
        value = new FilterValue(FilterValueKind.Null, 0, string.Empty);
        switch (operand.Kind)
        {
            case HavingOperandKind.GroupField:
                if (operand.Field is null)
                {
                    return false;
                }

                var index = GetGroupByIndex(operand.Field);
                if (index < 0 || index >= bucket.GroupValues.Count)
                {
                    return false;
                }

                return TryConvertJsonValue(bucket.GroupValues[index], out value);
            case HavingOperandKind.Aggregate:
                if (operand.Aggregate is null)
                {
                    return false;
                }

                var aggregateIndex = bucket.AggregateOrder.IndexOf(operand.Aggregate);
                if (aggregateIndex < 0 || aggregateIndex >= bucket.Aggregates.Count)
                {
                    return false;
                }

                value = bucket.Aggregates[aggregateIndex].GetFilterValue();
                return true;
            default:
                return false;
        }
    }

    private static bool TryConvertJsonValue(JsonElement element, out FilterValue value)
    {
        value = new FilterValue(FilterValueKind.Null, 0, string.Empty);
        switch (element.ValueKind)
        {
            case JsonValueKind.Number when element.TryGetDouble(out var numeric):
                value = new FilterValue(FilterValueKind.Number, numeric, string.Empty);
                return true;
            case JsonValueKind.String:
                value = new FilterValue(FilterValueKind.String, 0, element.GetString() ?? string.Empty);
                return true;
            case JsonValueKind.Null:
                value = new FilterValue(FilterValueKind.Null, 0, string.Empty);
                return true;
            default:
                return false;
        }
    }

    private static bool MatchesHavingCondition(FilterValue actual, FilterOperator filterOperator, FilterValue expected)
    {
        switch (filterOperator)
        {
            case FilterOperator.GreaterThan:
                return actual.Kind == FilterValueKind.Number
                    && expected.Kind == FilterValueKind.Number
                    && actual.Number > expected.Number;
            case FilterOperator.LessThan:
                return actual.Kind == FilterValueKind.Number
                    && expected.Kind == FilterValueKind.Number
                    && actual.Number < expected.Number;
            case FilterOperator.Equals:
                if (actual.Kind == FilterValueKind.Null || expected.Kind == FilterValueKind.Null)
                {
                    return actual.Kind == FilterValueKind.Null && expected.Kind == FilterValueKind.Null;
                }

                if (actual.Kind == FilterValueKind.Number && expected.Kind == FilterValueKind.Number)
                {
                    return actual.Number.Equals(expected.Number);
                }

                if (actual.Kind == FilterValueKind.String && expected.Kind == FilterValueKind.String)
                {
                    return string.Equals(actual.String, expected.String, StringComparison.Ordinal);
                }

                return false;
            default:
                return false;
        }
    }

        private static int GetSortRank(JsonValueKind kind) =>
            kind switch
            {
                JsonValueKind.Null => 0,
                JsonValueKind.False => 1,
                JsonValueKind.True => 2,
                JsonValueKind.Number => 3,
                JsonValueKind.String => 4,
                JsonValueKind.Object => 5,
                JsonValueKind.Array => 6,
                _ => 7
            };
    }
}

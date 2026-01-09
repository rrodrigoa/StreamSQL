using System.Text.Json;
using ChronosQL.Engine;

namespace ChronosQL.Engine.Compilation;

public static class QueryRuntime
{
    private const int BatchSize = 128;

    public static async IAsyncEnumerable<JsonElement> ExecuteAsync(
        CompiledQueryDefinition definition,
        IAsyncEnumerable<InputEvent> input,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (definition.Window is not null)
        {
            await foreach (var output in ExecuteWindowedAsync(definition, input, cancellationToken))
            {
                yield return output;
            }

            yield break;
        }

        if (definition.Aggregates.Length > 0)
        {
            await foreach (var output in ExecuteBatchAggregateAsync(definition, input, cancellationToken))
            {
                yield return output;
            }

            yield break;
        }

        if (definition.OrderBy.Length > 0)
        {
            await foreach (var output in ExecuteOrderedProjectionAsync(definition, input, cancellationToken))
            {
                yield return output;
            }

            yield break;
        }

        await foreach (var output in ExecuteStreamingAsync(definition, input, cancellationToken))
        {
            yield return output;
        }
    }

    private static async IAsyncEnumerable<JsonElement> ExecuteBatchAggregateAsync(
        CompiledQueryDefinition definition,
        IAsyncEnumerable<InputEvent> input,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var aggregates = definition.Aggregates.ToList();
        var groups = new Dictionary<string, CompiledAggregateBucket>(StringComparer.Ordinal);

        await foreach (var inputEvent in input.WithCancellation(cancellationToken))
        {
            if (definition.Filter is not null && !definition.Filter.Matches(inputEvent.Payload))
            {
                continue;
            }

            if (!TryGetGroupValues(definition, inputEvent.Payload, out var groupKey, out var groupValues))
            {
                continue;
            }

            if (!groups.TryGetValue(groupKey, out var bucket))
            {
                bucket = new CompiledAggregateBucket(groupValues, aggregates);
                groups.Add(groupKey, bucket);
            }

            bucket.Accumulate(inputEvent.Payload);
        }

        if ((definition.GroupBy?.Fields.Length ?? 0) == 0 && groups.Count == 0)
        {
            groups.Add(string.Empty, new CompiledAggregateBucket(new List<JsonElement>(), aggregates));
        }

        var outputs = BuildAggregateOutputs(definition, groups, includeWindow: false, windowStart: 0, windowEnd: 0);
        foreach (var output in OrderOutputs(definition, outputs))
        {
            yield return output;
        }
    }

    private static async IAsyncEnumerable<JsonElement> ExecuteWindowedAsync(
        CompiledQueryDefinition definition,
        IAsyncEnumerable<InputEvent> input,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        if (definition.Aggregates.Length == 0)
        {
            await foreach (var output in ExecuteStreamingAsync(definition, input, cancellationToken))
            {
                yield return output;
            }

            yield break;
        }

        var window = definition.Window!;
        if (window.Type == CompiledWindowType.Sliding)
        {
            await foreach (var output in ExecuteSlidingWindowAsync(definition, input, window.SizeMs, cancellationToken))
            {
                yield return output;
            }

            yield break;
        }

        var windowBuckets = new Dictionary<(long WindowStart, long WindowEnd, string GroupKey), CompiledAggregateBucket>();

        await foreach (var inputEvent in input.WithCancellation(cancellationToken))
        {
            if (definition.Filter is not null && !definition.Filter.Matches(inputEvent.Payload))
            {
                continue;
            }

            if (!TryGetGroupValues(definition, inputEvent.Payload, out var groupKey, out var groupValues))
            {
                continue;
            }

            var timestamp = ResolveTimestamp(definition, inputEvent);
            foreach (var windowStart in GetWindowStarts(window, timestamp))
            {
                var windowEnd = windowStart + window.SizeMs;
                var key = (windowStart, windowEnd, groupKey);
                if (!windowBuckets.TryGetValue(key, out var bucket))
                {
                    bucket = new CompiledAggregateBucket(groupValues, definition.Aggregates);
                    windowBuckets.Add(key, bucket);
                }

                bucket.Accumulate(inputEvent.Payload);
            }
        }

        foreach (var output in BuildWindowedOutputs(definition, windowBuckets))
        {
            yield return output;
        }
    }

    private static async IAsyncEnumerable<JsonElement> ExecuteSlidingWindowAsync(
        CompiledQueryDefinition definition,
        IAsyncEnumerable<InputEvent> input,
        long windowSize,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var aggregates = definition.Aggregates.ToList();
        var events = new List<(long Timestamp, string GroupKey, List<JsonElement> GroupValues, JsonElement Payload)>();

        await foreach (var inputEvent in input.WithCancellation(cancellationToken))
        {
            if (definition.Filter is not null && !definition.Filter.Matches(inputEvent.Payload))
            {
                continue;
            }

            if (!TryGetGroupValues(definition, inputEvent.Payload, out var groupKey, out var groupValues))
            {
                continue;
            }

            var timestamp = ResolveTimestamp(definition, inputEvent);
            events.Add((timestamp, groupKey, groupValues, inputEvent.Payload));
        }

        var windowBuckets = new List<(long WindowStart, long WindowEnd, string GroupKey, CompiledAggregateBucket Bucket)>();
        foreach (var group in events.GroupBy(item => item.GroupKey))
        {
            var groupEvents = group.OrderBy(item => item.Timestamp).ToList();
            var groupValues = groupEvents.FirstOrDefault().GroupValues ?? new List<JsonElement>();
            var timestamps = groupEvents.Select(item => item.Timestamp).Distinct().ToList();

            foreach (var windowEnd in timestamps)
            {
                var windowStart = windowEnd - windowSize;
                var bucket = new CompiledAggregateBucket(groupValues, aggregates);

                foreach (var entry in groupEvents.Where(item => item.Timestamp > windowStart && item.Timestamp <= windowEnd))
                {
                    bucket.Accumulate(entry.Payload);
                }

                windowBuckets.Add((windowStart, windowEnd, group.Key, bucket));
            }
        }

        foreach (var output in OrderOutputs(definition, BuildWindowedOutputs(definition, windowBuckets)))
        {
            yield return output;
        }
    }

    private static async IAsyncEnumerable<JsonElement> ExecuteOrderedProjectionAsync(
        CompiledQueryDefinition definition,
        IAsyncEnumerable<InputEvent> input,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var outputs = new List<JsonElement>();
        await foreach (var output in ExecuteStreamingAsync(definition, input, cancellationToken))
        {
            outputs.Add(output);
        }

        foreach (var output in OrderOutputs(definition, outputs))
        {
            yield return output;
        }
    }

    private static async IAsyncEnumerable<JsonElement> ExecuteStreamingAsync(
        CompiledQueryDefinition definition,
        IAsyncEnumerable<InputEvent> input,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var inputEvent in input.WithCancellation(cancellationToken))
        {
            if (definition.Filter is not null && !definition.Filter.Matches(inputEvent.Payload))
            {
                continue;
            }

            if (definition.Projection is null)
            {
                yield return inputEvent.Payload;
                continue;
            }

            yield return ProjectPayload(definition.Projection, inputEvent.Payload);
        }

        if (definition.Follow)
        {
            // Follow mode uses batching to avoid unbounded memory. The loop ends only when input completes.
        }
    }

    private static long ResolveTimestamp(CompiledQueryDefinition definition, InputEvent inputEvent)
    {
        if (definition.TimestampBy is null)
        {
            return inputEvent.ArrivalTime;
        }

        return definition.TimestampBy.Resolve(inputEvent.Payload, inputEvent.ArrivalTime);
    }

    private static JsonElement ProjectPayload(CompiledProjectionDefinition projection, JsonElement payload)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream))
        {
            writer.WriteStartObject();

            foreach (var field in projection.Fields)
            {
                if (!field.Getter(payload, out var value))
                {
                    continue;
                }

                writer.WritePropertyName(field.OutputName);
                value.WriteTo(writer);
            }

            writer.WriteEndObject();
        }

        stream.Position = 0;
        using var document = JsonDocument.Parse(stream);
        return document.RootElement.Clone();
    }

    private static bool TryGetGroupValues(
        CompiledQueryDefinition definition,
        JsonElement payload,
        out string groupKey,
        out List<JsonElement> groupValues)
    {
        groupKey = string.Empty;
        groupValues = new List<JsonElement>();
        if (definition.GroupBy is null || definition.GroupBy.Fields.Length == 0)
        {
            return true;
        }

        foreach (var groupField in definition.GroupBy.Fields)
        {
            if (!groupField(payload, out var value))
            {
                return false;
            }

            groupValues.Add(value.Clone());
        }

        groupKey = string.Join("|", groupValues.Select(value => value.GetRawText()));
        return true;
    }

    private static IEnumerable<JsonElement> BuildAggregateOutputs(
        CompiledQueryDefinition definition,
        Dictionary<string, CompiledAggregateBucket> buckets,
        bool includeWindow,
        long windowStart,
        long windowEnd)
    {
        foreach (var bucket in buckets.OrderBy(entry => entry.Key, StringComparer.Ordinal))
        {
            if (!MatchesHaving(definition, bucket.Value))
            {
                continue;
            }

            yield return BuildAggregatePayload(definition, bucket.Value, includeWindow, windowStart, windowEnd);
        }
    }

    private static IEnumerable<JsonElement> BuildWindowedOutputs(
        CompiledQueryDefinition definition,
        Dictionary<(long WindowStart, long WindowEnd, string GroupKey), CompiledAggregateBucket> buckets)
    {
        var outputs = buckets
            .OrderBy(entry => entry.Key.WindowStart)
            .ThenBy(entry => entry.Key.GroupKey, StringComparer.Ordinal)
            .Where(entry => MatchesHaving(definition, entry.Value))
            .Select(entry => BuildAggregatePayload(definition, entry.Value, includeWindow: true, entry.Key.WindowStart, entry.Key.WindowEnd))
            .ToList();

        foreach (var output in OrderOutputs(definition, outputs))
        {
            yield return output;
        }
    }

    private static IEnumerable<JsonElement> BuildWindowedOutputs(
        CompiledQueryDefinition definition,
        List<(long WindowStart, long WindowEnd, string GroupKey, CompiledAggregateBucket Bucket)> buckets)
    {
        var outputs = buckets
            .OrderBy(entry => entry.WindowStart)
            .ThenBy(entry => entry.GroupKey, StringComparer.Ordinal)
            .Where(entry => MatchesHaving(definition, entry.Bucket))
            .Select(entry => BuildAggregatePayload(definition, entry.Bucket, includeWindow: true, entry.WindowStart, entry.WindowEnd))
            .ToList();

        return outputs;
    }

    private static JsonElement BuildAggregatePayload(
        CompiledQueryDefinition definition,
        CompiledAggregateBucket bucket,
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

            foreach (var item in definition.SelectItems)
            {
                if (item.Kind == CompiledSelectItemKind.Field)
                {
                    var index = item.GroupIndex;
                    if (index < 0 || index >= bucket.GroupValues.Count)
                    {
                        continue;
                    }

                    writer.WritePropertyName(item.OutputName);
                    bucket.GroupValues[index].WriteTo(writer);
                    continue;
                }

                var aggregateIndex = item.AggregateIndex;
                if (aggregateIndex < 0 || aggregateIndex >= bucket.Aggregates.Count)
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

    private static IEnumerable<long> GetWindowStarts(CompiledWindowDefinition window, long timestamp)
    {
        if (window.Type == CompiledWindowType.Tumbling || window.SlideMs == window.SizeMs)
        {
            var start = timestamp - (timestamp % window.SizeMs);
            yield return start;
            yield break;
        }

        var lastStart = timestamp - (timestamp % window.SlideMs);
        for (var start = lastStart; start >= 0 && start + window.SizeMs > timestamp; start -= window.SlideMs)
        {
            yield return start;
        }
    }

    private static IEnumerable<JsonElement> OrderOutputs(CompiledQueryDefinition definition, IEnumerable<JsonElement> outputs)
    {
        if (definition.OrderBy.Length == 0)
        {
            return outputs;
        }

        var ordered = outputs.ToList();
        ordered.Sort(new JsonElementOrderComparer(definition.OrderBy));
        return ordered;
    }

    private static bool MatchesHaving(CompiledQueryDefinition definition, CompiledAggregateBucket bucket)
    {
        if (definition.Having is null)
        {
            return true;
        }

        foreach (var condition in definition.Having.Conditions)
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

    private static bool TryGetHavingValue(CompiledAggregateBucket bucket, CompiledHavingOperand operand, out CompiledFilterValue value)
    {
        value = new CompiledFilterValue(CompiledFilterValueKind.Null, 0, string.Empty);
        switch (operand.Kind)
        {
            case CompiledHavingOperandKind.GroupField:
                var groupIndex = operand.Index;
                if (groupIndex < 0 || groupIndex >= bucket.GroupValues.Count)
                {
                    return false;
                }

                return TryConvertJsonValue(bucket.GroupValues[groupIndex], out value);
            case CompiledHavingOperandKind.Aggregate:
                var aggregateIndex = operand.Index;
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

    private static bool TryConvertJsonValue(JsonElement element, out CompiledFilterValue value)
    {
        value = new CompiledFilterValue(CompiledFilterValueKind.Null, 0, string.Empty);
        switch (element.ValueKind)
        {
            case JsonValueKind.Number when element.TryGetDouble(out var numeric):
                value = new CompiledFilterValue(CompiledFilterValueKind.Number, numeric, string.Empty);
                return true;
            case JsonValueKind.String:
                value = new CompiledFilterValue(CompiledFilterValueKind.String, 0, element.GetString() ?? string.Empty);
                return true;
            case JsonValueKind.Null:
                value = new CompiledFilterValue(CompiledFilterValueKind.Null, 0, string.Empty);
                return true;
            default:
                return false;
        }
    }

    private static bool MatchesHavingCondition(CompiledFilterValue actual, CompiledFilterOperator filterOperator, CompiledFilterValue expected)
    {
        switch (filterOperator)
        {
            case CompiledFilterOperator.GreaterThan:
                return actual.Kind == CompiledFilterValueKind.Number
                    && expected.Kind == CompiledFilterValueKind.Number
                    && actual.Number > expected.Number;
            case CompiledFilterOperator.LessThan:
                return actual.Kind == CompiledFilterValueKind.Number
                    && expected.Kind == CompiledFilterValueKind.Number
                    && actual.Number < expected.Number;
            case CompiledFilterOperator.Equals:
                if (actual.Kind == CompiledFilterValueKind.Null || expected.Kind == CompiledFilterValueKind.Null)
                {
                    return actual.Kind == CompiledFilterValueKind.Null && expected.Kind == CompiledFilterValueKind.Null;
                }

                if (actual.Kind == CompiledFilterValueKind.Number && expected.Kind == CompiledFilterValueKind.Number)
                {
                    return actual.Number.Equals(expected.Number);
                }

                if (actual.Kind == CompiledFilterValueKind.String && expected.Kind == CompiledFilterValueKind.String)
                {
                    return string.Equals(actual.String, expected.String, StringComparison.Ordinal);
                }

                return false;
            default:
                return false;
        }
    }
}

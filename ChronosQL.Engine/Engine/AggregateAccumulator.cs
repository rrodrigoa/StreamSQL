using System;
using System.Text.Json;
using ChronosQL.Engine.Sql;

namespace ChronosQL.Engine;

internal sealed class AggregateAccumulator
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

            if (TrillPipelineBuilder.TryGetProperty(payload, _definition.Field.PathSegments, out var property) &&
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

        if (!TrillPipelineBuilder.TryGetNumericValue(payload, _definition.Field.PathSegments, out var numeric))
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

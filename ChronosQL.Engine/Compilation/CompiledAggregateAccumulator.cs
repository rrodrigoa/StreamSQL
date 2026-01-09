using System.Text.Json;

namespace ChronosQL.Engine.Compilation;

internal sealed class CompiledAggregateAccumulator
{
    private readonly CompiledAggregateDefinition _definition;
    private double _sum;
    private double _min;
    private double _max;
    private long _count;
    private bool _hasValue;

    public CompiledAggregateAccumulator(CompiledAggregateDefinition definition)
    {
        _definition = definition;
    }

    public void Accumulate(JsonElement payload)
    {
        if (_definition.Type == CompiledAggregateType.Count)
        {
            if (_definition.CountAll)
            {
                _count++;
                return;
            }

            if (_definition.HasValue is null)
            {
                return;
            }

            if (_definition.HasValue(payload))
            {
                _count++;
            }

            return;
        }

        if (_definition.NumericGetter is null)
        {
            return;
        }

        if (!_definition.NumericGetter(payload, out var numeric))
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
            case CompiledAggregateType.Count:
                writer.WriteNumberValue(_count);
                return;
            case CompiledAggregateType.Sum:
                writer.WriteNumberValue(_sum);
                return;
            case CompiledAggregateType.Avg:
                if (_count == 0)
                {
                    writer.WriteNullValue();
                }
                else
                {
                    writer.WriteNumberValue(_sum / _count);
                }
                return;
            case CompiledAggregateType.Min:
                if (!_hasValue)
                {
                    writer.WriteNullValue();
                }
                else
                {
                    writer.WriteNumberValue(_min);
                }
                return;
            case CompiledAggregateType.Max:
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

    public CompiledFilterValue GetFilterValue()
    {
        switch (_definition.Type)
        {
            case CompiledAggregateType.Count:
                return new CompiledFilterValue(CompiledFilterValueKind.Number, _count, string.Empty);
            case CompiledAggregateType.Sum:
                return new CompiledFilterValue(CompiledFilterValueKind.Number, _sum, string.Empty);
            case CompiledAggregateType.Avg:
                if (_count == 0)
                {
                    return new CompiledFilterValue(CompiledFilterValueKind.Null, 0, string.Empty);
                }

                return new CompiledFilterValue(CompiledFilterValueKind.Number, _sum / _count, string.Empty);
            case CompiledAggregateType.Min:
                if (!_hasValue)
                {
                    return new CompiledFilterValue(CompiledFilterValueKind.Null, 0, string.Empty);
                }

                return new CompiledFilterValue(CompiledFilterValueKind.Number, _min, string.Empty);
            case CompiledAggregateType.Max:
                if (!_hasValue)
                {
                    return new CompiledFilterValue(CompiledFilterValueKind.Null, 0, string.Empty);
                }

                return new CompiledFilterValue(CompiledFilterValueKind.Number, _max, string.Empty);
        }

        return new CompiledFilterValue(CompiledFilterValueKind.Null, 0, string.Empty);
    }
}

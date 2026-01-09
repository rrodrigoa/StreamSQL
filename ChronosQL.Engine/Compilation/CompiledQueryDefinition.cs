using System.Text.Json;

namespace ChronosQL.Engine.Compilation;

public delegate bool TryGetJsonValueDelegate(JsonElement payload, out JsonElement value);
public delegate bool TryGetNumericValueDelegate(JsonElement payload, out double value);
public delegate bool HasValueDelegate(JsonElement payload);
public delegate long ResolveTimestampDelegate(JsonElement payload, long arrivalTime);

public sealed record CompiledQueryDefinition(
    CompiledProjectionDefinition? Projection,
    CompiledSelectItem[] SelectItems,
    CompiledGroupByDefinition? GroupBy,
    CompiledAggregateDefinition[] Aggregates,
    CompiledFilterDefinition? Filter,
    CompiledHavingDefinition? Having,
    CompiledWindowDefinition? Window,
    CompiledOrderByDefinition[] OrderBy,
    CompiledTimestampByDefinition? TimestampBy,
    bool Follow);

public sealed record CompiledProjectionDefinition(CompiledProjectionField[] Fields);

public sealed record CompiledProjectionField(string OutputName, TryGetJsonValueDelegate Getter);

public enum CompiledSelectItemKind
{
    Field,
    Aggregate
}

public sealed record CompiledSelectItem(
    CompiledSelectItemKind Kind,
    string OutputName,
    int GroupIndex,
    int AggregateIndex,
    TryGetJsonValueDelegate? Getter);

public sealed record CompiledGroupByDefinition(TryGetJsonValueDelegate[] Fields);

public sealed record CompiledAggregateDefinition(
    CompiledAggregateType Type,
    string OutputName,
    bool CountAll,
    HasValueDelegate? HasValue,
    TryGetNumericValueDelegate? NumericGetter);

public enum CompiledAggregateType
{
    Count,
    Avg,
    Min,
    Max,
    Sum
}

public sealed record CompiledFilterDefinition(Func<JsonElement, bool> Matches);

public sealed record CompiledHavingDefinition(CompiledHavingCondition[] Conditions);

public sealed record CompiledHavingCondition(
    CompiledHavingOperand Operand,
    CompiledFilterOperator Operator,
    CompiledFilterValue Value);

public sealed record CompiledHavingOperand(CompiledHavingOperandKind Kind, int Index);

public enum CompiledHavingOperandKind
{
    GroupField,
    Aggregate
}

public sealed record CompiledFilterValue(CompiledFilterValueKind Kind, double Number, string String);

public enum CompiledFilterValueKind
{
    Number,
    String,
    Null
}

public enum CompiledFilterOperator
{
    GreaterThan,
    LessThan,
    Equals
}

public sealed record CompiledOrderByDefinition(string OutputName, CompiledSortDirection Direction);

public enum CompiledSortDirection
{
    Ascending,
    Descending
}

public sealed record CompiledWindowDefinition(CompiledWindowType Type, long SizeMs, long SlideMs);

public enum CompiledWindowType
{
    Tumbling,
    Hopping,
    Sliding
}

public sealed record CompiledTimestampByDefinition(ResolveTimestampDelegate Resolve);

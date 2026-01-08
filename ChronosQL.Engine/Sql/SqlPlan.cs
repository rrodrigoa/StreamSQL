using ChronosQL.Engine;

namespace ChronosQL.Engine.Sql;

public sealed record SqlScriptPlan(
    string RawSql,
    IReadOnlyList<SqlPlan> Statements);

public sealed record SqlPlan(
    string RawSql,
    IReadOnlyList<InputSourceDefinition> Inputs,
    JoinDefinition? Join,
    UnionDefinition? Union,
    string? OutputStream,
    TimestampByDefinition? TimestampBy,
    IReadOnlyList<SelectItem> SelectItems,
    IReadOnlyList<FieldReference> GroupBy,
    WindowDefinition? Window,
    IReadOnlyList<AggregateDefinition> Aggregates,
    FilterDefinition? Filter,
    HavingDefinition? Having,
    IReadOnlyList<OrderByDefinition> OrderBy);

public sealed record SelectItem(SelectItemKind Kind, FieldReference? Field, AggregateDefinition? Aggregate, string OutputName);

public enum SelectItemKind
{
    Field,
    Aggregate
}

public sealed record InputSourceDefinition(string Name, string? Alias);

public sealed record JoinDefinition(
    InputSourceDefinition LeftSource,
    InputSourceDefinition RightSource,
    FieldReference LeftKey,
    FieldReference RightKey);

public sealed record UnionDefinition(
    bool Distinct,
    IReadOnlyList<SqlPlan> Branches);

public sealed record FieldReference(IReadOnlyList<string> PathSegments);

public sealed record TimestampByDefinition(TimestampExpression Expression);

public abstract record TimestampExpression;

public sealed record TimestampFieldExpression(FieldReference Field) : TimestampExpression;

public sealed record TimestampLiteralExpression(FilterValue Value) : TimestampExpression;

public sealed record AggregateDefinition(AggregateType Type, FieldReference? Field, string OutputName, bool CountAll);

public enum AggregateType
{
    Count,
    Avg,
    Min,
    Max,
    Sum
}

public enum FilterOperator
{
    GreaterThan,
    LessThan,
    Equals
}

public sealed record FilterDefinition(IReadOnlyList<FilterCondition> Conditions);

public sealed record FilterCondition(FieldReference Field, FilterOperator Operator, FilterValue Value);

public sealed record FilterValue(FilterValueKind Kind, double Number, string String);

public enum FilterValueKind
{
    Number,
    String,
    Null
}

public sealed record HavingDefinition(IReadOnlyList<HavingCondition> Conditions);

public sealed record HavingCondition(HavingOperand Operand, FilterOperator Operator, FilterValue Value);

public sealed record HavingOperand(HavingOperandKind Kind, FieldReference? Field, AggregateDefinition? Aggregate);

public enum HavingOperandKind
{
    GroupField,
    Aggregate
}

public sealed record OrderByDefinition(string OutputName, SortDirection Direction);

public enum SortDirection
{
    Ascending,
    Descending
}

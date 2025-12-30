namespace StreamSql.Sql;

public sealed record SqlPlan(
    string RawSql,
    string? InputStream,
    string? OutputStream,
    IReadOnlyList<SelectItem> SelectItems,
    IReadOnlyList<FieldReference> GroupBy,
    FilterDefinition? Filter);

public sealed record SelectItem(SelectItemKind Kind, FieldReference? Field, AggregateDefinition? Aggregate, string OutputName);

public enum SelectItemKind
{
    Field,
    Aggregate
}

public sealed record FieldReference(IReadOnlyList<string> PathSegments);

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

public sealed record FilterValue(FilterValueKind Kind, double Number, bool Boolean);

public enum FilterValueKind
{
    Number,
    Boolean
}

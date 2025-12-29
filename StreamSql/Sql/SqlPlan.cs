namespace StreamSql.Sql;

public sealed record SqlPlan(
    string RawSql,
    string? InputStream,
    string? OutputStream,
    IReadOnlyList<SelectedField> SelectedFields,
    FilterDefinition? Filter,
    AggregateDefinition? Aggregate);

public sealed record SelectedField(FieldReference Source, string OutputName);

public sealed record FieldReference(IReadOnlyList<string> PathSegments);

public sealed record AggregateDefinition(AggregateType Type, FieldReference Field, string OutputName);

public enum AggregateType
{
    Sum
}

public enum FilterOperator
{
    GreaterThan
}

public sealed record FilterDefinition(FieldReference Field, double Value, FilterOperator Operator);

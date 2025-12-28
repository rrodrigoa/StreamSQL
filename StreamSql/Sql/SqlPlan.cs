namespace StreamSql.Sql;

public sealed record SqlPlan(
    string RawSql,
    string? TimestampField,
    string? InputStream,
    string? OutputStream,
    IReadOnlyList<SelectedField> SelectedFields,
    FilterDefinition? Filter);

public sealed record SelectedField(FieldReference Source, string OutputName);

public sealed record FieldReference(IReadOnlyList<string> PathSegments);

public enum FilterOperator
{
    GreaterThan
}

public sealed record FilterDefinition(FieldReference Field, double Value, FilterOperator Operator);

namespace ChronosQL.Engine.Sql;

public sealed record SqlScriptPlan(
    string RawSql,
    IReadOnlyList<SqlPlan> Statements);

public sealed record SqlPlan(
    string RawSql,
    string InputName,
    string OutputName,
    TimestampByDefinition? TimestampBy,
    SqlCondition? WhereCondition,
    bool SelectAll,
    IReadOnlyList<SelectFieldDefinition> SelectFields);

public sealed record SelectFieldDefinition(FieldReference Field, string OutputName);

public sealed record FieldReference(IReadOnlyList<string> PathSegments);

public sealed record TimestampByDefinition(FieldReference Field);

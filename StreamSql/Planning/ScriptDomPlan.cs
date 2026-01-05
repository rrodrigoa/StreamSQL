using ChronosQL.Engine.Sql;

namespace StreamSql.Planning;

public sealed record ScriptDomPlan(
    string RawSql,
    IReadOnlyList<WithDefinition> WithDefinitions,
    IReadOnlyList<SelectPlanDefinition> SelectStatements);

public sealed record WithDefinition(string Alias, SqlPlan Plan);

public sealed record SelectPlanDefinition(int Index, SqlPlan Plan);

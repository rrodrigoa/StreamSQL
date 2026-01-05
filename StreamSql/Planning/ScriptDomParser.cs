using System.Text.RegularExpressions;
using ChronosQL.Engine.Sql;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace StreamSql.Planning;

public static class ScriptDomParser
{
    public static ScriptDomPlan Parse(string sql)
    {
        var normalizedSql = Regex.Replace(sql, @"COUNT\s*\(\s*\)", "COUNT(*)", RegexOptions.IgnoreCase);
        var parser = new TSql170Parser(false);
        var fragment = parser.Parse(new StringReader(normalizedSql), out var errors);

        if (errors is { Count: > 0 })
        {
            var message = string.Join(Environment.NewLine, errors.Select(e => e.Message));
            throw new InvalidOperationException($"SQL parse error:{Environment.NewLine}{message}");
        }

        var statements = ExtractStatements(fragment).ToList();
        if (statements.Count == 0)
        {
            throw new InvalidOperationException("SQL script does not contain any statements.");
        }

        var withDefinitions = new List<WithDefinition>();
        var selectPlans = new List<SelectPlanDefinition>();

        for (var i = 0; i < statements.Count; i++)
        {
            var statement = statements[i];
            if (statement is not SelectStatement selectStatement)
            {
                throw new InvalidOperationException("Only SELECT statements are supported.");
            }

            var selectPlan = SqlParser.Parse(selectStatement.ToString());
            selectPlans.Add(new SelectPlanDefinition(i + 1, selectPlan));

            if (selectStatement.WithCtesAndXmlNamespaces?.CommonTableExpressions is null)
            {
                continue;
            }

            foreach (var cte in selectStatement.WithCtesAndXmlNamespaces.CommonTableExpressions)
            {
                var alias = cte.ExpressionName?.Value ?? string.Empty;
                var ctePlan = SqlParser.Parse(cte.QueryExpression.ToString());
                withDefinitions.Add(new WithDefinition(alias, ctePlan));
            }
        }

        return new ScriptDomPlan(sql, withDefinitions, selectPlans);
    }

    private static IEnumerable<TSqlStatement> ExtractStatements(TSqlFragment fragment)
    {
        if (fragment is TSqlScript script)
        {
            foreach (var batch in script.Batches)
            {
                foreach (var statement in batch.Statements)
                {
                    yield return statement;
                }
            }

            yield break;
        }

        if (fragment is TSqlBatch batchFragment)
        {
            foreach (var statement in batchFragment.Statements)
            {
                yield return statement;
            }

            yield break;
        }

        if (fragment is TSqlStatement statementFragment)
        {
            yield return statementFragment;
        }
    }
}

using System.Text.RegularExpressions;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace ChronosQL.Engine.Sql;

public static class SqlParser
{
    public static SqlScriptPlan ParseScript(string sql)
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

        var plans = new List<SqlPlan>(statements.Count);
        foreach (var statement in statements)
        {
            if (statement is not SelectStatement selectStatement)
            {
                throw new InvalidOperationException("Only SELECT statements are supported.");
            }

            plans.Add(Parse(selectStatement));
        }

        return new SqlScriptPlan(sql, plans);
    }

    public static SqlPlan Parse(string sql)
    {
        var scriptPlan = ParseScript(sql);
        if (scriptPlan.Statements.Count != 1)
        {
            throw new InvalidOperationException("SQL script must contain exactly one SELECT statement.");
        }

        return scriptPlan.Statements[0];
    }

    public static SqlPlan Parse(SelectStatement selectStatement)
    {
        if (selectStatement.QueryExpression is not QuerySpecification querySpecification)
        {
            throw new InvalidOperationException("Only simple SELECT statements are supported.");
        }

        ValidateQuerySpecification(querySpecification);

        if (selectStatement.Into is null)
        {
            throw new InvalidOperationException("SELECT statements must specify INTO <output>.");
        }

        var outputName = GetSchemaObjectName(selectStatement.Into);
        var inputName = GetSingleInputName(querySpecification);
        var fields = ParseSelectFields(querySpecification, inputName);
        var timestampBy = ParseTimestampBy(querySpecification, inputName);

        return new SqlPlan(selectStatement.ToString(), inputName, outputName, timestampBy, fields);
    }

    private static void ValidateQuerySpecification(QuerySpecification querySpecification)
    {
        if (querySpecification.WhereClause is not null)
        {
            throw new InvalidOperationException("WHERE clauses are not supported.");
        }

        if (querySpecification.GroupByClause is not null)
        {
            throw new InvalidOperationException("GROUP BY clauses are not supported.");
        }

        if (querySpecification.HavingClause is not null)
        {
            throw new InvalidOperationException("HAVING clauses are not supported.");
        }

        if (querySpecification.OrderByClause is not null)
        {
            throw new InvalidOperationException("ORDER BY clauses are not supported.");
        }

        if (querySpecification.TopRowFilter is not null)
        {
            throw new InvalidOperationException("TOP is not supported.");
        }

        if (querySpecification.UniqueRowFilter != UniqueRowFilter.NotSpecified)
        {
            throw new InvalidOperationException("DISTINCT is not supported.");
        }

        if (querySpecification.FromClause is null || querySpecification.FromClause.TableReferences.Count != 1)
        {
            throw new InvalidOperationException("SELECT must specify exactly one input source.");
        }
    }

    private static IReadOnlyList<SelectFieldDefinition> ParseSelectFields(QuerySpecification querySpecification, string inputName)
    {
        if (querySpecification.SelectElements.Count == 0)
        {
            throw new InvalidOperationException("SELECT must specify at least one field.");
        }

        var fields = new List<SelectFieldDefinition>();
        foreach (var element in querySpecification.SelectElements)
        {
            if (element is SelectStarExpression)
            {
                throw new InvalidOperationException("SELECT * is not supported.");
            }

            if (element is not SelectScalarExpression scalar)
            {
                throw new InvalidOperationException("Only column references are supported in SELECT.");
            }

            if (scalar.Expression is not ColumnReferenceExpression columnReference)
            {
                throw new InvalidOperationException("Only column references are supported in SELECT.");
            }

            var field = BuildFieldReference(columnReference, inputName);
            var outputName = scalar.ColumnName?.Value ?? field.PathSegments[^1];
            fields.Add(new SelectFieldDefinition(field, outputName));
        }

        return fields;
    }

    private static FieldReference BuildFieldReference(ColumnReferenceExpression expression, string? inputName)
    {
        if (expression.MultiPartIdentifier is null || expression.MultiPartIdentifier.Identifiers.Count == 0)
        {
            throw new InvalidOperationException("Invalid column reference.");
        }

        var segments = expression.MultiPartIdentifier.Identifiers
            .Select(identifier => identifier.Value)
            .Where(value => !string.IsNullOrWhiteSpace(value))
            .ToList();

        segments = RemoveInputPrefix(segments, inputName);

        if (segments.Count == 0)
        {
            throw new InvalidOperationException("Invalid column reference.");
        }

        return new FieldReference(segments);
    }

    private static TimestampByDefinition? ParseTimestampBy(QuerySpecification querySpecification, string inputName)
    {
        if (!TryGetTimestampByExpression(querySpecification, out var expression))
        {
            return null;
        }

        if (expression is not ColumnReferenceExpression columnReference)
        {
            throw new InvalidOperationException("TIMESTAMP BY only supports column references.");
        }

        var field = BuildFieldReference(columnReference, inputName);
        return new TimestampByDefinition(field);
    }

    private static bool TryGetTimestampByExpression(QuerySpecification querySpecification, out ScalarExpression expression)
    {
        expression = null!;
        var clauseProperty = querySpecification.GetType().GetProperty("TimestampByClause");
        if (clauseProperty?.GetValue(querySpecification) is not object clause)
        {
            return false;
        }

        var expressionProperty = clause.GetType().GetProperty("Expression");
        if (expressionProperty?.GetValue(clause) is not ScalarExpression scalarExpression)
        {
            return false;
        }

        expression = scalarExpression;
        return true;
    }

    private static string GetSingleInputName(QuerySpecification querySpecification)
    {
        var tableReference = querySpecification.FromClause!.TableReferences[0];
        if (tableReference is not NamedTableReference namedTable)
        {
            throw new InvalidOperationException("Only table references are supported in FROM.");
        }

        return GetSchemaObjectName(namedTable.SchemaObject);
    }

    private static string GetSchemaObjectName(SchemaObjectName name)
    {
        if (name == null || name.Identifiers.Count == 0)
        {
            throw new InvalidOperationException("Invalid schema object name.");
        }

        return string.Join(".", name.Identifiers.Select(id => id.Value));
    }

    private static List<string> RemoveInputPrefix(IReadOnlyList<string> segments, string? inputName)
    {
        if (string.IsNullOrWhiteSpace(inputName))
        {
            return segments.ToList();
        }

        var inputSegments = inputName
            .Split('.', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        if (inputSegments.Length == 0 || segments.Count < inputSegments.Length)
        {
            return segments.ToList();
        }

        for (var i = 0; i < inputSegments.Length; i++)
        {
            if (!string.Equals(segments[i], inputSegments[i], StringComparison.OrdinalIgnoreCase))
            {
                return segments.ToList();
            }
        }

        return segments.Skip(inputSegments.Length).ToList();
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

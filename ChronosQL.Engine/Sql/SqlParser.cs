using System.Globalization;
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
        var selectFields = ParseSelectFields(querySpecification, inputName);
        var timestampBy = ParseTimestampBy(querySpecification, inputName);
        var whereCondition = ParseWhereCondition(querySpecification, inputName);

        return new SqlPlan(
            selectStatement.ToString(),
            inputName,
            outputName,
            timestampBy,
            whereCondition,
            selectFields.SelectAll,
            selectFields.Fields);
    }

    private static void ValidateQuerySpecification(QuerySpecification querySpecification)
    {
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

    private static (bool SelectAll, IReadOnlyList<SelectFieldDefinition> Fields) ParseSelectFields(
        QuerySpecification querySpecification,
        string inputName)
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
                if (querySpecification.SelectElements.Count != 1)
                {
                    throw new InvalidOperationException("SELECT * cannot be combined with other fields.");
                }

                return (true, Array.Empty<SelectFieldDefinition>());
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

        return (false, fields);
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

    private static SqlCondition? ParseWhereCondition(QuerySpecification querySpecification, string inputName)
    {
        if (querySpecification.WhereClause?.SearchCondition is null)
        {
            return null;
        }

        return ParseCondition(querySpecification.WhereClause.SearchCondition, inputName);
    }

    private static SqlCondition ParseCondition(BooleanExpression expression, string inputName)
    {
        switch (expression)
        {
            case BooleanBinaryExpression binary:
                return new SqlBinaryCondition(
                    ParseCondition(binary.FirstExpression, inputName),
                    MapBinaryOperator(binary.BinaryExpressionType),
                    ParseCondition(binary.SecondExpression, inputName));
            case BooleanParenthesisExpression parenthesis:
                return ParseCondition(parenthesis.Expression, inputName);
            case BooleanNotExpression notExpression:
                return new SqlNotCondition(ParseCondition(notExpression.Expression, inputName));
            case BooleanComparisonExpression comparison:
                return new SqlPredicateCondition(new SqlComparisonPredicate(
                    ParseScalarExpression(comparison.FirstExpression, inputName),
                    MapComparisonOperator(comparison.ComparisonType),
                    ParseScalarExpression(comparison.SecondExpression, inputName)));
            case BooleanIsNullExpression isNullExpression:
                return new SqlPredicateCondition(new SqlIsNullPredicate(
                    ParseScalarExpression(isNullExpression.Expression, inputName),
                    GetBooleanProperty(isNullExpression, "IsNot")));
            case LikePredicate likePredicate:
                return new SqlPredicateCondition(new SqlLikePredicate(
                    ParseScalarExpression(GetScalarProperty(likePredicate, "Expression", "FirstExpression"), inputName),
                    ParseScalarExpression(GetScalarProperty(likePredicate, "Pattern", "SecondExpression"), inputName),
                    GetBooleanProperty(likePredicate, "NotDefined")));
            case BetweenPredicate betweenPredicate:
                return new SqlPredicateCondition(new SqlBetweenPredicate(
                    ParseScalarExpression(GetScalarProperty(betweenPredicate, "Expression", "FirstExpression"), inputName),
                    ParseScalarExpression(GetScalarProperty(betweenPredicate, "LowerExpression", "SecondExpression"), inputName),
                    ParseScalarExpression(GetScalarProperty(betweenPredicate, "UpperExpression", "ThirdExpression"), inputName),
                    GetBooleanProperty(betweenPredicate, "NotDefined")));
            case InPredicate inPredicate:
                return new SqlPredicateCondition(ParseInPredicate(inPredicate, inputName));
            default:
                throw new InvalidOperationException($"Unsupported WHERE condition: {expression.GetType().Name}.");
        }
    }

    private static SqlPredicate ParseInPredicate(InPredicate predicate, string inputName)
    {
        var values = GetValuesProperty(predicate, "Values");
        var subquery = GetPropertyValue<TSqlSubquery>(predicate, "Subquery");
        if (subquery is not null)
        {
            throw new InvalidOperationException("IN subqueries are not supported.");
        }

        if (values is null || values.Count == 0)
        {
            throw new InvalidOperationException("IN requires at least one value.");
        }

        var parsedValues = values
            .Select(value => ParseScalarExpression(value, inputName))
            .ToList();

        return new SqlInPredicate(
            ParseScalarExpression(GetScalarProperty(predicate, "Expression", "FirstExpression"), inputName),
            parsedValues,
            GetBooleanProperty(predicate, "NotDefined"));
    }

    private static SqlExpression ParseScalarExpression(ScalarExpression expression, string inputName)
    {
        switch (expression)
        {
            case ColumnReferenceExpression columnReference:
                return new SqlFieldExpression(BuildFieldReference(columnReference, inputName));
            case StringLiteral stringLiteral:
                return new SqlLiteralExpression(new SqlLiteral(SqlLiteralKind.String, stringLiteral.Value, null, null));
            case IntegerLiteral integerLiteral:
                return new SqlLiteralExpression(new SqlLiteral(
                    SqlLiteralKind.Number,
                    null,
                    double.Parse(integerLiteral.Value, CultureInfo.InvariantCulture),
                    null));
            case NumericLiteral numericLiteral:
                return new SqlLiteralExpression(new SqlLiteral(
                    SqlLiteralKind.Number,
                    null,
                    double.Parse(numericLiteral.Value, CultureInfo.InvariantCulture),
                    null));
            case MoneyLiteral moneyLiteral:
                return new SqlLiteralExpression(new SqlLiteral(
                    SqlLiteralKind.Number,
                    null,
                    double.Parse(moneyLiteral.Value, CultureInfo.InvariantCulture),
                    null));
            case NullLiteral:
                return new SqlLiteralExpression(new SqlLiteral(SqlLiteralKind.Null, null, null, null));
            case BooleanLiteral booleanLiteral:
                return new SqlLiteralExpression(new SqlLiteral(SqlLiteralKind.Boolean, null, null, booleanLiteral.Value));
            default:
                throw new InvalidOperationException($"Unsupported expression in WHERE clause: {expression.GetType().Name}.");
        }
    }

    private static SqlBinaryOperator MapBinaryOperator(BooleanBinaryExpressionType type)
        => type switch
        {
            BooleanBinaryExpressionType.And => SqlBinaryOperator.And,
            BooleanBinaryExpressionType.Or => SqlBinaryOperator.Or,
            _ => throw new InvalidOperationException($"Unsupported boolean operator: {type}.")
        };

    private static SqlComparisonOperator MapComparisonOperator(BooleanComparisonType type)
        => type switch
        {
            BooleanComparisonType.Equals => SqlComparisonOperator.Equal,
            BooleanComparisonType.GreaterThan => SqlComparisonOperator.GreaterThan,
            BooleanComparisonType.GreaterThanOrEqualTo => SqlComparisonOperator.GreaterThanOrEqual,
            BooleanComparisonType.LessThan => SqlComparisonOperator.LessThan,
            BooleanComparisonType.LessThanOrEqualTo => SqlComparisonOperator.LessThanOrEqual,
            BooleanComparisonType.NotEqualToBrackets => SqlComparisonOperator.NotEqual,
            BooleanComparisonType.NotEqualToExclamation => SqlComparisonOperator.NotEqual,
            BooleanComparisonType.NotGreaterThan => SqlComparisonOperator.NotGreaterThan,
            BooleanComparisonType.NotLessThan => SqlComparisonOperator.NotLessThan,
            _ => throw new InvalidOperationException($"Unsupported comparison operator: {type}.")
        };

    private static ScalarExpression GetScalarProperty(object instance, params string[] propertyNames)
    {
        var expression = GetPropertyValue<ScalarExpression>(instance, propertyNames);
        if (expression is null)
        {
            throw new InvalidOperationException($"Unsupported predicate shape for {instance.GetType().Name}.");
        }

        return expression;
    }

    private static IList<ScalarExpression>? GetValuesProperty(object instance, params string[] propertyNames)
        => GetPropertyValue<IList<ScalarExpression>>(instance, propertyNames);

    private static bool GetBooleanProperty(object instance, params string[] propertyNames)
        => GetPropertyValue<bool>(instance, propertyNames);

    private static T? GetPropertyValue<T>(object instance, params string[] propertyNames)
    {
        foreach (var name in propertyNames)
        {
            var property = instance.GetType().GetProperty(name);
            if (property?.GetValue(instance) is T value)
            {
                return value;
            }
        }

        return default;
    }
}

using Microsoft.SqlServer.TransactSql.ScriptDom;
using System.Text.RegularExpressions;

namespace StreamSql.Sql;

public static class SqlParser
{
    public static SqlPlan Parse(string sql)
    {
        var normalizedSql = Regex.Replace(sql, @"COUNT\s*\(\s*\)", "COUNT(*)", RegexOptions.IgnoreCase);
        var parser = new TSql150Parser(false);
        var fragment = parser.Parse(new StringReader(normalizedSql), out var errors);

        if (errors is { Count: > 0 })
        {
            var message = string.Join(Environment.NewLine, errors.Select(e => e.Message));
            throw new InvalidOperationException($"SQL parse error:{Environment.NewLine}{message}");
        }

        var visitor = new SqlValidationVisitor();
        fragment.Accept(visitor);

        if (visitor.Unsupported.Count > 0)
        {
            var details = string.Join(", ", visitor.Unsupported);
            throw new InvalidOperationException($"Unsupported SQL syntax detected: {details}");
        }

        return new SqlPlan(
            sql,
            visitor.InputStream,
            visitor.OutputStream,
            visitor.SelectItems,
            visitor.GroupBy,
            visitor.Filter);
    }

    private sealed class SqlValidationVisitor : TSqlFragmentVisitor
    {
        private const string NestedPathError = "Nested JSON paths beyond one level require ChronosQL Pro";
        public List<string> Unsupported { get; } = new();
        public string? InputStream { get; private set; }
        public string? OutputStream { get; private set; }
        public List<SelectItem> SelectItems { get; } = new();
        public List<FieldReference> GroupBy { get; } = new();
        public FilterDefinition? Filter { get; private set; }
        public bool HasAggregate => SelectItems.Any(item => item.Kind == SelectItemKind.Aggregate);

        public override void ExplicitVisit(QuerySpecification node)
        {
            base.ExplicitVisit(node);
            if (node.SelectElements is null || node.SelectElements.Count == 0)
            {
                Unsupported.Add("SELECT list");
                return;
            }

            foreach (var element in node.SelectElements)
            {
                if (element is SelectStarExpression)
                {
                    Unsupported.Add("SELECT *");
                    continue;
                }

                if (element is not SelectScalarExpression scalar)
                {
                    Unsupported.Add("SELECT expression");
                    continue;
                }

                if (scalar.Expression is ColumnReferenceExpression column)
                {
                    if (!TryBuildFieldReference(column, InputStream, out var fieldReference, out var error))
                    {
                        if (!string.IsNullOrWhiteSpace(error))
                        {
                            Unsupported.Add(error);
                        }
                        else
                        {
                            Unsupported.Add("SELECT column");
                        }
                        continue;
                    }

                    var outputName = scalar.ColumnName?.Value ?? fieldReference.PathSegments.Last();
                    SelectItems.Add(new SelectItem(SelectItemKind.Field, fieldReference, null, outputName));
                    continue;
                }

                if (scalar.Expression is FunctionCall functionCall)
                {
                    if (!TryBuildAggregate(functionCall, scalar.ColumnName?.Value, InputStream, out var aggregate, out var error))
                    {
                        Unsupported.Add(error ?? "SELECT aggregate");
                        continue;
                    }

                    SelectItems.Add(new SelectItem(SelectItemKind.Aggregate, null, aggregate, aggregate.OutputName));
                    continue;
                }

                Unsupported.Add("SELECT expression");
            }

            if (node.WhereClause is not null)
            {
                Filter = BuildFilter(node.WhereClause.SearchCondition, InputStream, out var error);
                if (Filter is null)
                {
                    Unsupported.Add(error ?? "WHERE clause");
                }
            }

            if (node.GroupByClause is not null)
            {
                var groupByColumns = node.GroupByClause.GroupingSpecifications
                    .OfType<ExpressionGroupingSpecification>()
                    .Select(spec => spec.Expression)
                    .ToList();
                if (groupByColumns.Count == 0)
                {
                    Unsupported.Add("GROUP BY");
                }
                else
                {
                    foreach (var expression in groupByColumns)
                    {
                        if (expression is not ColumnReferenceExpression column)
                        {
                            Unsupported.Add("GROUP BY expression");
                            continue;
                        }

                        if (!TryBuildFieldReference(column, InputStream, out var fieldReference, out var error))
                        {
                            Unsupported.Add(error ?? "GROUP BY column");
                            continue;
                        }

                        GroupBy.Add(fieldReference);
                    }
                }
            }

            ValidateAggregates();
        }

        public override void ExplicitVisit(FromClause node)
        {
            base.ExplicitVisit(node);
            if (node.TableReferences.Count == 0)
            {
                Unsupported.Add("FROM");
                return;
            }

            if (node.TableReferences[0] is NamedTableReference namedTable)
            {
                InputStream = GetSchemaObjectName(namedTable.SchemaObject);
                return;
            }

            Unsupported.Add("FROM reference");
        }

        public override void ExplicitVisit(SelectStatement node)
        {
            base.ExplicitVisit(node);
            if (node.QueryExpression is not QuerySpecification)
            {
                Unsupported.Add("Only simple SELECT statements are supported");
            }
            if (node.Into is not null)
            {
                OutputStream = GetSchemaObjectName(node.Into);
            }
        }

        private static string? GetSchemaObjectName(SchemaObjectName? schemaObjectName)
        {
            if (schemaObjectName?.Identifiers is null || schemaObjectName.Identifiers.Count == 0)
            {
                return null;
            }

            return string.Join('.', schemaObjectName.Identifiers.Select(id => id.Value));
        }

        private static bool TryBuildFieldReference(
            ColumnReferenceExpression column,
            string? inputStream,
            out FieldReference fieldReference,
            out string? error)
        {
            error = null;
            fieldReference = default!;

            if (column.MultiPartIdentifier?.Identifiers is null || column.MultiPartIdentifier.Identifiers.Count == 0)
            {
                return false;
            }

            var identifiers = column.MultiPartIdentifier.Identifiers.Select(id => id.Value).ToList();
            if (identifiers.Count > 1)
            {
                var streamName = NormalizeStreamName(inputStream);
                if (!string.IsNullOrWhiteSpace(streamName) &&
                    identifiers[0].Equals(streamName, StringComparison.OrdinalIgnoreCase))
                {
                    identifiers.RemoveAt(0);
                }
            }

            if (identifiers.Count > 2)
            {
                error = NestedPathError;
                return false;
            }

            if (identifiers.Count == 0)
            {
                return false;
            }

            fieldReference = new FieldReference(identifiers);
            return true;
        }

        private static string? NormalizeStreamName(string? inputStream)
        {
            if (string.IsNullOrWhiteSpace(inputStream))
            {
                return null;
            }

            var segments = inputStream.Split('.', StringSplitOptions.RemoveEmptyEntries);
            return segments.Length > 0 ? segments[^1] : inputStream;
        }

        private static FilterDefinition? BuildFilter(BooleanExpression? searchCondition, string? inputStream, out string? error)
        {
            error = null;
            if (searchCondition is null)
            {
                return null;
            }

            var conditions = new List<FilterCondition>();
            if (!TryCollectConditions(searchCondition, inputStream, conditions, out error))
            {
                return null;
            }

            return new FilterDefinition(conditions);
        }

        private static bool TryCollectConditions(
            BooleanExpression expression,
            string? inputStream,
            List<FilterCondition> conditions,
            out string? error)
        {
            error = null;
            if (expression is BooleanBinaryExpression binary)
            {
                if (binary.BinaryExpressionType != BooleanBinaryExpressionType.And)
                {
                    error = "WHERE clause";
                    return false;
                }

                return TryCollectConditions(binary.FirstExpression, inputStream, conditions, out error)
                    && TryCollectConditions(binary.SecondExpression, inputStream, conditions, out error);
            }

            if (expression is not BooleanComparisonExpression comparison)
            {
                error = "WHERE clause";
                return false;
            }

            if (!TryGetOperator(comparison.ComparisonType, out var filterOperator))
            {
                error = "WHERE clause";
                return false;
            }

            if (comparison.FirstExpression is ColumnReferenceExpression column &&
                TryGetLiteral(comparison.SecondExpression, out var literal))
            {
                if (!TryBuildFieldReference(column, inputStream, out var fieldReference, out error))
                {
                    return false;
                }

                conditions.Add(new FilterCondition(fieldReference, filterOperator, literal));
                return true;
            }

            error = "WHERE clause";
            return false;
        }

        private static bool TryGetOperator(BooleanComparisonType comparisonType, out FilterOperator filterOperator)
        {
            filterOperator = FilterOperator.Equals;
            switch (comparisonType)
            {
                case BooleanComparisonType.GreaterThan:
                    filterOperator = FilterOperator.GreaterThan;
                    return true;
                case BooleanComparisonType.LessThan:
                    filterOperator = FilterOperator.LessThan;
                    return true;
                case BooleanComparisonType.Equals:
                    filterOperator = FilterOperator.Equals;
                    return true;
            }

            return false;
        }

        private static bool TryGetLiteral(ScalarExpression expression, out FilterValue value)
        {
            value = new FilterValue(FilterValueKind.Number, 0, false);
            switch (expression)
            {
                case IntegerLiteral integerLiteral when double.TryParse(integerLiteral.Value, out var integer):
                    value = new FilterValue(FilterValueKind.Number, integer, false);
                    return true;
                case NumericLiteral numericLiteral when double.TryParse(numericLiteral.Value, out var numeric):
                    value = new FilterValue(FilterValueKind.Number, numeric, false);
                    return true;
                case BooleanLiteral booleanLiteral when bool.TryParse(booleanLiteral.Value, out var boolean):
                    value = new FilterValue(FilterValueKind.Boolean, 0, boolean);
                    return true;
                default:
                    return false;
            }
        }

        private static bool TryBuildAggregate(
            FunctionCall functionCall,
            string? alias,
            string? inputStream,
            out AggregateDefinition? aggregate,
            out string? error)
        {
            aggregate = null;
            error = null;

            var functionName = functionCall.FunctionName.Value;
            if (!TryGetAggregateType(functionName, out var aggregateType))
            {
                return false;
            }

            if (aggregateType == AggregateType.Count)
            {
                if (functionCall.Parameters.Count == 0)
                {
                    aggregate = new AggregateDefinition(aggregateType, null, alias ?? "count", CountAll: true);
                    return true;
                }

                if (functionCall.Parameters.Count == 1)
                {
                    if (functionCall.Parameters[0] is StarExpression)
                    {
                        aggregate = new AggregateDefinition(aggregateType, null, alias ?? "count", CountAll: true);
                        return true;
                    }

                    if (functionCall.Parameters[0] is ColumnReferenceExpression countColumn)
                    {
                        if (!TryBuildFieldReference(countColumn, inputStream, out var countField, out error))
                        {
                            return false;
                        }

                        aggregate = new AggregateDefinition(aggregateType, countField, alias ?? "count", CountAll: false);
                        return true;
                    }
                }

                error = "COUNT expects no arguments, *, or a single column.";
                return false;
            }

            if (functionCall.Parameters.Count != 1 || functionCall.Parameters[0] is not ColumnReferenceExpression column)
            {
                error = "Aggregate expects a single column.";
                return false;
            }

            if (!TryBuildFieldReference(column, inputStream, out var fieldReference, out error))
            {
                return false;
            }

            var defaultName = aggregateType switch
            {
                AggregateType.Sum => "sum",
                AggregateType.Avg => "avg",
                AggregateType.Min => "min",
                AggregateType.Max => "max",
                _ => "count"
            };

            aggregate = new AggregateDefinition(aggregateType, fieldReference, alias ?? defaultName, CountAll: false);
            return true;
        }

        private static bool TryGetAggregateType(string name, out AggregateType aggregateType)
        {
            aggregateType = AggregateType.Sum;
            switch (name.ToUpperInvariant())
            {
                case "COUNT":
                    aggregateType = AggregateType.Count;
                    return true;
                case "AVG":
                    aggregateType = AggregateType.Avg;
                    return true;
                case "MIN":
                    aggregateType = AggregateType.Min;
                    return true;
                case "MAX":
                    aggregateType = AggregateType.Max;
                    return true;
                case "SUM":
                    aggregateType = AggregateType.Sum;
                    return true;
                default:
                    return false;
            }
        }

        private void ValidateAggregates()
        {
            if (!HasAggregate)
            {
                if (GroupBy.Count > 0)
                {
                    Unsupported.Add("GROUP BY without aggregate");
                }
                return;
            }

            var nonAggregateFields = SelectItems
                .Where(item => item.Kind == SelectItemKind.Field)
                .Select(item => item.Field!)
                .ToList();

            if (GroupBy.Count == 0)
            {
                if (nonAggregateFields.Count > 0)
                {
                    Unsupported.Add("Aggregate queries must GROUP BY all non-aggregated fields.");
                }

                return;
            }

            foreach (var field in nonAggregateFields)
            {
                if (!GroupBy.Any(group => FieldEquals(group, field)))
                {
                    Unsupported.Add("GROUP BY mismatch: all non-aggregated fields must appear in GROUP BY.");
                }
            }
        }

        private static bool FieldEquals(FieldReference left, FieldReference right) =>
            left.PathSegments.SequenceEqual(right.PathSegments, StringComparer.OrdinalIgnoreCase);
    }
}
}

using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace StreamSql.Sql;

public static class SqlParser
{
    public static SqlPlan Parse(string sql)
    {
        var parser = new TSql150Parser(false);
        var fragment = parser.Parse(new StringReader(sql), out var errors);

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
            visitor.SelectedFields,
            visitor.Filter,
            visitor.Aggregate);
    }

    private sealed class SqlValidationVisitor : TSqlFragmentVisitor
    {
        public List<string> Unsupported { get; } = new();
        public string? InputStream { get; private set; }
        public string? OutputStream { get; private set; }
        public List<SelectedField> SelectedFields { get; } = new();
        public AggregateDefinition? Aggregate { get; private set; }
        public FilterDefinition? Filter { get; private set; }

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
                if (element is not SelectScalarExpression scalar)
                {
                    Unsupported.Add("SELECT expression");
                    continue;
                }

                if (scalar.Expression is ColumnReferenceExpression column)
                {
                    var fieldReference = BuildFieldReference(column, InputStream);
                    if (fieldReference is null)
                    {
                        Unsupported.Add("SELECT column");
                        continue;
                    }

                    var outputName = scalar.ColumnName?.Value ?? fieldReference.PathSegments.Last();
                    SelectedFields.Add(new SelectedField(fieldReference, outputName));
                    continue;
                }

                if (scalar.Expression is FunctionCall functionCall)
                {
                    if (Aggregate is not null)
                    {
                        Unsupported.Add("SELECT multiple aggregates");
                        continue;
                    }

                    if (!TryBuildAggregate(functionCall, scalar.ColumnName?.Value, InputStream, out var aggregate))
                    {
                        Unsupported.Add("SELECT aggregate");
                        continue;
                    }

                    Aggregate = aggregate;
                    continue;
                }

                Unsupported.Add("SELECT expression");
            }

            if (Aggregate is not null && SelectedFields.Count > 0)
            {
                Unsupported.Add("SELECT list with aggregate");
            }

            if (node.WhereClause is not null)
            {
                Filter = BuildFilter(node.WhereClause.SearchCondition, InputStream);
                if (Filter is null)
                {
                    Unsupported.Add("WHERE clause");
                }
            }
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

        private static FieldReference? BuildFieldReference(ColumnReferenceExpression column, string? inputStream)
        {
            if (column.MultiPartIdentifier?.Identifiers is null || column.MultiPartIdentifier.Identifiers.Count == 0)
            {
                return null;
            }

            var identifiers = column.MultiPartIdentifier.Identifiers.Select(id => id.Value).ToList();
            if (identifiers.Count > 1)
            {
                if (!string.IsNullOrWhiteSpace(inputStream) &&
                    identifiers[0].Equals(inputStream, StringComparison.OrdinalIgnoreCase))
                {
                    identifiers.RemoveAt(0);
                }
            }

            return new FieldReference(identifiers);
        }

        private static FilterDefinition? BuildFilter(BooleanExpression? searchCondition, string? inputStream)
        {
            if (searchCondition is not BooleanComparisonExpression comparison)
            {
                return null;
            }

            if (comparison.ComparisonType != BooleanComparisonType.GreaterThan)
            {
                return null;
            }

            if (comparison.FirstExpression is ColumnReferenceExpression column &&
                TryGetNumericLiteral(comparison.SecondExpression, out var value))
            {
                var fieldReference = BuildFieldReference(column, inputStream);
                if (fieldReference is null)
                {
                    return null;
                }

                return new FilterDefinition(fieldReference, value, FilterOperator.GreaterThan);
            }

            return null;
        }

        private static bool TryGetNumericLiteral(ScalarExpression expression, out double value)
        {
            value = 0;
            switch (expression)
            {
                case IntegerLiteral integerLiteral when double.TryParse(integerLiteral.Value, out value):
                    return true;
                case NumericLiteral numericLiteral when double.TryParse(numericLiteral.Value, out value):
                    return true;
                default:
                    return false;
            }
        }

        private static bool TryBuildAggregate(FunctionCall functionCall, string? alias, string? inputStream, out AggregateDefinition? aggregate)
        {
            aggregate = null;

            if (!string.Equals(functionCall.FunctionName.Value, "SUM", StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            if (functionCall.Parameters.Count != 1 || functionCall.Parameters[0] is not ColumnReferenceExpression column)
            {
                return false;
            }

            var fieldReference = BuildFieldReference(column, inputStream);
            if (fieldReference is null)
            {
                return false;
            }

            var outputName = alias ?? "sum";
            aggregate = new AggregateDefinition(AggregateType.Sum, fieldReference, outputName);
            return true;
        }
    }
}

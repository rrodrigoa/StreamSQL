using Microsoft.SqlServer.TransactSql.ScriptDom;
using ChronosQL.Engine;
using System.Text.RegularExpressions;

namespace ChronosQL.Engine.Sql;

public static class SqlParser
{
    public static SqlPlan Parse(string sql)
    {
        var normalizedSql = Regex.Replace(sql, @"COUNT\s*\(\s*\)", "COUNT(*)", RegexOptions.IgnoreCase);
        var parser = new TSql170Parser(false);
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
            visitor.TimestampBy,
            visitor.SelectItems,
            visitor.GroupBy,
            visitor.Window,
            visitor.Aggregates,
            visitor.Filter,
            visitor.Having,
            visitor.OrderBy);
    }

    private sealed class SqlValidationVisitor : TSqlFragmentVisitor
    {
        private const string NestedPathError = "Nested JSON paths beyond one level require ChronosQL Pro";
        public List<string> Unsupported { get; } = new();
        public string? InputStream { get; private set; }
        public string? OutputStream { get; private set; }
        public TimestampByDefinition? TimestampBy { get; private set; }
        public List<SelectItem> SelectItems { get; } = new();
        public List<FieldReference> GroupBy { get; } = new();
        public WindowDefinition? Window { get; private set; }
        public List<AggregateDefinition> Aggregates { get; } = new();
        public FilterDefinition? Filter { get; private set; }
        public HavingDefinition? Having { get; private set; }
        public List<OrderByDefinition> OrderBy { get; } = new();
        public bool HasAggregate => Aggregates.Count > 0;
        private readonly List<ExpressionWithSortOrder> _pendingOrderBy = new();

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
                    if (IsWindowFunction(functionCall))
                    {
                        Unsupported.Add("Window functions are only supported in GROUP BY.");
                        continue;
                    }

                    if (!TryBuildAggregate(functionCall, scalar.ColumnName?.Value, InputStream, out var aggregate, out var error))
                    {
                        Unsupported.Add(error ?? "SELECT aggregate");
                        continue;
                    }

                    SelectItems.Add(new SelectItem(SelectItemKind.Aggregate, null, aggregate, aggregate.OutputName));
                    AddAggregate(aggregate);
                    continue;
                }

                Unsupported.Add("SELECT expression");
            }

            if (TryGetTimestampByExpression(node, out var timestampExpression))
            {
                if (TimestampBy is not null)
                {
                    Unsupported.Add("Only one TIMESTAMP BY clause is supported.");
                }
                else if (!TryBuildTimestampBy(timestampExpression, InputStream, out var timestampBy, out var error))
                {
                    Unsupported.Add(error ?? "TIMESTAMP BY expression");
                }
                else
                {
                    TimestampBy = timestampBy;
                }
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
                            if (expression is FunctionCall functionCall)
                            {
                                if (!TryBuildWindowDefinition(functionCall, out var windowDefinition, out var error1))
                                {
                                    Unsupported.Add(error1 ?? "GROUP BY expression");
                                    continue;
                                }

                                if (Window is not null)
                                {
                                    Unsupported.Add("Only one window function can appear in GROUP BY.");
                                    continue;
                                }

                                Window = windowDefinition;
                                continue;
                            }

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

            if (node.HavingClause is not null)
            {
                Having = BuildHaving(node.HavingClause.SearchCondition, out var havingError);
                if (Having is null)
                {
                    Unsupported.Add(havingError ?? "HAVING clause");
                }
            }

            BuildOrderBy();
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

        public override void ExplicitVisit(OrderByClause node)
        {
            base.ExplicitVisit(node);

            if (node.OrderByElements is null || node.OrderByElements.Count == 0)
            {
                Unsupported.Add("ORDER BY");
                return;
            }

            _pendingOrderBy.AddRange(node.OrderByElements);
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

        private static bool TryGetTimestampByExpression(QuerySpecification node, out ScalarExpression expression)
        {
            expression = null!;

            var clauseProperty = node.GetType().GetProperty("TimestampByClause");
            if (clauseProperty?.GetValue(node) is not object clause)
            {
                return false;
            }

            var expressionProperty = clause.GetType().GetProperty("TimestampExpression")
                                     ?? clause.GetType().GetProperty("Expression");
            if (expressionProperty?.GetValue(clause) is ScalarExpression scalarExpression)
            {
                expression = scalarExpression;
                return true;
            }

            return false;
        }

        private static bool TryBuildTimestampBy(
            ScalarExpression expression,
            string? inputStream,
            out TimestampByDefinition? timestampBy,
            out string? error)
        {
            timestampBy = null;
            error = null;

            var unwrapped = UnwrapParentheses(expression);
            if (unwrapped is ColumnReferenceExpression column)
            {
                if (!TryBuildFieldReference(column, inputStream, out var fieldReference, out error))
                {
                    return false;
                }

                timestampBy = new TimestampByDefinition(new TimestampFieldExpression(fieldReference));
                return true;
            }

            if (TryGetLiteral(unwrapped, out var literal))
            {
                if (literal.Kind == FilterValueKind.Null)
                {
                    error = "TIMESTAMP BY does not support NULL.";
                    return false;
                }

                timestampBy = new TimestampByDefinition(new TimestampLiteralExpression(literal));
                return true;
            }

            error = "TIMESTAMP BY expression";
            return false;
        }

        private static ScalarExpression UnwrapParentheses(ScalarExpression expression)
        {
            var current = expression;
            while (current is ParenthesisExpression parenthesis)
            {
                current = parenthesis.Expression;
            }

            return current;
        }

        private bool TryBuildOrderBy(ExpressionWithSortOrder element, out OrderByDefinition orderBy, out string? error)
        {
            orderBy = default!;
            error = null;

            var direction = element.SortOrder == SortOrder.Descending
                ? SortDirection.Descending
                : SortDirection.Ascending;

            if (element.Expression is ColumnReferenceExpression column)
            {
                if (TryResolveOrderByColumn(column, out var outputName, out error))
                {
                    orderBy = new OrderByDefinition(outputName, direction);
                    return true;
                }

                return false;
            }

            if (element.Expression is FunctionCall functionCall)
            {
                if (IsWindowFunction(functionCall))
                {
                    error = "Window functions are only supported in GROUP BY.";
                    return false;
                }

                if (!TryBuildAggregate(functionCall, null, InputStream, out var aggregate, out error))
                {
                    return false;
                }

                var match = SelectItems.FirstOrDefault(item =>
                    item.Kind == SelectItemKind.Aggregate &&
                    AggregateEquals(item.Aggregate!, aggregate!));

                if (match is null)
                {
                    error = "ORDER BY aggregate must appear in SELECT";
                    return false;
                }

                orderBy = new OrderByDefinition(match.OutputName, direction);
                return true;
            }

            error = "ORDER BY expression";
            return false;
        }

        private void BuildOrderBy()
        {
            if (_pendingOrderBy.Count == 0)
            {
                return;
            }

            foreach (var element in _pendingOrderBy)
            {
                if (!TryBuildOrderBy(element, out var orderBy, out var error))
                {
                    Unsupported.Add(error ?? "ORDER BY");
                    continue;
                }

                OrderBy.Add(orderBy);
            }

            _pendingOrderBy.Clear();
        }

        private bool TryResolveOrderByColumn(
            ColumnReferenceExpression column,
            out string outputName,
            out string? error)
        {
            outputName = string.Empty;
            error = null;

            if (column.MultiPartIdentifier?.Identifiers is null || column.MultiPartIdentifier.Identifiers.Count == 0)
            {
                error = "ORDER BY column";
                return false;
            }

            var identifiers = column.MultiPartIdentifier.Identifiers.Select(id => id.Value).ToList();
            if (identifiers.Count == 1)
            {
                var identifier = identifiers[0];
                if (identifier.Equals("windowStart", StringComparison.OrdinalIgnoreCase) ||
                    identifier.Equals("windowEnd", StringComparison.OrdinalIgnoreCase))
                {
                    outputName = identifier;
                    return true;
                }

                var match = SelectItems.FirstOrDefault(item =>
                    item.OutputName.Equals(identifier, StringComparison.OrdinalIgnoreCase));
                if (match is not null)
                {
                    outputName = match.OutputName;
                    return true;
                }
            }

            if (!TryBuildFieldReference(column, InputStream, out var fieldReference, out error))
            {
                return false;
            }

            var fieldMatch = SelectItems.FirstOrDefault(item =>
                item.Kind == SelectItemKind.Field && FieldEquals(item.Field!, fieldReference));
            if (fieldMatch is null)
            {
                error = "ORDER BY column must appear in SELECT";
                return false;
            }

            outputName = fieldMatch.OutputName;
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

        private HavingDefinition? BuildHaving(BooleanExpression? searchCondition, out string? error)
        {
            error = null;
            if (searchCondition is null)
            {
                return null;
            }

            var conditions = new List<HavingCondition>();
            if (!TryCollectHavingConditions(searchCondition, conditions, out error))
            {
                return null;
            }

            return new HavingDefinition(conditions);
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

            if (comparison.FirstExpression is FunctionCall functionCall && IsWindowFunction(functionCall))
            {
                error = "Window functions are only supported in GROUP BY.";
                return false;
            }

            error = "WHERE clause";
            return false;
        }

        private bool TryCollectHavingConditions(
            BooleanExpression expression,
            List<HavingCondition> conditions,
            out string? error)
        {
            error = null;
            if (expression is BooleanBinaryExpression binary)
            {
                if (binary.BinaryExpressionType != BooleanBinaryExpressionType.And)
                {
                    error = "HAVING clause";
                    return false;
                }

                return TryCollectHavingConditions(binary.FirstExpression, conditions, out error)
                    && TryCollectHavingConditions(binary.SecondExpression, conditions, out error);
            }

            if (expression is not BooleanComparisonExpression comparison)
            {
                error = "HAVING clause";
                return false;
            }

            if (!TryGetOperator(comparison.ComparisonType, out var filterOperator))
            {
                error = "HAVING clause";
                return false;
            }

            if (!TryGetLiteral(comparison.SecondExpression, out var literal))
            {
                error = "HAVING clause";
                return false;
            }

            if (comparison.FirstExpression is ColumnReferenceExpression column)
            {
                if (GroupBy.Count == 0)
                {
                    error = "HAVING column requires GROUP BY";
                    return false;
                }

                if (!TryBuildFieldReference(column, InputStream, out var fieldReference, out error))
                {
                    return false;
                }

                if (!GroupBy.Any(group => FieldEquals(group, fieldReference)))
                {
                    error = "HAVING column must appear in GROUP BY";
                    return false;
                }

                conditions.Add(new HavingCondition(
                    new HavingOperand(HavingOperandKind.GroupField, fieldReference, null),
                    filterOperator,
                    literal));
                return true;
            }

            if (comparison.FirstExpression is FunctionCall functionCall)
            {
                if (IsWindowFunction(functionCall))
                {
                    error = "Window functions are only supported in GROUP BY.";
                    return false;
                }

                if (!TryBuildAggregate(functionCall, null, InputStream, out var aggregate, out error))
                {
                    return false;
                }

                AddAggregate(aggregate!);
                conditions.Add(new HavingCondition(
                    new HavingOperand(HavingOperandKind.Aggregate, null, aggregate),
                    filterOperator,
                    literal));
                return true;
            }

            error = "HAVING clause";
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
            value = default;

            switch (expression)
            {
                case IntegerLiteral integerLiteral
                    when double.TryParse(integerLiteral.Value, out var integer):
                    {
                        value = new FilterValue(FilterValueKind.Number, integer, string.Empty);

                        return true;
                    }

                case NumericLiteral numericLiteral
                    when double.TryParse(numericLiteral.Value, out var numeric):
                    {
                        value = new FilterValue(FilterValueKind.Number, numeric, string.Empty);
                        return true;
                    }

                case StringLiteral stringLiteral:
                    {
                        value = new FilterValue(FilterValueKind.String, 0, stringLiteral.Value);
                        return true;
                    }

                case NullLiteral:
                    {
                        value = new FilterValue(FilterValueKind.Null, 0, string.Empty);
                        return true;
                    }

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
            if (IsWindowFunctionName(functionName))
            {
                error = "Window functions are only supported in GROUP BY.";
                return false;
            }

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
                    if (functionCall.Parameters[0] is ColumnReferenceExpression col && col.ColumnType == ColumnType.Wildcard)
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

        private static bool IsWindowFunction(FunctionCall functionCall) =>
            IsWindowFunctionName(functionCall.FunctionName.Value);

        private static bool IsWindowFunctionName(string functionName) =>
            functionName.Equals("TUMBLINGWINDOW", StringComparison.OrdinalIgnoreCase) ||
            functionName.Equals("HOPPINGWINDOW", StringComparison.OrdinalIgnoreCase) ||
            functionName.Equals("SLIDINGWINDOW", StringComparison.OrdinalIgnoreCase);

        private static bool TryBuildWindowDefinition(
            FunctionCall functionCall,
            out WindowDefinition? windowDefinition,
            out string? error)
        {
            windowDefinition = null;
            error = null;

            if (!IsWindowFunction(functionCall))
            {
                error = "GROUP BY expression";
                return false;
            }

            if (!TryGetWindowParameters(functionCall, out var windowType, out var size, out var hop, out error))
            {
                return false;
            }

            windowDefinition = new WindowDefinition(windowType, size, hop);
            return true;
        }

        private static bool TryGetWindowParameters(
            FunctionCall functionCall,
            out WindowType windowType,
            out TimeSpan size,
            out TimeSpan? hop,
            out string? error)
        {
            windowType = WindowType.Tumbling;
            size = default;
            hop = null;
            error = null;

            var upperName = functionCall.FunctionName.Value.ToUpperInvariant();
            if (upperName == "TUMBLINGWINDOW")
            {
                windowType = WindowType.Tumbling;
                if (functionCall.Parameters.Count != 2)
                {
                    error = "TUMBLINGWINDOW expects (time_unit, size).";
                    return false;
                }

                if (!TryGetTimeUnit(functionCall.Parameters[0], out var unit, out error) ||
                    !TryGetWindowSize(functionCall.Parameters[1], unit, out size, out error))
                {
                    return false;
                }

                return true;
            }

            if (upperName == "HOPPINGWINDOW")
            {
                windowType = WindowType.Hopping;
                if (functionCall.Parameters.Count != 3)
                {
                    error = "HOPPINGWINDOW expects (time_unit, window_size, hop_size).";
                    return false;
                }

                if (!TryGetTimeUnit(functionCall.Parameters[0], out var unit, out error) ||
                    !TryGetWindowSize(functionCall.Parameters[1], unit, out size, out error) ||
                    !TryGetWindowSize(functionCall.Parameters[2], unit, out var hopSize, out error))
                {
                    return false;
                }

                hop = hopSize;
                return true;
            }

            if (upperName == "SLIDINGWINDOW")
            {
                windowType = WindowType.Sliding;
                if (functionCall.Parameters.Count != 2)
                {
                    error = "SLIDINGWINDOW expects (time_unit, window_size).";
                    return false;
                }

                if (!TryGetTimeUnit(functionCall.Parameters[0], out var unit, out error) ||
                    !TryGetWindowSize(functionCall.Parameters[1], unit, out size, out error))
                {
                    return false;
                }

                return true;
            }

            error = "Unsupported window function.";
            return false;
        }

        private static bool TryGetTimeUnit(
            ScalarExpression expression,
            out TimeSpan unit,
            out string? error)
        {
            unit = default;
            error = null;

            if (expression is not ColumnReferenceExpression column ||
                column.MultiPartIdentifier?.Identifiers is null ||
                column.MultiPartIdentifier.Identifiers.Count != 1)
            {
                error = "Window time unit must be an identifier.";
                return false;
            }

            var identifier = column.MultiPartIdentifier.Identifiers[0].Value;
            switch (identifier.ToLowerInvariant())
            {
                case "millisecond":
                    unit = TimeSpan.FromMilliseconds(1);
                    return true;
                case "second":
                    unit = TimeSpan.FromSeconds(1);
                    return true;
                case "minute":
                    unit = TimeSpan.FromMinutes(1);
                    return true;
                case "hour":
                    unit = TimeSpan.FromHours(1);
                    return true;
                case "day":
                    unit = TimeSpan.FromDays(1);
                    return true;
            }

            error = $"Unsupported time unit '{identifier}'.";
            return false;
        }

        private static bool TryGetWindowSize(
            ScalarExpression expression,
            TimeSpan unit,
            out TimeSpan size,
            out string? error)
        {
            size = default;
            error = null;

            if (!TryGetPositiveInteger(expression, out var value))
            {
                error = "Window size must be a positive integer literal.";
                return false;
            }

            try
            {
                size = TimeSpan.FromTicks(checked(unit.Ticks * value));
            }
            catch (OverflowException)
            {
                error = "Window size is too large.";
                return false;
            }

            return true;
        }

        private static bool TryGetPositiveInteger(ScalarExpression expression, out long value)
        {
            value = 0;
            if (expression is IntegerLiteral integerLiteral &&
                long.TryParse(integerLiteral.Value, out var integerValue) &&
                integerValue > 0)
            {
                value = integerValue;
                return true;
            }

            if (expression is NumericLiteral numericLiteral &&
                long.TryParse(numericLiteral.Value, out var numericValue) &&
                numericValue > 0)
            {
                value = numericValue;
                return true;
            }

            return false;
        }

        private void ValidateAggregates()
        {
            if (!HasAggregate)
            {
                if (Having is not null)
                {
                    Unsupported.Add("HAVING without aggregate");
                }

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

        private void AddAggregate(AggregateDefinition aggregate)
        {
            if (!Aggregates.Any(existing => AggregateEquals(existing, aggregate)))
            {
                Aggregates.Add(aggregate);
            }
        }

        private static bool FieldEquals(FieldReference left, FieldReference right) =>
            left.PathSegments.SequenceEqual(right.PathSegments, StringComparer.OrdinalIgnoreCase);

        private static bool AggregateEquals(AggregateDefinition left, AggregateDefinition right) =>
            left.Type == right.Type &&
            left.CountAll == right.CountAll &&
            ((left.Field is null && right.Field is null) ||
             (left.Field is not null && right.Field is not null && FieldEquals(left.Field, right.Field)));
    }
}

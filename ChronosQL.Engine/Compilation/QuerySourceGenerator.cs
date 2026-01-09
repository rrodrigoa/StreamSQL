using System.Globalization;
using System.Text;
using ChronosQL.Engine.Sql;

namespace ChronosQL.Engine.Compilation;

internal sealed class QuerySourceGenerator
{
    private readonly SqlPlan _plan;
    private readonly bool _follow;
    private readonly Dictionary<string, FieldReference> _fields = new(StringComparer.Ordinal);
    private readonly List<string> _fieldKeys = new();
    private int _methodIndex;

    public QuerySourceGenerator(SqlPlan plan, bool follow)
    {
        _plan = plan;
        _follow = follow;
        CollectFields();
    }

    public string Generate()
    {
        var builder = new StringBuilder();
        builder.AppendLine("using System;");
        builder.AppendLine("using System.Text.Json;");
        builder.AppendLine("using System.Collections.Generic;");
        builder.AppendLine("using System.Threading;");
        builder.AppendLine("using ChronosQL.Engine;");
        builder.AppendLine("using ChronosQL.Engine.Compilation;");
        builder.AppendLine();
        builder.AppendLine("namespace ChronosQL.Generated;");
        builder.AppendLine();
        builder.AppendLine("public static class QueryProgram");
        builder.AppendLine("{");
        builder.AppendLine("    private static readonly CompiledQueryDefinition Definition =");
        builder.AppendLine("        new CompiledQueryDefinition(");
        builder.AppendLine($"            Projection: {BuildProjectionDefinition()},");
        builder.AppendLine($"            SelectItems: {BuildSelectItemsDefinition()},");
        builder.AppendLine($"            GroupBy: {BuildGroupByDefinition()},");
        builder.AppendLine($"            Aggregates: {BuildAggregatesDefinition()},");
        builder.AppendLine($"            Filter: {BuildFilterDefinition()},");
        builder.AppendLine($"            Having: {BuildHavingDefinition()},");
        builder.AppendLine($"            Window: {BuildWindowDefinition()},");
        builder.AppendLine($"            OrderBy: {BuildOrderByDefinition()},");
        builder.AppendLine($"            TimestampBy: {BuildTimestampDefinition()},");
        builder.AppendLine($"            Follow: {_follow.ToString().ToLowerInvariant()});");
        builder.AppendLine();
        builder.AppendLine("    public static IAsyncEnumerable<JsonElement> Run(IAsyncEnumerable<InputEvent> input, CancellationToken cancellationToken)");
        builder.AppendLine("        => QueryRuntime.ExecuteAsync(Definition, input, cancellationToken);");
        builder.AppendLine();

        foreach (var entry in _fields)
        {
            BuildFieldAccessors(builder, entry.Key, entry.Value);
        }

        if (_plan.Filter is not null)
        {
            BuildFilterMethod(builder);
        }

        if (_plan.TimestampBy is not null)
        {
            BuildTimestampMethod(builder);
        }

        builder.AppendLine("}");
        return builder.ToString();
    }

    private void CollectFields()
    {
        foreach (var field in _plan.SelectItems.Select(item => item.Field).Where(field => field is not null))
        {
            AddField(field!);
        }

        foreach (var field in _plan.GroupBy)
        {
            AddField(field);
        }

        foreach (var aggregate in _plan.Aggregates)
        {
            if (aggregate.Field is not null)
            {
                AddField(aggregate.Field);
            }
        }

        if (_plan.Filter is not null)
        {
            foreach (var condition in _plan.Filter.Conditions)
            {
                AddField(condition.Field);
            }
        }

        if (_plan.Having is not null)
        {
            foreach (var condition in _plan.Having.Conditions)
            {
                if (condition.Operand.Field is not null)
                {
                    AddField(condition.Operand.Field);
                }
            }
        }

        if (_plan.TimestampBy?.Expression is TimestampFieldExpression timestampField)
        {
            AddField(timestampField.Field);
        }
    }

    private void AddField(FieldReference field)
    {
        var key = string.Join(".", field.PathSegments);
        if (!_fields.ContainsKey(key))
        {
            _fields[key] = field;
            _fieldKeys.Add(key);
        }
    }

    private string BuildProjectionDefinition()
    {
        if (_plan.Aggregates.Count > 0)
        {
            return "null";
        }

        if (_plan.SelectItems.Count == 0)
        {
            return "null";
        }

        var items = _plan.SelectItems
            .Where(item => item.Kind == SelectItemKind.Field)
            .Select(item => $"new CompiledProjectionField(\"{Escape(item.OutputName)}\", {GetFieldAccessorName(item.Field!)})")
            .ToArray();

        return $"new CompiledProjectionDefinition(new[] {{ {string.Join(", ", items)} }})";
    }

    private string BuildSelectItemsDefinition()
    {
        if (_plan.Aggregates.Count == 0)
        {
            var items = _plan.SelectItems
                .Where(item => item.Kind == SelectItemKind.Field)
                .Select(item => $"new CompiledSelectItem(CompiledSelectItemKind.Field, \"{Escape(item.OutputName)}\", -1, -1, {GetFieldAccessorName(item.Field!)})")
                .ToArray();

            return $"new[] {{ {string.Join(", ", items)} }}";
        }

        var aggregateItems = new List<string>();
        foreach (var item in _plan.SelectItems)
        {
            if (item.Kind == SelectItemKind.Field)
            {
                var index = GetGroupByIndex(item.Field!);
                aggregateItems.Add($"new CompiledSelectItem(CompiledSelectItemKind.Field, \"{Escape(item.OutputName)}\", {index}, -1, null)");
            }
            else
            {
                var index = _plan.Aggregates.IndexOf(item.Aggregate!);
                aggregateItems.Add($"new CompiledSelectItem(CompiledSelectItemKind.Aggregate, \"{Escape(item.OutputName)}\", -1, {index}, null)");
            }
        }

        return $"new[] {{ {string.Join(", ", aggregateItems)} }}";
    }

    private string BuildGroupByDefinition()
    {
        if (_plan.GroupBy.Count == 0)
        {
            return "null";
        }

        var fields = _plan.GroupBy
            .Select(field => GetFieldAccessorName(field))
            .ToArray();

        return $"new CompiledGroupByDefinition(new[] {{ {string.Join(", ", fields)} }})";
    }

    private string BuildAggregatesDefinition()
    {
        if (_plan.Aggregates.Count == 0)
        {
            return "Array.Empty<CompiledAggregateDefinition>()";
        }

        var aggregates = new List<string>();
        foreach (var aggregate in _plan.Aggregates)
        {
            var type = aggregate.Type switch
            {
                AggregateType.Count => "CompiledAggregateType.Count",
                AggregateType.Avg => "CompiledAggregateType.Avg",
                AggregateType.Min => "CompiledAggregateType.Min",
                AggregateType.Max => "CompiledAggregateType.Max",
                AggregateType.Sum => "CompiledAggregateType.Sum",
                _ => "CompiledAggregateType.Count"
            };

            var hasValue = aggregate.CountAll || aggregate.Field is null
                ? "null"
                : GetHasValueAccessorName(aggregate.Field);
            var numericGetter = aggregate.Field is null
                ? "null"
                : GetNumericAccessorName(aggregate.Field);

            aggregates.Add($"new CompiledAggregateDefinition({type}, \"{Escape(aggregate.OutputName)}\", {aggregate.CountAll.ToString().ToLowerInvariant()}, {hasValue}, {numericGetter})");
        }

        return $"new[] {{ {string.Join(", ", aggregates)} }}";
    }

    private string BuildFilterDefinition()
    {
        if (_plan.Filter is null)
        {
            return "null";
        }

        return "new CompiledFilterDefinition(MatchesFilter)";
    }

    private string BuildHavingDefinition()
    {
        if (_plan.Having is null)
        {
            return "null";
        }

        var conditions = new List<string>();
        foreach (var condition in _plan.Having.Conditions)
        {
            var operand = condition.Operand.Kind switch
            {
                HavingOperandKind.GroupField => $"new CompiledHavingOperand(CompiledHavingOperandKind.GroupField, {GetGroupByIndex(condition.Operand.Field!)})",
                HavingOperandKind.Aggregate => $"new CompiledHavingOperand(CompiledHavingOperandKind.Aggregate, {_plan.Aggregates.IndexOf(condition.Operand.Aggregate!)})",
                _ => "new CompiledHavingOperand(CompiledHavingOperandKind.GroupField, -1)"
            };

            var op = condition.Operator switch
            {
                FilterOperator.GreaterThan => "CompiledFilterOperator.GreaterThan",
                FilterOperator.LessThan => "CompiledFilterOperator.LessThan",
                FilterOperator.Equals => "CompiledFilterOperator.Equals",
                _ => "CompiledFilterOperator.Equals"
            };

            var value = BuildFilterValue(condition.Value);
            conditions.Add($"new CompiledHavingCondition({operand}, {op}, {value})");
        }

        return $"new CompiledHavingDefinition(new[] {{ {string.Join(", ", conditions)} }})";
    }

    private string BuildWindowDefinition()
    {
        if (_plan.Window is null)
        {
            return "null";
        }

        var type = _plan.Window.Type switch
        {
            WindowType.Tumbling => "CompiledWindowType.Tumbling",
            WindowType.Hopping => "CompiledWindowType.Hopping",
            WindowType.Sliding => "CompiledWindowType.Sliding",
            _ => "CompiledWindowType.Tumbling"
        };

        var sizeMs = (long)_plan.Window.Size.TotalMilliseconds;
        var slideMs = (long)(_plan.Window.Slide ?? _plan.Window.Size).TotalMilliseconds;
        return $"new CompiledWindowDefinition({type}, {sizeMs}, {slideMs})";
    }

    private string BuildOrderByDefinition()
    {
        if (_plan.OrderBy.Count == 0)
        {
            return "Array.Empty<CompiledOrderByDefinition>()";
        }

        var orderings = _plan.OrderBy.Select(order =>
        {
            var direction = order.Direction switch
            {
                SortDirection.Ascending => "CompiledSortDirection.Ascending",
                SortDirection.Descending => "CompiledSortDirection.Descending",
                _ => "CompiledSortDirection.Ascending"
            };

            return $"new CompiledOrderByDefinition(\"{Escape(order.OutputName)}\", {direction})";
        }).ToArray();

        return $"new[] {{ {string.Join(", ", orderings)} }}";
    }

    private string BuildTimestampDefinition()
    {
        if (_plan.TimestampBy is null)
        {
            return "null";
        }

        return "new CompiledTimestampByDefinition(ResolveTimestamp)";
    }

    private void BuildFieldAccessors(StringBuilder builder, string key, FieldReference field)
    {
        var accessorName = GetFieldAccessorName(field);
        builder.AppendLine($"    private static bool {accessorName}(JsonElement payload, out JsonElement value)");
        builder.AppendLine("    {");
        builder.AppendLine("        value = payload;");
        foreach (var segment in field.PathSegments)
        {
            builder.AppendLine($"        if (value.ValueKind != JsonValueKind.Object || !value.TryGetProperty(\"{Escape(segment)}\", out var next{_methodIndex}))");
            builder.AppendLine("        {");
            builder.AppendLine("            return false;");
            builder.AppendLine("        }");
            builder.AppendLine($"        value = next{_methodIndex};");
            _methodIndex++;
        }

        builder.AppendLine("        return true;");
        builder.AppendLine("    }");
        builder.AppendLine();

        var numericName = GetNumericAccessorName(field);
        builder.AppendLine($"    private static bool {numericName}(JsonElement payload, out double value)");
        builder.AppendLine("    {");
        builder.AppendLine("        value = 0;");
        builder.AppendLine($"        if (!{accessorName}(payload, out var numericValue))");
        builder.AppendLine("        {");
        builder.AppendLine("            return false;");
        builder.AppendLine("        }");
        builder.AppendLine();
        builder.AppendLine("        if (numericValue.ValueKind != JsonValueKind.Number || !numericValue.TryGetDouble(out var numeric))");
        builder.AppendLine("        {");
        builder.AppendLine("            return false;");
        builder.AppendLine("        }");
        builder.AppendLine();
        builder.AppendLine("        value = numeric;");
        builder.AppendLine("        return true;");
        builder.AppendLine("    }");
        builder.AppendLine();

        var hasValueName = GetHasValueAccessorName(field);
        builder.AppendLine($"    private static bool {hasValueName}(JsonElement payload)");
        builder.AppendLine("    {");
        builder.AppendLine($"        if (!{accessorName}(payload, out var current))");
        builder.AppendLine("        {");
        builder.AppendLine("            return false;");
        builder.AppendLine("        }");
        builder.AppendLine();
        builder.AppendLine("        return current.ValueKind != JsonValueKind.Null;");
        builder.AppendLine("    }");
        builder.AppendLine();
    }

    private void BuildFilterMethod(StringBuilder builder)
    {
        builder.AppendLine("    private static bool MatchesFilter(JsonElement payload)");
        builder.AppendLine("    {");
        var filterIndex = 0;
        foreach (var condition in _plan.Filter!.Conditions)
        {
            var accessorName = GetFieldAccessorName(condition.Field);
            var numericAccessor = GetNumericAccessorName(condition.Field);
            var comparison = BuildFilterComparison(condition, accessorName, numericAccessor, filterIndex);
            builder.AppendLine("        if (!(" + comparison + "))");
            builder.AppendLine("        {");
            builder.AppendLine("            return false;");
            builder.AppendLine("        }");
            filterIndex++;
        }

        builder.AppendLine();
        builder.AppendLine("        return true;");
        builder.AppendLine("    }");
        builder.AppendLine();
    }

    private void BuildTimestampMethod(StringBuilder builder)
    {
        builder.AppendLine("    private static long ResolveTimestamp(JsonElement element, long arrivalTime)");
        builder.AppendLine("    {");
        builder.AppendLine("        long timestamp;");
        builder.AppendLine("        string? error;" );
        builder.AppendLine("        if (TryResolveTimestamp(element, out timestamp, out error))");
        builder.AppendLine("        {");
        builder.AppendLine("            return timestamp;" );
        builder.AppendLine("        }");
        builder.AppendLine("        throw new InvalidOperationException(error ?? \"TIMESTAMP BY expression did not evaluate to a valid timestamp.\");");
        builder.AppendLine("    }");
        builder.AppendLine();

        builder.AppendLine("    private static bool TryResolveTimestamp(JsonElement element, out long timestamp, out string? error)");
        builder.AppendLine("    {");
        builder.AppendLine("        timestamp = 0;");
        builder.AppendLine("        error = null;");
        var expression = _plan.TimestampBy!.Expression;
        if (expression is TimestampFieldExpression fieldExpression)
        {
            var accessorName = GetFieldAccessorName(fieldExpression.Field);
            builder.AppendLine($"        if (!{accessorName}(element, out var value))");
            builder.AppendLine("        {");
            builder.AppendLine($"            error = \"TIMESTAMP BY field '{Escape(string.Join(".", fieldExpression.Field.PathSegments))}' was not found.\";");
            builder.AppendLine("            return false;");
            builder.AppendLine("        }");
            builder.AppendLine();
            builder.AppendLine("        return TryParseTimestampValue(value, out timestamp, out error);");
        }
        else if (expression is TimestampLiteralExpression literalExpression)
        {
            builder.AppendLine($"        return TryParseTimestampLiteral({BuildFilterValue(literalExpression.Value)}, out timestamp, out error);");
        }
        else
        {
            builder.AppendLine("        error = \"Unsupported TIMESTAMP BY expression.\";");
            builder.AppendLine("        return false;");
        }

        builder.AppendLine("    }");
        builder.AppendLine();

        builder.AppendLine("    private static bool TryParseTimestampLiteral(CompiledFilterValue literal, out long timestamp, out string? error)");
        builder.AppendLine("    {");
        builder.AppendLine("        timestamp = 0;");
        builder.AppendLine("        error = null;");
        builder.AppendLine("        return literal.Kind switch");
        builder.AppendLine("        {");
        builder.AppendLine("            CompiledFilterValueKind.Number => TryParseNumericTimestamp(literal.Number, out timestamp, out error),");
        builder.AppendLine("            CompiledFilterValueKind.String => TryParseStringTimestamp(literal.String, out timestamp, out error),");
        builder.AppendLine("            _ => FailTimestampParse(\"TIMESTAMP BY does not support NULL values.\", out error)");
        builder.AppendLine("        };" );
        builder.AppendLine("    }");
        builder.AppendLine();

        builder.AppendLine("    private static bool TryParseTimestampValue(JsonElement value, out long timestamp, out string? error)");
        builder.AppendLine("    {");
        builder.AppendLine("        timestamp = 0;");
        builder.AppendLine("        error = null;");
        builder.AppendLine();
        builder.AppendLine("        if (value.ValueKind == JsonValueKind.Number)");
        builder.AppendLine("        {");
        builder.AppendLine("            if (value.TryGetInt64(out var numeric))");
        builder.AppendLine("            {");
        builder.AppendLine("                timestamp = numeric;");
        builder.AppendLine("                return true;");
        builder.AppendLine("            }");
        builder.AppendLine();
        builder.AppendLine("            if (value.TryGetDouble(out var floating))");
        builder.AppendLine("            {");
        builder.AppendLine("                return TryParseNumericTimestamp(floating, out timestamp, out error);");
        builder.AppendLine("            }");
        builder.AppendLine("        }");
        builder.AppendLine();
        builder.AppendLine("        if (value.ValueKind == JsonValueKind.String)");
        builder.AppendLine("        {");
        builder.AppendLine("            return TryParseStringTimestamp(value.GetString(), out timestamp, out error);");
        builder.AppendLine("        }");
        builder.AppendLine();
        builder.AppendLine("        return FailTimestampParse(\"TIMESTAMP BY value must be an integer/long or ISO 8601 string.\", out error);");
        builder.AppendLine("    }");
        builder.AppendLine();

        builder.AppendLine("    private static bool TryParseNumericTimestamp(double numeric, out long timestamp, out string? error)");
        builder.AppendLine("    {");
        builder.AppendLine("        timestamp = 0;");
        builder.AppendLine("        error = null;");
        builder.AppendLine();
        builder.AppendLine("        if (Math.Abs(numeric % 1) > double.Epsilon)");
        builder.AppendLine("        {");
        builder.AppendLine("            return FailTimestampParse(\"TIMESTAMP BY numeric values must be integers.\", out error);");
        builder.AppendLine("        }");
        builder.AppendLine();
        builder.AppendLine("        timestamp = Convert.ToInt64(numeric);");
        builder.AppendLine("        return true;");
        builder.AppendLine("    }");
        builder.AppendLine();

        builder.AppendLine("    private static bool TryParseStringTimestamp(string? value, out long timestamp, out string? error)");
        builder.AppendLine("    {");
        builder.AppendLine("        timestamp = 0;");
        builder.AppendLine("        error = null;");
        builder.AppendLine();
        builder.AppendLine("        if (!string.IsNullOrWhiteSpace(value) && DateTimeOffset.TryParse(value, out var parsed))");
        builder.AppendLine("        {");
        builder.AppendLine("            timestamp = parsed.ToUnixTimeMilliseconds();");
        builder.AppendLine("            return true;");
        builder.AppendLine("        }");
        builder.AppendLine();
        builder.AppendLine("        return FailTimestampParse($\"TIMESTAMP BY value '{value}' is not a valid ISO 8601 timestamp.\", out error);");
        builder.AppendLine("    }");
        builder.AppendLine();

        builder.AppendLine("    private static bool FailTimestampParse(string message, out string? error)");
        builder.AppendLine("    {");
        builder.AppendLine("        error = message;");
        builder.AppendLine("        return false;");
        builder.AppendLine("    }");
        builder.AppendLine();
    }

    private string BuildFilterComparison(FilterCondition condition, string accessorName, string numericAccessor, int index)
    {
        if (condition.Value.Kind == FilterValueKind.Number)
        {
            var op = condition.Operator switch
            {
                FilterOperator.GreaterThan => ">",
                FilterOperator.LessThan => "<",
                FilterOperator.Equals => "==",
                _ => "=="
            };

            var numericName = $"numeric{index}";
            return $"{numericAccessor}(payload, out var {numericName}) && {numericName} {op} {condition.Value.Number.ToString(CultureInfo.InvariantCulture)}";
        }

        if (condition.Value.Kind == FilterValueKind.String)
        {
            var valueName = $"value{index}";
            return $"{accessorName}(payload, out var {valueName}) && {valueName}.ValueKind == JsonValueKind.String && string.Equals({valueName}.GetString(), \"{Escape(condition.Value.String)}\", StringComparison.Ordinal)";
        }

        var nullName = $"value{index}";
        return $"{accessorName}(payload, out var {nullName}) && {nullName}.ValueKind == JsonValueKind.Null";
    }

    private string BuildFilterValue(FilterValue value)
    {
        return value.Kind switch
        {
            FilterValueKind.Number => $"new CompiledFilterValue(CompiledFilterValueKind.Number, {value.Number.ToString(CultureInfo.InvariantCulture)}, string.Empty)",
            FilterValueKind.String => $"new CompiledFilterValue(CompiledFilterValueKind.String, 0, \"{Escape(value.String)}\")",
            _ => "new CompiledFilterValue(CompiledFilterValueKind.Null, 0, string.Empty)"
        };
    }

    private int GetGroupByIndex(FieldReference field)
    {
        for (var i = 0; i < _plan.GroupBy.Count; i++)
        {
            if (_plan.GroupBy[i].PathSegments.SequenceEqual(field.PathSegments, StringComparer.OrdinalIgnoreCase))
            {
                return i;
            }
        }

        return -1;
    }

    private int GetFieldIndex(FieldReference field)
    {
        var key = string.Join(".", field.PathSegments);
        return _fieldKeys.IndexOf(key);
    }

    private string GetFieldAccessorName(FieldReference field)
    {
        var index = GetFieldIndex(field);
        return $"TryGetField{index}";
    }

    private string GetNumericAccessorName(FieldReference field)
    {
        var index = GetFieldIndex(field);
        return $"TryGetNumeric{index}";
    }

    private string GetHasValueAccessorName(FieldReference field)
    {
        var index = GetFieldIndex(field);
        return $"HasValue{index}";
    }

    private static string Escape(string value)
        => value.Replace("\\", "\\\\", StringComparison.Ordinal).Replace("\"", "\\\"", StringComparison.Ordinal);
}

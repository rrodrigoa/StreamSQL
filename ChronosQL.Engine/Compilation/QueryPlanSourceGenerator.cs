using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using ChronosQL.Engine.Sql;

namespace ChronosQL.Engine.Compilation;

internal sealed class QueryPlanSourceGenerator
{
    private readonly SqlPlan _plan;
    private readonly string _prefix;
    private readonly Dictionary<string, FieldReference> _fieldMap = new(StringComparer.Ordinal);
    private readonly Dictionary<string, string> _accessorMap = new(StringComparer.Ordinal);
    private readonly Dictionary<SqlExpression, string> _expressionMap = new();
    private readonly List<Action<StringBuilder>> _predicateBuilders = new();
    private int _accessorIndex;
    private int _expressionIndex;
    private int _predicateIndex;

    public QueryPlanSourceGenerator(SqlPlan plan, string prefix)
    {
        _plan = plan;
        _prefix = prefix;
        CollectFields();
    }

    public string ExecuteMethodName => $"ExecuteAsync_{_prefix}";

    public void Generate(StringBuilder builder)
    {
        var createObservableName = Name("CreateObservable");
        var projectName = Name("Project");
        var resolveTimestampName = Name("ResolveTimestamp");
        var parseTimestampName = Name("ParseTimestamp");
        var matchesWhereName = Name("MatchesWhere");
        var executeName = ExecuteMethodName;

        builder.AppendLine($"    private static async Task {executeName}(IAsyncEnumerable<InputEvent> input, ChannelWriter<JsonElement> output, CancellationToken cancellationToken)");
        builder.AppendLine("    {");
        builder.AppendLine($"        IObservable<StreamEvent<JsonElement>> observable = {createObservableName}(input, cancellationToken);");
        builder.AppendLine("        IStreamable<Empty, JsonElement> stream = observable.ToStreamable();");
        builder.AppendLine($"        IStreamable<Empty, JsonElement> projected = stream.Select(payload => {projectName}(payload));");
        builder.AppendLine("        IObservable<StreamEvent<JsonElement>> outputObservable = projected.ToStreamEventObservable();");
        builder.AppendLine("        IObservable<JsonElement> dataEvents = outputObservable");
        if (_plan.WhereCondition is not null)
        {
            builder.AppendLine($"            .Where(streamEvent => !streamEvent.IsData || {matchesWhereName}(streamEvent.Payload))");
        }
        builder.AppendLine("            .Where(streamEvent => streamEvent.IsData)");
        builder.AppendLine("            .Select(streamEvent => streamEvent.Payload);");
        builder.AppendLine("        TaskCompletionSource<object?> completion = new(TaskCreationOptions.RunContinuationsAsynchronously);");
        builder.AppendLine("        using IDisposable subscription = dataEvents.Subscribe(");
        builder.AppendLine("            onNext: payload =>");
        builder.AppendLine("            {");
        builder.AppendLine("                output.TryWrite(payload);");
        builder.AppendLine("            },");
        builder.AppendLine("            onError: ex =>");
        builder.AppendLine("            {");
        builder.AppendLine("                output.TryComplete(ex);");
        builder.AppendLine("                completion.TrySetException(ex);");
        builder.AppendLine("            },");
        builder.AppendLine("            onCompleted: () =>");
        builder.AppendLine("            {");
        builder.AppendLine("                output.TryComplete();");
        builder.AppendLine("                completion.TrySetResult(null);");
        builder.AppendLine("            });");
        builder.AppendLine("        using CancellationTokenRegistration registration = cancellationToken.Register(() =>");
        builder.AppendLine("        {");
        builder.AppendLine("            OperationCanceledException exception = new(cancellationToken);");
        builder.AppendLine("            output.TryComplete(exception);");
        builder.AppendLine("            completion.TrySetCanceled(cancellationToken);");
        builder.AppendLine("        });");
        builder.AppendLine("        await completion.Task.ConfigureAwait(false);");
        builder.AppendLine("    }");
        builder.AppendLine();
        builder.AppendLine($"    private static IObservable<StreamEvent<JsonElement>> {createObservableName}(IAsyncEnumerable<InputEvent> input, CancellationToken cancellationToken)");
        builder.AppendLine("        => Observable.Create<StreamEvent<JsonElement>>(async (observer, ct) =>");
        builder.AppendLine("        {");
        builder.AppendLine("            try");
        builder.AppendLine("            {");
        builder.AppendLine("                await foreach (InputEvent item in input.WithCancellation(ct))");
        builder.AppendLine("                {");
        builder.AppendLine($"                    long timestamp = {resolveTimestampName}(item.Payload, item.ArrivalTime);");
        builder.AppendLine("                    observer.OnNext(StreamEvent.CreatePoint(timestamp, item.Payload));");
        builder.AppendLine("                }");
        builder.AppendLine();
        builder.AppendLine("                observer.OnNext(StreamEvent.CreatePunctuation<JsonElement>(StreamEvent.InfinitySyncTime));");
        builder.AppendLine("                observer.OnCompleted();");
        builder.AppendLine("            }");
        builder.AppendLine("            catch (Exception ex)");
        builder.AppendLine("            {");
        builder.AppendLine("                observer.OnError(ex);");
        builder.AppendLine("            }");
        builder.AppendLine("        });");
        builder.AppendLine();
        builder.AppendLine($"    private static JsonElement {projectName}(JsonElement payload)");
        builder.AppendLine("    {");
        if (_plan.SelectAll)
        {
            builder.AppendLine("        return payload;");
        }
        else
        {
            builder.AppendLine("        using MemoryStream stream = new();");
            builder.AppendLine("        using (Utf8JsonWriter writer = new(stream))");
            builder.AppendLine("        {");
            builder.AppendLine("            writer.WriteStartObject();");
            var fieldIndex = 0;
            foreach (var field in _plan.SelectFields)
            {
                string accessor = GetFieldAccessorName(field.Field);
                string outputName = Escape(field.OutputName);
                builder.AppendLine($"            writer.WritePropertyName(\"{outputName}\");");
                builder.AppendLine($"            if ({accessor}(payload, out JsonElement fieldValue{fieldIndex}))");
                builder.AppendLine("            {");
                builder.AppendLine($"                fieldValue{fieldIndex}.WriteTo(writer);");
                builder.AppendLine("            }");
                builder.AppendLine("            else");
                builder.AppendLine("            {");
                builder.AppendLine("                writer.WriteNullValue();");
                builder.AppendLine("            }");
                fieldIndex++;
            }
            builder.AppendLine("            writer.WriteEndObject();");
            builder.AppendLine("        }");
            builder.AppendLine("        stream.Position = 0;");
            builder.AppendLine("        using JsonDocument document = JsonDocument.Parse(stream);");
            builder.AppendLine("        return document.RootElement.Clone();");
        }
        builder.AppendLine("    }");
        builder.AppendLine();
        builder.AppendLine($"    private static long {resolveTimestampName}(JsonElement payload, long arrivalTime)");
        builder.AppendLine("    {");
        if (_plan.TimestampBy is null)
        {
            builder.AppendLine("        return arrivalTime;");
        }
        else
        {
            string accessor = GetFieldAccessorName(_plan.TimestampBy.Field);
            builder.AppendLine($"        if (!{accessor}(payload, out JsonElement timestampElement))");
            builder.AppendLine("        {");
            builder.AppendLine("            throw new InvalidOperationException(\"TIMESTAMP BY field was not found in the payload.\");");
            builder.AppendLine("        }");
            builder.AppendLine($"        return {parseTimestampName}(timestampElement);");
        }
        builder.AppendLine("    }");
        builder.AppendLine();
        builder.AppendLine($"    private static long {parseTimestampName}(JsonElement element)");
        builder.AppendLine("    {");
        builder.AppendLine("        switch (element.ValueKind)");
        builder.AppendLine("        {");
        builder.AppendLine("            case JsonValueKind.Number:");
        builder.AppendLine("                if (element.TryGetInt64(out long intValue))");
        builder.AppendLine("                {");
        builder.AppendLine("                    return intValue;");
        builder.AppendLine("                }");
        builder.AppendLine("                return (long)element.GetDouble();");
        builder.AppendLine("            case JsonValueKind.String:");
        builder.AppendLine("                string? text = element.GetString();");
        builder.AppendLine("                if (DateTimeOffset.TryParse(text, out DateTimeOffset parsed))");
        builder.AppendLine("                {");
        builder.AppendLine("                    return parsed.ToUnixTimeMilliseconds();");
        builder.AppendLine("                }");
        builder.AppendLine("                break;");
        builder.AppendLine("        }");
        builder.AppendLine("        throw new InvalidOperationException(\"TIMESTAMP BY requires a numeric or ISO-8601 string value.\");");
        builder.AppendLine("    }");
        builder.AppendLine();
        if (_plan.WhereCondition is not null)
        {
            var conditionExpression = BuildConditionExpression(_plan.WhereCondition);
            builder.AppendLine($"    private static bool {matchesWhereName}(JsonElement payload)");
            builder.AppendLine("    {");
            builder.AppendLine($"        return {conditionExpression};");
            builder.AppendLine("    }");
            builder.AppendLine();
            foreach (var predicateBuilder in _predicateBuilders)
            {
                predicateBuilder(builder);
            }
            AppendWhereHelpers(builder);
            foreach (var entry in _expressionMap)
            {
                BuildExpressionAccessor(builder, entry.Key, entry.Value);
            }
        }
        foreach (var entry in _fieldMap)
        {
            BuildFieldAccessor(builder, entry.Key, entry.Value);
        }
    }

    private string Name(string baseName) => $"{baseName}_{_prefix}";

    private void CollectFields()
    {
        foreach (var field in _plan.SelectFields.Select(item => item.Field))
        {
            AddField(field);
        }

        if (_plan.TimestampBy is not null)
        {
            AddField(_plan.TimestampBy.Field);
        }

        if (_plan.WhereCondition is not null)
        {
            CollectWhereFields(_plan.WhereCondition);
        }
    }

    private void AddField(FieldReference field)
    {
        var key = string.Join(".", field.PathSegments);
        if (!_fieldMap.ContainsKey(key))
        {
            _fieldMap[key] = field;
            _accessorMap[key] = $"TryGetField_{_prefix}_{_accessorIndex++}";
        }
    }

    private static string Escape(string value) => value.Replace("\\", "\\\\").Replace("\"", "\\\"");

    private string GetFieldAccessorName(FieldReference field)
    {
        var key = string.Join(".", field.PathSegments);
        return _accessorMap[key];
    }

    private string BuildConditionExpression(SqlCondition condition)
    {
        switch (condition)
        {
            case SqlBinaryCondition binary:
                var op = binary.Operator == SqlBinaryOperator.And ? "&&" : "||";
                return $"({BuildConditionExpression(binary.Left)} {op} {BuildConditionExpression(binary.Right)})";
            case SqlNotCondition notCondition:
                return $"!({BuildConditionExpression(notCondition.Condition)})";
            case SqlPredicateCondition predicateCondition:
                return BuildPredicateExpression(predicateCondition.Predicate);
            default:
                throw new InvalidOperationException($"Unsupported condition type: {condition.GetType().Name}");
        }
    }

    private string BuildPredicateExpression(SqlPredicate predicate)
    {
        var methodName = Name($"EvaluatePredicate_{_predicateIndex++}");
        _predicateBuilders.Add(builder => BuildPredicateMethod(builder, methodName, predicate));
        return $"{methodName}(payload)";
    }

    private void BuildPredicateMethod(StringBuilder builder, string methodName, SqlPredicate predicate)
    {
        builder.AppendLine($"    private static bool {methodName}(JsonElement payload)");
        builder.AppendLine("    {");
        switch (predicate)
        {
            case SqlComparisonPredicate comparison:
                WriteComparisonPredicate(builder, comparison);
                break;
            case SqlLikePredicate likePredicate:
                WriteLikePredicate(builder, likePredicate);
                break;
            case SqlBetweenPredicate betweenPredicate:
                WriteBetweenPredicate(builder, betweenPredicate);
                break;
            case SqlIsNullPredicate isNullPredicate:
                WriteIsNullPredicate(builder, isNullPredicate);
                break;
            case SqlInPredicate inPredicate:
                WriteInPredicate(builder, inPredicate);
                break;
            default:
                throw new InvalidOperationException($"Unsupported predicate type: {predicate.GetType().Name}");
        }
        builder.AppendLine("    }");
        builder.AppendLine();
    }

    private void WriteComparisonPredicate(StringBuilder builder, SqlComparisonPredicate predicate)
    {
        string leftAccessor = GetExpressionAccessorName(predicate.Left);
        string rightAccessor = GetExpressionAccessorName(predicate.Right);
        builder.AppendLine($"        if (!{leftAccessor}(payload, out SqlValue leftValue))");
        builder.AppendLine("        {");
        builder.AppendLine("            return false;");
        builder.AppendLine("        }");
        builder.AppendLine($"        if (!{rightAccessor}(payload, out SqlValue rightValue))");
        builder.AppendLine("        {");
        builder.AppendLine("            return false;");
        builder.AppendLine("        }");
        builder.AppendLine($"        return {GetComparisonMethod(predicate.Operator)}(leftValue, rightValue);");
    }

    private void WriteLikePredicate(StringBuilder builder, SqlLikePredicate predicate)
    {
        string expressionAccessor = GetExpressionAccessorName(predicate.Expression);
        string patternAccessor = GetExpressionAccessorName(predicate.Pattern);
        builder.AppendLine($"        if (!{expressionAccessor}(payload, out SqlValue expressionValue))");
        builder.AppendLine("        {");
        builder.AppendLine("            return false;");
        builder.AppendLine("        }");
        builder.AppendLine($"        if (!{patternAccessor}(payload, out SqlValue patternValue))");
        builder.AppendLine("        {");
        builder.AppendLine("            return false;");
        builder.AppendLine("        }");
        var resultExpression = "MatchesLike(expressionValue, patternValue)";
        if (predicate.Negated)
        {
            resultExpression = $"!{resultExpression}";
        }
        builder.AppendLine($"        return {resultExpression};");
    }

    private void WriteBetweenPredicate(StringBuilder builder, SqlBetweenPredicate predicate)
    {
        string expressionAccessor = GetExpressionAccessorName(predicate.Expression);
        string lowerAccessor = GetExpressionAccessorName(predicate.Lower);
        string upperAccessor = GetExpressionAccessorName(predicate.Upper);
        builder.AppendLine($"        if (!{expressionAccessor}(payload, out SqlValue expressionValue))");
        builder.AppendLine("        {");
        builder.AppendLine("            return false;");
        builder.AppendLine("        }");
        builder.AppendLine($"        if (!{lowerAccessor}(payload, out SqlValue lowerValue))");
        builder.AppendLine("        {");
        builder.AppendLine("            return false;");
        builder.AppendLine("        }");
        builder.AppendLine($"        if (!{upperAccessor}(payload, out SqlValue upperValue))");
        builder.AppendLine("        {");
        builder.AppendLine("            return false;");
        builder.AppendLine("        }");
        var resultExpression = "IsBetween(expressionValue, lowerValue, upperValue)";
        if (predicate.Negated)
        {
            resultExpression = $"!{resultExpression}";
        }
        builder.AppendLine($"        return {resultExpression};");
    }

    private void WriteIsNullPredicate(StringBuilder builder, SqlIsNullPredicate predicate)
    {
        string expressionAccessor = GetExpressionAccessorName(predicate.Expression);
        builder.AppendLine($"        if (!{expressionAccessor}(payload, out SqlValue expressionValue))");
        builder.AppendLine("        {");
        builder.AppendLine("            return false;");
        builder.AppendLine("        }");
        var resultExpression = "expressionValue.Kind == SqlValueKind.Null";
        if (predicate.Negated)
        {
            resultExpression = $"!({resultExpression})";
        }
        builder.AppendLine($"        return {resultExpression};");
    }

    private void WriteInPredicate(StringBuilder builder, SqlInPredicate predicate)
    {
        string expressionAccessor = GetExpressionAccessorName(predicate.Expression);
        builder.AppendLine($"        if (!{expressionAccessor}(payload, out SqlValue expressionValue))");
        builder.AppendLine("        {");
        builder.AppendLine("            return false;");
        builder.AppendLine("        }");
        for (var index = 0; index < predicate.Values.Count; index++)
        {
            var valueAccessor = GetExpressionAccessorName(predicate.Values[index]);
            builder.AppendLine($"        if (!{valueAccessor}(payload, out SqlValue inValue{index}))");
            builder.AppendLine("        {");
            builder.AppendLine("            return false;");
            builder.AppendLine("        }");
        }
        builder.AppendLine("        SqlValue[] values = new SqlValue[]");
        builder.AppendLine("        {");
        for (var index = 0; index < predicate.Values.Count; index++)
        {
            builder.AppendLine($"            inValue{index},");
        }
        builder.AppendLine("        };");
        var resultExpression = "IsIn(expressionValue, values)";
        if (predicate.Negated)
        {
            resultExpression = $"!{resultExpression}";
        }
        builder.AppendLine($"        return {resultExpression};");
    }

    private void AppendWhereHelpers(StringBuilder builder)
    {
        builder.AppendLine("    private enum SqlValueKind");
        builder.AppendLine("    {");
        builder.AppendLine("        Null,");
        builder.AppendLine("        Number,");
        builder.AppendLine("        String,");
        builder.AppendLine("        Boolean");
        builder.AppendLine("    }");
        builder.AppendLine();
        builder.AppendLine("    private readonly struct SqlValue");
        builder.AppendLine("    {");
        builder.AppendLine("        public SqlValueKind Kind { get; }");
        builder.AppendLine("        public double Number { get; }");
        builder.AppendLine("        public string? Text { get; }");
        builder.AppendLine("        public bool Boolean { get; }");
        builder.AppendLine();
        builder.AppendLine("        private SqlValue(SqlValueKind kind, double number = 0, string? text = null, bool boolean = false)");
        builder.AppendLine("        {");
        builder.AppendLine("            Kind = kind;");
        builder.AppendLine("            Number = number;");
        builder.AppendLine("            Text = text;");
        builder.AppendLine("            Boolean = boolean;");
        builder.AppendLine("        }");
        builder.AppendLine();
        builder.AppendLine("        public static SqlValue Null => new(SqlValueKind.Null);");
        builder.AppendLine("        public static SqlValue FromNumber(double number) => new(SqlValueKind.Number, number: number);");
        builder.AppendLine("        public static SqlValue FromString(string text) => new(SqlValueKind.String, text: text);");
        builder.AppendLine("        public static SqlValue FromBoolean(bool value) => new(SqlValueKind.Boolean, boolean: value);");
        builder.AppendLine("    }");
        builder.AppendLine();
        builder.AppendLine("    private static bool TryGetScalarValue(JsonElement element, out SqlValue value)");
        builder.AppendLine("    {");
        builder.AppendLine("        switch (element.ValueKind)");
        builder.AppendLine("        {");
        builder.AppendLine("            case JsonValueKind.Null:");
        builder.AppendLine("                value = SqlValue.Null;");
        builder.AppendLine("                return true;");
        builder.AppendLine("            case JsonValueKind.Number:");
        builder.AppendLine("                value = SqlValue.FromNumber(element.GetDouble());");
        builder.AppendLine("                return true;");
        builder.AppendLine("            case JsonValueKind.String:");
        builder.AppendLine("                value = SqlValue.FromString(element.GetString() ?? string.Empty);");
        builder.AppendLine("                return true;");
        builder.AppendLine("            case JsonValueKind.True:");
        builder.AppendLine("            case JsonValueKind.False:");
        builder.AppendLine("                value = SqlValue.FromBoolean(element.GetBoolean());");
        builder.AppendLine("                return true;");
        builder.AppendLine("            default:");
        builder.AppendLine("                value = default;");
        builder.AppendLine("                return false;");
        builder.AppendLine("        }");
        builder.AppendLine("    }");
        builder.AppendLine();
        builder.AppendLine("    private static bool TryCompare(SqlValue left, SqlValue right, out int result)");
        builder.AppendLine("    {");
        builder.AppendLine("        result = 0;");
        builder.AppendLine("        if (left.Kind != right.Kind)");
        builder.AppendLine("        {");
        builder.AppendLine("            return false;");
        builder.AppendLine("        }");
        builder.AppendLine("        switch (left.Kind)");
        builder.AppendLine("        {");
        builder.AppendLine("            case SqlValueKind.Number:");
        builder.AppendLine("                result = left.Number.CompareTo(right.Number);");
        builder.AppendLine("                return true;");
        builder.AppendLine("            case SqlValueKind.String:");
        builder.AppendLine("                result = string.CompareOrdinal(left.Text, right.Text);");
        builder.AppendLine("                return true;");
        builder.AppendLine("            default:");
        builder.AppendLine("                return false;");
        builder.AppendLine("        }");
        builder.AppendLine("    }");
        builder.AppendLine();
        builder.AppendLine("    private static bool CompareEqual(SqlValue left, SqlValue right)");
        builder.AppendLine("    {");
        builder.AppendLine("        if (left.Kind == SqlValueKind.Null || right.Kind == SqlValueKind.Null)");
        builder.AppendLine("        {");
        builder.AppendLine("            return false;");
        builder.AppendLine("        }");
        builder.AppendLine("        if (left.Kind != right.Kind)");
        builder.AppendLine("        {");
        builder.AppendLine("            return false;");
        builder.AppendLine("        }");
        builder.AppendLine("        return left.Kind switch");
        builder.AppendLine("        {");
        builder.AppendLine("            SqlValueKind.Number => left.Number.Equals(right.Number),");
        builder.AppendLine("            SqlValueKind.String => string.Equals(left.Text, right.Text, StringComparison.Ordinal),");
        builder.AppendLine("            SqlValueKind.Boolean => left.Boolean == right.Boolean,");
        builder.AppendLine("            _ => false");
        builder.AppendLine("        };");
        builder.AppendLine("    }");
        builder.AppendLine();
        builder.AppendLine("    private static bool CompareNotEqual(SqlValue left, SqlValue right)");
        builder.AppendLine("    {");
        builder.AppendLine("        if (left.Kind == SqlValueKind.Null || right.Kind == SqlValueKind.Null)");
        builder.AppendLine("        {");
        builder.AppendLine("            return false;");
        builder.AppendLine("        }");
        builder.AppendLine("        if (left.Kind != right.Kind)");
        builder.AppendLine("        {");
        builder.AppendLine("            return false;");
        builder.AppendLine("        }");
        builder.AppendLine("        return !CompareEqual(left, right);");
        builder.AppendLine("    }");
        builder.AppendLine();
        builder.AppendLine("    private static bool CompareGreaterThan(SqlValue left, SqlValue right)");
        builder.AppendLine("        => TryCompare(left, right, out int result) && result > 0;");
        builder.AppendLine();
        builder.AppendLine("    private static bool CompareGreaterThanOrEqual(SqlValue left, SqlValue right)");
        builder.AppendLine("        => TryCompare(left, right, out int result) && result >= 0;");
        builder.AppendLine();
        builder.AppendLine("    private static bool CompareLessThan(SqlValue left, SqlValue right)");
        builder.AppendLine("        => TryCompare(left, right, out int result) && result < 0;");
        builder.AppendLine();
        builder.AppendLine("    private static bool CompareLessThanOrEqual(SqlValue left, SqlValue right)");
        builder.AppendLine("        => TryCompare(left, right, out int result) && result <= 0;");
        builder.AppendLine();
        builder.AppendLine("    private static bool CompareNotGreaterThan(SqlValue left, SqlValue right)");
        builder.AppendLine("        => TryCompare(left, right, out int result) && result <= 0;");
        builder.AppendLine();
        builder.AppendLine("    private static bool CompareNotLessThan(SqlValue left, SqlValue right)");
        builder.AppendLine("        => TryCompare(left, right, out int result) && result >= 0;");
        builder.AppendLine();
        builder.AppendLine("    private static bool IsBetween(SqlValue value, SqlValue lower, SqlValue upper)");
        builder.AppendLine("        => TryCompare(value, lower, out int lowerCompare) && lowerCompare >= 0");
        builder.AppendLine("            && TryCompare(value, upper, out int upperCompare) && upperCompare <= 0;");
        builder.AppendLine();
        builder.AppendLine("    private static bool IsIn(SqlValue value, IReadOnlyList<SqlValue> values)");
        builder.AppendLine("    {");
        builder.AppendLine("        if (value.Kind == SqlValueKind.Null)");
        builder.AppendLine("        {");
        builder.AppendLine("            return false;");
        builder.AppendLine("        }");
        builder.AppendLine("        foreach (SqlValue candidate in values)");
        builder.AppendLine("        {");
        builder.AppendLine("            if (CompareEqual(value, candidate))");
        builder.AppendLine("            {");
        builder.AppendLine("                return true;");
        builder.AppendLine("            }");
        builder.AppendLine("        }");
        builder.AppendLine("        return false;");
        builder.AppendLine("    }");
        builder.AppendLine();
        builder.AppendLine("    private static bool MatchesLike(SqlValue value, SqlValue pattern)");
        builder.AppendLine("    {");
        builder.AppendLine("        if (value.Kind != SqlValueKind.String || pattern.Kind != SqlValueKind.String)");
        builder.AppendLine("        {");
        builder.AppendLine("            return false;");
        builder.AppendLine("        }");
        builder.AppendLine("        return Regex.IsMatch(value.Text ?? string.Empty, BuildLikePattern(pattern.Text ?? string.Empty), RegexOptions.Singleline);");
        builder.AppendLine("    }");
        builder.AppendLine();
        builder.AppendLine("    private static string BuildLikePattern(string pattern)");
        builder.AppendLine("        => \"^\" + Regex.Escape(pattern).Replace(\"%\", \".*\").Replace(\"_\", \".\") + \"$\";");
        builder.AppendLine();
    }

    private void BuildExpressionAccessor(StringBuilder builder, SqlExpression expression, string name)
    {
        builder.AppendLine($"    private static bool {name}(JsonElement payload, out SqlValue value)");
        builder.AppendLine("    {");
        switch (expression)
        {
            case SqlFieldExpression fieldExpression:
                string accessor = GetFieldAccessorName(fieldExpression.Field);
                builder.AppendLine($"        if (!{accessor}(payload, out JsonElement element))");
                builder.AppendLine("        {");
                builder.AppendLine("            value = SqlValue.Null;");
                builder.AppendLine("            return true;");
                builder.AppendLine("        }");
                builder.AppendLine("        return TryGetScalarValue(element, out value);");
                break;
            case SqlLiteralExpression literalExpression:
                builder.AppendLine($"        value = {GetLiteralValueExpression(literalExpression.Literal)};");
                builder.AppendLine("        return true;");
                break;
            default:
                throw new InvalidOperationException($"Unsupported expression type: {expression.GetType().Name}");
        }
        builder.AppendLine("    }");
        builder.AppendLine();
    }

    private string GetExpressionAccessorName(SqlExpression expression)
    {
        if (!_expressionMap.TryGetValue(expression, out string? name))
        {
            name = Name($"TryGetExpression_{_expressionIndex++}");
            _expressionMap[expression] = name;
        }
        return name;
    }

    private static string GetLiteralValueExpression(SqlLiteral literal)
        => literal.Kind switch
        {
            SqlLiteralKind.Null => "SqlValue.Null",
            SqlLiteralKind.String => $"SqlValue.FromString(\"{Escape(literal.Text ?? string.Empty)}\")",
            SqlLiteralKind.Number => $"SqlValue.FromNumber({literal.Number?.ToString(System.Globalization.CultureInfo.InvariantCulture) ?? "0"})",
            SqlLiteralKind.Boolean => $"SqlValue.FromBoolean({(literal.Boolean == true ? "true" : "false")})",
            _ => "SqlValue.Null"
        };

    private static string GetComparisonMethod(SqlComparisonOperator comparisonOperator)
        => comparisonOperator switch
        {
            SqlComparisonOperator.Equal => "CompareEqual",
            SqlComparisonOperator.NotEqual => "CompareNotEqual",
            SqlComparisonOperator.GreaterThan => "CompareGreaterThan",
            SqlComparisonOperator.GreaterThanOrEqual => "CompareGreaterThanOrEqual",
            SqlComparisonOperator.LessThan => "CompareLessThan",
            SqlComparisonOperator.LessThanOrEqual => "CompareLessThanOrEqual",
            SqlComparisonOperator.NotGreaterThan => "CompareNotGreaterThan",
            SqlComparisonOperator.NotLessThan => "CompareNotLessThan",
            _ => "CompareEqual"
        };

    private void CollectWhereFields(SqlCondition condition)
    {
        switch (condition)
        {
            case SqlBinaryCondition binary:
                CollectWhereFields(binary.Left);
                CollectWhereFields(binary.Right);
                break;
            case SqlNotCondition notCondition:
                CollectWhereFields(notCondition.Condition);
                break;
            case SqlPredicateCondition predicateCondition:
                CollectWhereFields(predicateCondition.Predicate);
                break;
        }
    }

    private void CollectWhereFields(SqlPredicate predicate)
    {
        switch (predicate)
        {
            case SqlComparisonPredicate comparison:
                CollectWhereFields(comparison.Left);
                CollectWhereFields(comparison.Right);
                break;
            case SqlLikePredicate likePredicate:
                CollectWhereFields(likePredicate.Expression);
                CollectWhereFields(likePredicate.Pattern);
                break;
            case SqlBetweenPredicate betweenPredicate:
                CollectWhereFields(betweenPredicate.Expression);
                CollectWhereFields(betweenPredicate.Lower);
                CollectWhereFields(betweenPredicate.Upper);
                break;
            case SqlIsNullPredicate isNullPredicate:
                CollectWhereFields(isNullPredicate.Expression);
                break;
            case SqlInPredicate inPredicate:
                CollectWhereFields(inPredicate.Expression);
                foreach (var value in inPredicate.Values)
                {
                    CollectWhereFields(value);
                }
                break;
        }
    }

    private void CollectWhereFields(SqlExpression expression)
    {
        if (expression is SqlFieldExpression fieldExpression)
        {
            AddField(fieldExpression.Field);
        }
    }

    private void BuildFieldAccessor(StringBuilder builder, string key, FieldReference field)
    {
        var accessorName = _accessorMap[key];
        builder.AppendLine($"    private static bool {accessorName}(JsonElement payload, out JsonElement value)");
        builder.AppendLine("    {");
        builder.AppendLine("        value = payload;");
        foreach (var segment in field.PathSegments)
        {
            string escaped = Escape(segment);
            builder.AppendLine("        if (value.ValueKind != JsonValueKind.Object)");
            builder.AppendLine("        {");
            builder.AppendLine("            return false;");
            builder.AppendLine("        }");
            builder.AppendLine($"        if (!value.TryGetProperty(\"{escaped}\", out value))");
            builder.AppendLine("        {");
            builder.AppendLine("            return false;");
            builder.AppendLine("        }");
        }
        builder.AppendLine("        return true;");
        builder.AppendLine("    }");
        builder.AppendLine();
    }
}

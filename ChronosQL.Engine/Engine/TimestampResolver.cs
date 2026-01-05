using System.Text.Json;
using ChronosQL.Engine.Sql;

namespace ChronosQL.Engine;

public static class TimestampResolver
{
    public static long ResolveTimestamp(JsonElement element, long arrivalTime, TimestampByDefinition? timestampBy)
    {
        if (timestampBy is null)
        {
            return arrivalTime;
        }

        if (TryResolveTimestamp(element, timestampBy.Expression, out var resolved, out var error))
        {
            return resolved;
        }

        throw new InvalidOperationException(error ?? "TIMESTAMP BY expression did not evaluate to a valid timestamp.");
    }

    public static bool TryResolveTimestamp(
        JsonElement element,
        TimestampExpression expression,
        out long timestamp,
        out string? error)
    {
        timestamp = 0;
        error = null;

        switch (expression)
        {
            case TimestampFieldExpression fieldExpression:
                if (!TrillPipelineBuilder.TryGetProperty(element, fieldExpression.Field.PathSegments, out var value))
                {
                    error = $"TIMESTAMP BY field '{string.Join('.', fieldExpression.Field.PathSegments)}' was not found.";
                    return false;
                }

                return TryParseTimestampValue(value, out timestamp, out error);
            case TimestampLiteralExpression literalExpression:
                return TryParseTimestampLiteral(literalExpression.Value, out timestamp, out error);
            default:
                error = "Unsupported TIMESTAMP BY expression.";
                return false;
        }
    }

    private static bool TryParseTimestampLiteral(FilterValue literal, out long timestamp, out string? error)
    {
        timestamp = 0;
        error = null;

        return literal.Kind switch
        {
            FilterValueKind.Number => TryParseNumericTimestamp(literal.Number, out timestamp, out error),
            FilterValueKind.String => TryParseStringTimestamp(literal.String, out timestamp, out error),
            _ => FailTimestampParse("TIMESTAMP BY does not support NULL values.", out error)
        };
    }

    private static bool TryParseTimestampValue(JsonElement value, out long timestamp, out string? error)
    {
        timestamp = 0;
        error = null;

        if (value.ValueKind == JsonValueKind.Number)
        {
            if (value.TryGetInt64(out var numeric))
            {
                timestamp = numeric;
                return true;
            }

            if (value.TryGetDouble(out var floating))
            {
                return TryParseNumericTimestamp(floating, out timestamp, out error);
            }
        }

        if (value.ValueKind == JsonValueKind.String)
        {
            return TryParseStringTimestamp(value.GetString(), out timestamp, out error);
        }

        return FailTimestampParse("TIMESTAMP BY value must be an integer/long or ISO 8601 string.", out error);
    }

    private static bool TryParseNumericTimestamp(double numeric, out long timestamp, out string? error)
    {
        timestamp = 0;
        error = null;

        if (Math.Abs(numeric % 1) > double.Epsilon)
        {
            return FailTimestampParse("TIMESTAMP BY numeric values must be integers.", out error);
        }

        timestamp = Convert.ToInt64(numeric);
        return true;
    }

    private static bool TryParseStringTimestamp(string? value, out long timestamp, out string? error)
    {
        timestamp = 0;
        error = null;

        if (!string.IsNullOrWhiteSpace(value) && DateTimeOffset.TryParse(value, out var parsed))
        {
            timestamp = parsed.ToUnixTimeMilliseconds();
            return true;
        }

        return FailTimestampParse($"TIMESTAMP BY value '{value}' is not a valid ISO 8601 timestamp.", out error);
    }

    private static bool FailTimestampParse(string message, out string? error)
    {
        error = message;
        return false;
    }
}

using System.Text.Json;
using ChronosQL.Engine.Sql;

namespace ChronosQL.Engine.Compilation;

public static class TimestampCompiler
{
    public static ResolveTimestampDelegate? Compile(TimestampByDefinition? timestampBy)
    {
        if (timestampBy is null)
        {
            return null;
        }

        return timestampBy.Expression switch
        {
            TimestampFieldExpression fieldExpression => CompileFieldTimestamp(fieldExpression.Field),
            TimestampLiteralExpression literalExpression => CompileLiteralTimestamp(literalExpression.Value),
            _ => throw new InvalidOperationException("Unsupported TIMESTAMP BY expression.")
        };
    }

    private static ResolveTimestampDelegate CompileFieldTimestamp(FieldReference field)
    {
        var segments = field.PathSegments.ToArray();
        return (JsonElement payload, long arrivalTime) =>
        {
            if (!TryGetProperty(payload, segments, out var value))
            {
                throw new InvalidOperationException($"TIMESTAMP BY field '{string.Join('.', segments)}' was not found.");
            }

            if (TryParseTimestampValue(value, out var timestamp, out var error))
            {
                return timestamp;
            }

            throw new InvalidOperationException(error ?? "TIMESTAMP BY expression did not evaluate to a valid timestamp.");
        };
    }

    private static ResolveTimestampDelegate CompileLiteralTimestamp(FilterValue literal)
    {
        if (TryParseTimestampLiteral(literal, out var timestamp, out var error))
        {
            return (_, _) => timestamp;
        }

        throw new InvalidOperationException(error ?? "TIMESTAMP BY expression did not evaluate to a valid timestamp.");
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

    private static bool TryGetProperty(JsonElement payload, IReadOnlyList<string> pathSegments, out JsonElement value)
    {
        value = payload;

        foreach (var segment in pathSegments)
        {
            if (value.ValueKind != JsonValueKind.Object || !value.TryGetProperty(segment, out var next))
            {
                return false;
            }

            value = next;
        }

        return true;
    }
}

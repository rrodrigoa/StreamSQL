using Microsoft.StreamProcessing;
using System.Text.Json;

namespace ChronosQL.Engine.Sql;

public static class SqlToTrillTranslator
{
    public static IStreamable<Empty, JsonElement> ApplyPlan(IStreamable<Empty, JsonElement> source, SqlPlan plan)
    {
        var filtered = plan.Filter is null
            ? source
            : source.Where(payload => MatchesFilter(payload, plan.Filter));

        if (plan.SelectItems.Count == 0)
        {
            return filtered;
        }

        var output =  filtered.Select(payload => ProjectPayload(payload, plan.SelectItems));
        return output;
    }

    private static bool MatchesFilter(JsonElement payload, FilterDefinition filter)
    {
        foreach (var condition in filter.Conditions)
        {
            if (!TryGetProperty(payload, condition.Field.PathSegments, out var value))
            {
                return false;
            }

            if (condition.Value.Kind != FilterValueKind.Number)
            {
                return false;
            }

            if (value.ValueKind != JsonValueKind.Number || !value.TryGetDouble(out var numeric))
            {
                return false;
            }

            var expected = condition.Value.Number;
            var match = condition.Operator switch
            {
                FilterOperator.GreaterThan => numeric > expected,
                FilterOperator.LessThan => numeric < expected,
                FilterOperator.Equals => numeric.Equals(expected),
                _ => false
            };

            if (!match)
            {
                return false;
            }
        }

        return true;
    }

    private static JsonElement ProjectPayload(JsonElement payload, IReadOnlyList<SelectItem> fields)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream))
        {
            writer.WriteStartObject();

            foreach (var field in fields)
            {
                if (field.Kind != SelectItemKind.Field)
                {
                    continue;
                }

                var reference = field.Field!;
                if (!TryGetProperty(payload, reference.PathSegments, out var value))
                {
                    continue;
                }

                writer.WritePropertyName(field.OutputName);
                value.WriteTo(writer);
            }

            writer.WriteEndObject();
        }

        stream.Position = 0;
        using var document = JsonDocument.Parse(stream);
        var clonedObject = document.RootElement.Clone();
        return clonedObject;
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

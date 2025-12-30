using Microsoft.StreamProcessing;
using System.Text.Json;

namespace StreamSql.Sql;

public static class SqlToTrillTranslator
{
    public static IStreamable<Empty, JsonElement> ApplyPlan(IStreamable<Empty, JsonElement> source, SqlPlan plan)
    {
        var filtered = plan.Filter is null
            ? source
            : source.Where(payload => MatchesFilter(payload, plan.Filter));

        if (plan.SelectedFields.Count == 0)
        {
            return filtered;
        }

        var output =  filtered.Select(payload => ProjectPayload(payload, plan.SelectedFields));
        return output;
    }

    private static bool MatchesFilter(JsonElement payload, FilterDefinition filter)
    {
        if (!TryGetProperty(payload, filter.Field.PathSegments, out var value))
        {
            return false;
        }

        if (value.ValueKind != JsonValueKind.Number || !value.TryGetDouble(out var numeric))
        {
            return false;
        }

        return filter.Operator switch
        {
            FilterOperator.GreaterThan => numeric > filter.Value,
            _ => false
        };
    }

    private static JsonElement ProjectPayload(JsonElement payload, IReadOnlyList<SelectedField> fields)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream))
        {
            writer.WriteStartObject();

            foreach (var field in fields)
            {
                if (!TryGetProperty(payload, field.Source.PathSegments, out var value))
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

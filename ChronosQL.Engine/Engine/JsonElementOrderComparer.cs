using System;
using System.Collections.Generic;
using System.Text.Json;
using ChronosQL.Engine.Compilation;
using ChronosQL.Engine.Sql;

namespace ChronosQL.Engine;

internal sealed class JsonElementOrderComparer : IComparer<JsonElement>
{
    private readonly IReadOnlyList<CompiledOrderByDefinition> _orderBy;

    public JsonElementOrderComparer(IReadOnlyList<CompiledOrderByDefinition> orderBy)
    {
        _orderBy = orderBy;
    }

    public JsonElementOrderComparer(IReadOnlyList<OrderByDefinition> orderBy)
    {
        _orderBy = orderBy.Select(definition => new CompiledOrderByDefinition(
            definition.OutputName,
            definition.Direction == SortDirection.Descending
                ? CompiledSortDirection.Descending
                : CompiledSortDirection.Ascending)).ToList();
    }

    public int Compare(JsonElement left, JsonElement right)
    {
        foreach (var orderBy in _orderBy)
        {
            var leftValue = TryGetOrderValue(left, orderBy.OutputName, out var leftElement)
                ? leftElement
                : default;
            var rightValue = TryGetOrderValue(right, orderBy.OutputName, out var rightElement)
                ? rightElement
                : default;

            var comparison = CompareJsonValues(leftValue, rightValue);
            if (comparison == 0)
            {
                continue;
            }

            return orderBy.Direction == CompiledSortDirection.Descending ? -comparison : comparison;
        }

        return 0;
    }

    private static int CompareJsonValues(JsonElement left, JsonElement right)
    {
        var leftNull = left.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined;
        var rightNull = right.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined;

        if (leftNull && rightNull)
        {
            return 0;
        }

        if (leftNull)
        {
            return -1;
        }

        if (rightNull)
        {
            return 1;
        }

        if (left.ValueKind == JsonValueKind.Number && right.ValueKind == JsonValueKind.Number)
        {
            left.TryGetDouble(out var leftNumber);
            right.TryGetDouble(out var rightNumber);
            return leftNumber.CompareTo(rightNumber);
        }

        if (left.ValueKind == JsonValueKind.String && right.ValueKind == JsonValueKind.String)
        {
            return string.CompareOrdinal(left.GetString(), right.GetString());
        }

        if (left.ValueKind is JsonValueKind.True or JsonValueKind.False &&
            right.ValueKind is JsonValueKind.True or JsonValueKind.False)
        {
            var leftBool = left.ValueKind == JsonValueKind.True;
            var rightBool = right.ValueKind == JsonValueKind.True;
            return leftBool.CompareTo(rightBool);
        }

        return GetSortRank(left.ValueKind).CompareTo(GetSortRank(right.ValueKind));
    }

    private static bool TryGetOrderValue(JsonElement element, string propertyName, out JsonElement value)
    {
        value = default;
        if (element.ValueKind != JsonValueKind.Object)
        {
            return false;
        }

        if (element.TryGetProperty(propertyName, out value))
        {
            return true;
        }

        foreach (var property in element.EnumerateObject())
        {
            if (property.NameEquals(propertyName) ||
                property.Name.Equals(propertyName, StringComparison.OrdinalIgnoreCase))
            {
                value = property.Value;
                return true;
            }
        }

        return false;
    }

    private static int GetSortRank(JsonValueKind kind) =>
        kind switch
        {
            JsonValueKind.Null => 0,
            JsonValueKind.False => 1,
            JsonValueKind.True => 2,
            JsonValueKind.Number => 3,
            JsonValueKind.String => 4,
            JsonValueKind.Object => 5,
            JsonValueKind.Array => 6,
            _ => 7
        };
}

using System.Text.Json;
using ChronosQL.Engine.Sql;

namespace ChronosQL.Engine.Compilation;

public static class JoinCompiler
{
    public static CompiledJoinDefinition Compile(JoinDefinition join)
    {
        var leftAlias = string.IsNullOrWhiteSpace(join.LeftSource.Alias) ? join.LeftSource.Name : join.LeftSource.Alias!;
        var rightAlias = string.IsNullOrWhiteSpace(join.RightSource.Alias) ? join.RightSource.Name : join.RightSource.Alias!;
        var constraint = join.TemporalConstraint is null
            ? null
            : new CompiledTemporalJoinConstraint(join.TemporalConstraint.Unit, join.TemporalConstraint.MinDelta, join.TemporalConstraint.MaxDelta);

        return new CompiledJoinDefinition(
            leftAlias,
            rightAlias,
            constraint,
            BuildJoinKeyAccessor(join.LeftKey),
            BuildJoinKeyAccessor(join.RightKey),
            TimestampCompiler.Compile(join.LeftSource.TimestampBy),
            TimestampCompiler.Compile(join.RightSource.TimestampBy));
    }

    private static TryGetJoinKeyDelegate BuildJoinKeyAccessor(FieldReference key)
    {
        var segments = key.PathSegments.ToArray();
        return (JsonElement payload, out string joinKey) =>
        {
            joinKey = string.Empty;
            if (!TryGetProperty(payload, segments, out var value))
            {
                return false;
            }

            joinKey = value.GetRawText();
            return true;
        };
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

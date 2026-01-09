using System.Text.Json;

namespace ChronosQL.Engine.Compilation;

public delegate bool TryGetJoinKeyDelegate(JsonElement payload, out string joinKey);

public sealed record CompiledTemporalJoinConstraint(TimeSpan Unit, long MinDelta, long MaxDelta);

public sealed record CompiledJoinDefinition(
    string LeftAlias,
    string RightAlias,
    CompiledTemporalJoinConstraint? TemporalConstraint,
    TryGetJoinKeyDelegate LeftKey,
    TryGetJoinKeyDelegate RightKey,
    ResolveTimestampDelegate? LeftTimestamp,
    ResolveTimestampDelegate? RightTimestamp);

using System.Text.Json;

namespace ChronosQL.Engine;

public readonly record struct InputEvent(JsonElement Payload, long ArrivalTime);

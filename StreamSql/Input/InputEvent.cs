using System.Text.Json;

namespace StreamSql.Input;

public readonly record struct InputEvent(JsonElement Payload, long ArrivalTime);

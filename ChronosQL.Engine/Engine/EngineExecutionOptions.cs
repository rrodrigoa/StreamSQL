namespace ChronosQL.Engine;

public sealed record EngineExecutionOptions
{
    public string TimestampField { get; init; } = "timestamp";
    public bool Follow { get; init; }
}

namespace StreamSql.Cli;

public sealed record WindowDefinition(WindowType Type, TimeSpan Size, TimeSpan? Slide);

public enum WindowType
{
    Tumbling,
    Rolling,
    Sliding
}

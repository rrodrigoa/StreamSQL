namespace StreamSql.Cli;

public sealed class CommandLineOptions
{
    public string? QueryText { get; init; }
    public string? QueryFilePath { get; init; }
    public string? InputFilePath { get; init; }
    public bool Follow { get; init; }
    public string? OutputFilePath { get; init; }
    public string? EventTimeField { get; init; }
    public WindowDefinition? Window { get; init; }

    public static bool TryParse(string[] args, out CommandLineOptions? options, out string? error)
    {
        options = null;
        error = null;

        if (args.Length == 0)
        {
            error = "Usage: streamsql [--query \"SQL\"] [--file path] [--follow] [--out path] [--timestamp-by field] [--window tumbling:<duration>|sliding:<size>,<slide>] [--tumbling-window duration] <query.sql>";
            return false;
        }

        string? queryText = null;
        string? queryFilePath = null;
        string? inputFilePath = null;
        string? outputFilePath = null;
        string? eventTimeField = null;
        TimeSpan? tumblingWindow = null;
        WindowDefinition? windowDefinition = null;
        var follow = false;

        var remaining = new List<string>();

        for (var i = 0; i < args.Length; i++)
        {
            var arg = args[i];
            switch (arg)
            {
                case "--query":
                    if (!TryReadValue(args, ref i, out queryText, out error))
                    {
                        return false;
                    }
                    break;
                case "--file":
                    if (!TryReadValue(args, ref i, out inputFilePath, out error))
                    {
                        return false;
                    }
                    break;
                case "--out":
                    if (!TryReadValue(args, ref i, out outputFilePath, out error))
                    {
                        return false;
                    }
                    break;
                case "--event-time":
                case "--timestamp-by":
                    if (!TryReadValue(args, ref i, out var candidateField, out error))
                    {
                        return false;
                    }
                    if (!string.IsNullOrWhiteSpace(eventTimeField))
                    {
                        error = "Only one of --event-time or --timestamp-by can be specified.";
                        return false;
                    }
                    eventTimeField = candidateField;
                    break;
                case "--tumbling-window":
                    if (!TryReadValue(args, ref i, out var rawWindow, out error))
                    {
                        return false;
                    }
                    if (!TryParseWindow(rawWindow!, out tumblingWindow, out error))
                    {
                        return false;
                    }
                    break;
                case "--window":
                    if (!TryReadValue(args, ref i, out var rawDefinition, out error))
                    {
                        return false;
                    }
                    if (!TryParseWindowDefinition(rawDefinition!, out windowDefinition, out error))
                    {
                        return false;
                    }
                    break;
                case "--follow":
                    follow = true;
                    break;
                default:
                    remaining.Add(arg);
                    break;
            }
        }

        if (follow && string.IsNullOrWhiteSpace(inputFilePath))
        {
            error = "--follow can only be used with --file.";
            return false;
        }

        if (string.IsNullOrWhiteSpace(queryText))
        {
            if (remaining.Count == 0)
            {
                error = "Missing query SQL. Provide --query or the path to a .sql file.";
                return false;
            }

            queryFilePath = remaining.Last();
        }

        options = new CommandLineOptions
        {
            QueryText = queryText,
            QueryFilePath = queryFilePath,
            InputFilePath = inputFilePath,
            Follow = follow,
            OutputFilePath = outputFilePath,
            EventTimeField = eventTimeField,
            Window = windowDefinition ?? (tumblingWindow is null
                ? null
                : new WindowDefinition(WindowType.Tumbling, tumblingWindow.Value, null))
        };

        return true;
    }

    private static bool TryReadValue(string[] args, ref int index, out string? value, out string? error)
    {
        error = null;
        value = null;

        if (index + 1 >= args.Length)
        {
            error = $"Missing value for {args[index]}.";
            return false;
        }

        value = args[++index];
        return true;
    }

    private static bool TryParseWindow(string value, out TimeSpan? window, out string? error)
    {
        error = null;
        window = null;

        if (long.TryParse(value, out var milliseconds) && milliseconds > 0)
        {
            window = TimeSpan.FromMilliseconds(milliseconds);
            return true;
        }

        if (TimeSpan.TryParse(value, out var parsed) && parsed > TimeSpan.Zero)
        {
            window = parsed;
            return true;
        }

        error = "Invalid tumbling window duration. Provide a positive millisecond value or a TimeSpan.";
        return false;
    }

    private static bool TryParseWindowDefinition(string value, out WindowDefinition? windowDefinition, out string? error)
    {
        error = null;
        windowDefinition = null;

        var parts = value.Split(':', 2, StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length != 2)
        {
            error = "Invalid window definition. Use tumbling:<duration> or sliding:<size>,<slide>.";
            return false;
        }

        if (parts[0].Equals("tumbling", StringComparison.OrdinalIgnoreCase))
        {
            if (!TryParseDuration(parts[1], out var size))
            {
                error = "Invalid tumbling window duration.";
                return false;
            }

            windowDefinition = new WindowDefinition(WindowType.Tumbling, size, null);
            return true;
        }

        if (parts[0].Equals("sliding", StringComparison.OrdinalIgnoreCase))
        {
            var durations = parts[1].Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            if (durations.Length != 2 ||
                !TryParseDuration(durations[0], out var size) ||
                !TryParseDuration(durations[1], out var slide))
            {
                error = "Invalid sliding window definition. Use sliding:<size>,<slide>.";
                return false;
            }

            windowDefinition = new WindowDefinition(WindowType.Sliding, size, slide);
            return true;
        }

        error = "Unsupported window type. Use tumbling or sliding.";
        return false;
    }

    private static bool TryParseDuration(string raw, out TimeSpan duration)
    {
        duration = default;
        if (string.IsNullOrWhiteSpace(raw))
        {
            return false;
        }

        if (raw.EndsWith("ms", StringComparison.OrdinalIgnoreCase) &&
            long.TryParse(raw[..^2], out var milliseconds) &&
            milliseconds > 0)
        {
            duration = TimeSpan.FromMilliseconds(milliseconds);
            return true;
        }

        if (raw.EndsWith("s", StringComparison.OrdinalIgnoreCase) &&
            long.TryParse(raw[..^1], out var seconds) &&
            seconds > 0)
        {
            duration = TimeSpan.FromSeconds(seconds);
            return true;
        }

        if (raw.EndsWith("m", StringComparison.OrdinalIgnoreCase) &&
            long.TryParse(raw[..^1], out var minutes) &&
            minutes > 0)
        {
            duration = TimeSpan.FromMinutes(minutes);
            return true;
        }

        if (raw.EndsWith("h", StringComparison.OrdinalIgnoreCase) &&
            long.TryParse(raw[..^1], out var hours) &&
            hours > 0)
        {
            duration = TimeSpan.FromHours(hours);
            return true;
        }

        if (long.TryParse(raw, out var numericMs) && numericMs > 0)
        {
            duration = TimeSpan.FromMilliseconds(numericMs);
            return true;
        }

        if (TimeSpan.TryParse(raw, out var parsed) && parsed > TimeSpan.Zero)
        {
            duration = parsed;
            return true;
        }

        return false;
    }
}

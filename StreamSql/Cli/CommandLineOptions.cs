namespace StreamSql.Cli;

public sealed class CommandLineOptions
{
    public string? QueryText { get; init; }
    public string? QueryFilePath { get; init; }
    public string? InputFilePath { get; init; }
    public bool Follow { get; init; }
    public string? OutputFilePath { get; init; }
    public string? EventTimeField { get; init; }

    public static bool TryParse(string[] args, out CommandLineOptions? options, out string? error)
    {
        options = null;
        error = null;

        if (args.Length == 0)
        {
            error = "Usage: streamsql [--query \"SQL\"] [--file path] [--follow] [--out path] [--timestamp-by field] <query.sql>";
            return false;
        }

        string? queryText = null;
        string? queryFilePath = null;
        string? inputFilePath = null;
        string? outputFilePath = null;
        string? eventTimeField = null;
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
            EventTimeField = eventTimeField
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

}

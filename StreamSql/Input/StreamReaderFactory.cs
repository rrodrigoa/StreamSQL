using StreamSql.Cli;

namespace StreamSql.Input;

public static class StreamReaderFactory
{
    public static Stream? InputOverride { get; set; }
    public static Stream? OutputOverride { get; set; }

    public static Stream OpenInput(CommandLineOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.InputFilePath))
        {
            if (InputOverride is not null)
            {
                return InputOverride;
            }

            return Console.OpenStandardInput();
        }

        return new FileStream(options.InputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
    }

    public static Stream OpenOutput(CommandLineOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.OutputFilePath))
        {
            if (OutputOverride is not null)
            {
                return OutputOverride;
            }

            return Console.OpenStandardOutput();
        }

        return new FileStream(options.OutputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read);
    }
}

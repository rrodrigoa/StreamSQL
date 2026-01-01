using ChronosQL.Engine;
using StreamSql.Cli;
using StreamSql.Input;
using StreamSql.Output;

namespace StreamSql;

public static class Program
{
    public static async Task<int> Main(string[] args)
    {
        if (!CommandLineOptions.TryParse(args, out var options, out var error))
        {
            Console.Error.WriteLine(error);
            return 1;
        }

        if (options is null)
        {
            Console.Error.WriteLine("No command line options were provided.");
            return 1;
        }

        var sqlText = options.QueryText ?? await File.ReadAllTextAsync(options.QueryFilePath!);
        await using var inputStream = StreamReaderFactory.OpenInput(options);
        var jsonReader = new JsonLineReader(inputStream, options.Follow);

        await using var outputStream = StreamReaderFactory.OpenOutput(options);
        var jsonWriter = new JsonLineWriter(outputStream);

        var engine = new ChronosQLEngine(new EngineExecutionOptions
        {
            TimestampField = options.EventTimeField ?? "timestamp",
            Follow = options.Follow,
            Window = options.Window
        });
        var results = engine.ExecuteAsync(sqlText, jsonReader.ReadAllAsync());

        await jsonWriter.WriteAllAsync(results);
        return 0;
    }
}

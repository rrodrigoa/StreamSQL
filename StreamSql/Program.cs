using StreamSql.Cli;
using StreamSql.Engine;
using StreamSql.Input;
using StreamSql.Output;
using StreamSql.Sql;

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
        var plan = SqlParser.Parse(sqlText);

        await using var inputStream = StreamReaderFactory.OpenInput(options);
        var jsonReader = new JsonLineReader(inputStream, options.Follow);

        await using var outputStream = StreamReaderFactory.OpenOutput(options);
        var jsonWriter = new JsonLineWriter(outputStream);

        var timestampField = options.EventTimeField ?? "timestamp";
        var pipeline = new TrillPipelineBuilder(
            timestampField,
            options.Follow,
            plan,
            options.Window);
        var results = pipeline.ExecuteAsync(jsonReader.ReadAllAsync());

        await jsonWriter.WriteAllAsync(results);
        return 0;
    }
}

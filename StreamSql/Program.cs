using ChronosQL.Engine;
using ChronosQL.Engine.Sql;
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
        var engine = new ChronosQLEngine(new EngineExecutionOptions
        {
            Follow = options.ReadMode != InputReadMode.Normal
        });

        SqlPlan plan;
        try
        {
            plan = engine.Parse(sqlText);
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine(ex.Message);
            return 1;
        }

        if (!options.Inputs.TryGetValue(plan.InputName, out var inputSource))
        {
            Console.Error.WriteLine($"SELECT references unknown input '{plan.InputName}'.");
            return 1;
        }

        if (!options.Outputs.TryGetValue(plan.OutputName, out var outputDestination))
        {
            Console.Error.WriteLine($"SELECT references unknown output '{plan.OutputName}'.");
            return 1;
        }

        using var shutdown = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            shutdown.Cancel();
        };

        try
        {
            await using var inputStream = StreamReaderFactory.OpenInput(inputSource);
            await using var outputStream = StreamReaderFactory.OpenOutput(outputDestination);
            var reader = new JsonLineReader(inputStream, options.ReadMode, inputSource.Path);
            var writer = new JsonLineWriter(outputStream);

            if (options.ReadMode == InputReadMode.Normal)
            {
                var inputEvents = await reader.ReadAllToListAsync(shutdown.Token);
                var results = await engine.ExecuteBatchAsync(plan, inputEvents, shutdown.Token);
                await writer.WriteAllAsync(results, shutdown.Token);
            }
            else
            {
                var results = engine.ExecuteAsync(plan, reader.ReadAllAsync(shutdown.Token), shutdown.Token);
                await writer.WriteAllAsync(results, shutdown.Token);
            }
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine(ex.Message);
            return 1;
        }

        return 0;
    }
}

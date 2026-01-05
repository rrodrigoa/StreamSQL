using ChronosQL.Engine;
using StreamSql.Cli;
using StreamSql.Execution;
using StreamSql.Planning;

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
            Follow = options.Follow
        });
        ScriptDomPlan scriptPlan;
        try
        {
            scriptPlan = ScriptDomParser.Parse(sqlText);
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine(ex.Message);
            return 1;
        }

        using var shutdown = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            shutdown.Cancel();
        };

        if (!StreamGraphPlanner.TryPlan(scriptPlan, options, out var graphPlan, out var validationError))
        {
            Console.Error.WriteLine(validationError);
            return 1;
        }

        var executionEngine = new StreamExecutionEngine(engine, options.Follow);
        try
        {
            await executionEngine.ExecuteAsync(
                graphPlan!,
                options.Inputs,
                options.Outputs,
                shutdown.Token);
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine(ex.Message);
            return 1;
        }

        return 0;
    }

}

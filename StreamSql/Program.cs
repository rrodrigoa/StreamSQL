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
            Follow = options.Follow
        });
        SqlScriptPlan scriptPlan;
        try
        {
            scriptPlan = engine.ParseScript(sqlText);
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine(ex.Message);
            return 1;
        }

        using var shutdown = new CancellationTokenSource();
        var stopRequested = false;

        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            stopRequested = true;
            shutdown.Cancel();
        };

        if (!TryValidateBindings(scriptPlan, options, out var validationError))
        {
            Console.Error.WriteLine(validationError);
            return 1;
        }

        var outputStreams = new Dictionary<string, Stream>(StringComparer.OrdinalIgnoreCase);
        var outputWriters = new Dictionary<string, JsonLineWriter>(StringComparer.OrdinalIgnoreCase);
        Stream? stdinStream = null;

        try
        {
            for (var index = 0; index < scriptPlan.Statements.Count; index++)
            {
                var plan = scriptPlan.Statements[index];
                var selectIndex = index + 1;

                var outputName = plan.OutputStream ?? CommandLineOptions.DefaultOutputName;
                var inputSource = options.Inputs[plan.InputStream!];
                var outputDestination = options.Outputs[outputName];

                if (!outputStreams.TryGetValue(outputName, out var outputStream))
                {
                    outputStream = StreamReaderFactory.OpenOutput(outputDestination);
                    outputStreams[outputName] = outputStream;
                    outputWriters[outputName] = new JsonLineWriter(outputStream);
                }

                var jsonWriter = outputWriters[outputName];
                var inputStream = inputSource.Kind == InputSourceKind.Stdin
                    ? stdinStream ??= StreamReaderFactory.OpenInput(inputSource)
                    : StreamReaderFactory.OpenInput(inputSource);

                try
                {
                    await ExecutePlanAsync(
                        engine,
                        plan,
                        inputStream,
                        jsonWriter,
                        options.Follow,
                        inputSource.Path,
                        shutdown.Token,
                        () => stopRequested);
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine($"SELECT {selectIndex} failed: {ex.Message}");
                    return 1;
                }
                finally
                {
                    if (inputSource.Kind == InputSourceKind.File)
                    {
                        await inputStream.DisposeAsync();
                    }
                }

                if (stopRequested)
                {
                    break;
                }
            }
        }
        finally
        {
            foreach (var stream in outputStreams.Values)
            {
                await stream.FlushAsync();
                await stream.DisposeAsync();
            }

            if (stdinStream is not null)
            {
                await stdinStream.FlushAsync();
                await stdinStream.DisposeAsync();
            }
        }

        return 0;
    }

    private static bool TryValidateBindings(
        SqlScriptPlan scriptPlan,
        CommandLineOptions options,
        out string? error)
    {
        error = null;
        var stdinSelectCount = 0;
        var usedOutputs = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        for (var index = 0; index < scriptPlan.Statements.Count; index++)
        {
            var plan = scriptPlan.Statements[index];
            var selectIndex = index + 1;

            if (string.IsNullOrWhiteSpace(plan.InputStream))
            {
                error = $"SELECT {selectIndex} does not specify an input.";
                return false;
            }

            if (!options.Inputs.TryGetValue(plan.InputStream, out var inputSource))
            {
                error = $"SELECT {selectIndex} references unknown input '{plan.InputStream}'.";
                return false;
            }

            if (inputSource.Kind == InputSourceKind.Stdin)
            {
                stdinSelectCount++;
            }

            if (plan.OutputStream is null && scriptPlan.Statements.Count > 1)
            {
                error = $"SELECT {selectIndex} must specify an output using INTO.";
                return false;
            }

            var outputName = plan.OutputStream ?? CommandLineOptions.DefaultOutputName;
            if (!options.Outputs.ContainsKey(outputName))
            {
                error = $"SELECT {selectIndex} references unknown output '{outputName}'.";
                return false;
            }

            if (!usedOutputs.Add(outputName))
            {
                error = $"SELECT {selectIndex} cannot reuse output '{outputName}'.";
                return false;
            }
        }

        if (stdinSelectCount > 1)
        {
            error = "Only one SELECT may read from stdin.";
            return false;
        }

        return true;
    }

    private static async Task ExecutePlanAsync(
        ChronosQLEngine engine,
        SqlPlan plan,
        Stream inputStream,
        JsonLineWriter jsonWriter,
        bool follow,
        string? inputFilePath,
        CancellationToken cancellationToken,
        Func<bool> stopRequested)
    {
        var jsonReader = new JsonLineReader(inputStream, follow, inputFilePath);

        while (!stopRequested())
        {
            var batch = await jsonReader.ReadAvailableBatchAsync(cancellationToken);
            if (batch.IsCompleted && batch.Events.Count == 0)
            {
                break;
            }

            if (batch.Events.Count == 0)
            {
                continue;
            }

            await using var query = engine.CreateStreamingQuery(plan);
            var writeTask = jsonWriter.WriteAllAsync(query.Results, cancellationToken);

            foreach (var inputEvent in batch.Events)
            {
                await query.EnqueueAsync(inputEvent.Payload, inputEvent.ArrivalTime);
            }

            query.Complete();
            await writeTask;
        }
    }
}

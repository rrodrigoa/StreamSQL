using System.Collections.Generic;
using System.Linq;
using System.Threading.Channels;
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
        System.Diagnostics.Debugger.Launch();
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

        var requiredInputs = scriptPlan.Statements
            .Select(statement => statement.InputName)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();
        var requiredOutputs = scriptPlan.Statements
            .Select(statement => statement.OutputName)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        foreach (var inputName in requiredInputs)
        {
            if (!options.Inputs.ContainsKey(inputName))
            {
                Console.Error.WriteLine($"SELECT references unknown input '{inputName}'.");
                return 1;
            }
        }

        foreach (var outputName in requiredOutputs)
        {
            if (!options.Outputs.ContainsKey(outputName))
            {
                Console.Error.WriteLine($"SELECT references unknown output '{outputName}'.");
                return 1;
            }
        }

        using var shutdown = new CancellationTokenSource();
        var quitListener = options.ReadMode != InputReadMode.Normal
            ? MonitorQuitAsync(shutdown)
            : Task.CompletedTask;

        try
        {
            var inputStreams = new Dictionary<string, Stream>(StringComparer.OrdinalIgnoreCase);
            var outputStreams = new Dictionary<string, Stream>(StringComparer.OrdinalIgnoreCase);
            try
            {
                var outputChannels = new Dictionary<string, Channel<JsonElement>>(StringComparer.OrdinalIgnoreCase);
                var inputEnumerables = new Dictionary<string, IAsyncEnumerable<InputEvent>>(StringComparer.OrdinalIgnoreCase);
                var outputWriterTasks = new List<Task>();

                foreach (var inputName in requiredInputs)
                {
                    var inputSource = options.Inputs[inputName];
                    var inputStream = StreamReaderFactory.OpenInput(inputSource);
                    inputStreams[inputName] = inputStream;
                    var reader = new JsonLineReader(inputStream, options.ReadMode, inputSource.Path);
                    inputEnumerables[inputName] = reader.ReadAllAsync(shutdown.Token);
                }

                foreach (var outputName in requiredOutputs)
                {
                    var outputDestination = options.Outputs[outputName];
                    var outputStream = StreamReaderFactory.OpenOutput(outputDestination);
                    outputStreams[outputName] = outputStream;
                    var writer = new JsonLineWriter(outputStream);
                    var channel = Channel.CreateUnbounded<JsonElement>(new UnboundedChannelOptions
                    {
                        SingleReader = true,
                        SingleWriter = false
                    });
                    outputChannels[outputName] = channel;
                    outputWriterTasks.Add(writer.WriteAllAsync(channel.Reader.ReadAllAsync(shutdown.Token), shutdown.Token));
                }

                var compiled = engine.Compile(scriptPlan);
                var executionTask = compiled.ExecuteAsync(
                    inputEnumerables,
                    outputChannels.ToDictionary(entry => entry.Key, entry => entry.Value.Writer),
                    shutdown.Token);
                await Task.WhenAll(outputWriterTasks.Append(executionTask)).ConfigureAwait(false);
            }
            finally
            {
                foreach (var stream in inputStreams.Values)
                {
                    await stream.DisposeAsync().ConfigureAwait(false);
                }

                foreach (var stream in outputStreams.Values)
                {
                    await stream.DisposeAsync().ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException) when (shutdown.IsCancellationRequested)
        {
            return 0;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine(ex.Message);
            return 1;
        }

        await quitListener;
        return 0;
    }

    private static Task MonitorQuitAsync(CancellationTokenSource shutdown)
        => Task.Run(async () =>
        {
            var buffer = new char[1];
            try
            {
                while (!shutdown.IsCancellationRequested)
                {
                    var read = await Console.In.ReadAsync(buffer.AsMemory(0, 1), shutdown.Token);
                    if (read == 0)
                    {
                        return;
                    }

                    if (buffer[0] is 'q' or 'Q')
                    {
                        shutdown.Cancel();
                        return;
                    }
                }
            }
            catch (OperationCanceledException)
            {
            }
        });
}

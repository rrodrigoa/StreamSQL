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
        var jsonReader = new JsonLineReader(inputStream, options.Follow, options.InputFilePath);

        await using var outputStream = StreamReaderFactory.OpenOutput(options);
        var jsonWriter = new JsonLineWriter(outputStream);

        var engine = new ChronosQLEngine(new EngineExecutionOptions
        {
            TimestampField = options.EventTimeField ?? "timestamp",
            Follow = options.Follow
        });
        var plan = engine.Parse(sqlText);
        if (plan.Window is not null && string.IsNullOrWhiteSpace(options.EventTimeField))
        {
            Console.Error.WriteLine("Windowed queries require --timestamp-by to specify event time.");
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

        while (!stopRequested)
        {
            var batch = await jsonReader.ReadAvailableBatchAsync(shutdown.Token);
            if (batch.IsCompleted && batch.Events.Count == 0)
            {
                break;
            }

            if (batch.Events.Count == 0)
            {
                continue;
            }

            await using var query = engine.CreateStreamingQuery(plan);
            var writeTask = jsonWriter.WriteAllAsync(query.Results);

            foreach (var inputEvent in batch.Events)
            {
                await query.EnqueueAsync(inputEvent.Payload, inputEvent.ArrivalTime);
            }

            query.Complete();
            await writeTask;
        }

        await outputStream.FlushAsync();
        return 0;
    }
}

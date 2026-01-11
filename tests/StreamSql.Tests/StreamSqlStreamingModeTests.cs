using System.Diagnostics;
using System.IO.Pipes;
using System.Text;
using StreamSql;
using Xunit;

namespace StreamSql.Tests;

[CollectionDefinition("Console", DisableParallelization = true)]
public class ConsoleCollection;

[Collection("Console")]
public class StreamSqlStreamingModeTests
{
    [Fact]
    public async Task FollowModeStreamsOutputBeforeExit()
    {
        var sql = "SELECT data.value INTO output FROM input";
        var inputPath = Path.GetTempFileName();
        var outputPath = Path.GetTempFileName();

        await using var quitPipe = new AnonymousPipeServerStream(PipeDirection.Out, HandleInheritability.None);
        await using var quitReader = new AnonymousPipeClientStream(PipeDirection.In, quitPipe.ClientSafePipeHandle);

        var originalIn = Console.In;
        Console.SetIn(new StreamReader(quitReader));

        try
        {
            await File.WriteAllTextAsync(inputPath, "{\"data\":{\"value\":1}}\n");
            await File.WriteAllTextAsync(outputPath, string.Empty);

            var args = new[]
            {
                "--query", sql,
                "--follow",
                "--input", $"input={inputPath}",
                "--output", $"output={outputPath}"
            };

            var runTask = Task.Run(() => Program.Main(args));

            var firstBatch = await WaitForOutputLinesAsync(outputPath, expectedCount: 1);
            Assert.Equal(new[] { "{\"value\":1}" }, firstBatch);

            await File.AppendAllTextAsync(inputPath, "{\"data\":{\"value\":2}}\n");

            var secondBatch = await WaitForOutputLinesAsync(outputPath, expectedCount: 2);
            Assert.Equal(new[] { "{\"value\":1}", "{\"value\":2}" }, secondBatch);

            await quitPipe.WriteAsync(Encoding.UTF8.GetBytes("q"));
            await quitPipe.FlushAsync();

            var exitCode = await runTask;
            Assert.Equal(0, exitCode);
        }
        finally
        {
            Console.SetIn(originalIn);
            File.Delete(inputPath);
            File.Delete(outputPath);
        }
    }

    [Fact]
    public async Task TailModeStreamsOnlyNewDataBeforeExit()
    {
        var sql = "SELECT data.value INTO output FROM input";
        var inputPath = Path.GetTempFileName();
        var outputPath = Path.GetTempFileName();

        await using var quitPipe = new AnonymousPipeServerStream(PipeDirection.Out, HandleInheritability.None);
        await using var quitReader = new AnonymousPipeClientStream(PipeDirection.In, quitPipe.ClientSafePipeHandle);

        var originalIn = Console.In;
        Console.SetIn(new StreamReader(quitReader));

        try
        {
            await File.WriteAllTextAsync(inputPath, "{\"data\":{\"value\":10}}\n");
            await File.WriteAllTextAsync(outputPath, string.Empty);

            var args = new[]
            {
                "--query", sql,
                "--tail",
                "--input", $"input={inputPath}",
                "--output", $"output={outputPath}"
            };

            var runTask = Task.Run(() => Program.Main(args));

            await Task.Delay(150);
            var initial = await ReadOutputLinesAsync(outputPath);
            Assert.Empty(initial);

            await File.AppendAllTextAsync(inputPath, "{\"data\":{\"value\":11}}\n");
            var firstBatch = await WaitForOutputLinesAsync(outputPath, expectedCount: 1);
            Assert.Equal(new[] { "{\"value\":11}" }, firstBatch);

            await File.AppendAllTextAsync(inputPath, "{\"data\":{\"value\":12}}\n");
            var secondBatch = await WaitForOutputLinesAsync(outputPath, expectedCount: 2);
            Assert.Equal(new[] { "{\"value\":11}", "{\"value\":12}" }, secondBatch);

            await quitPipe.WriteAsync(Encoding.UTF8.GetBytes("q"));
            await quitPipe.FlushAsync();

            var exitCode = await runTask;
            Assert.Equal(0, exitCode);
        }
        finally
        {
            Console.SetIn(originalIn);
            File.Delete(inputPath);
            File.Delete(outputPath);
        }
    }

    private static async Task<string[]> ReadOutputLinesAsync(string outputPath)
    {
        var text = await File.ReadAllTextAsync(outputPath);
        return text.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
    }

    private static async Task<string[]> WaitForOutputLinesAsync(string outputPath, int expectedCount)
    {
        var stopwatch = Stopwatch.StartNew();
        while (stopwatch.Elapsed < TimeSpan.FromSeconds(5))
        {
            var lines = await ReadOutputLinesAsync(outputPath);
            if (lines.Length >= expectedCount)
            {
                return lines;
            }

            await Task.Delay(50);
        }

        throw new TimeoutException($"Timed out waiting for {expectedCount} output lines.");
    }
}

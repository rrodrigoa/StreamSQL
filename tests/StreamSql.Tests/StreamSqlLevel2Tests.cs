using System.Text;
using StreamSql;
using StreamSql.Input;
using Xunit;

namespace StreamSql.Tests;

public class StreamSqlLevel2Tests
{
    [Fact]
    public async Task ExecutesMultiSelectScriptWithNamedInputsAndOutputs()
    {
        var sql = string.Join(Environment.NewLine, new[]
        {
            "SELECT data.value AS v INTO orders_out FROM orders;",
            "SELECT data.value AS v INTO trades_out FROM trades;"
        });

        var ordersInput = string.Join('\n', new[]
        {
            "{\"data\":{\"value\":1}}",
            "{\"data\":{\"value\":2}}"
        }) + "\n";

        var tradesInput = string.Join('\n', new[]
        {
            "{\"data\":{\"value\":5}}",
            "{\"data\":{\"value\":6}}"
        }) + "\n";

        var inputPath = Path.GetTempFileName();
        var outputPath = Path.GetTempFileName();

        var stdinStream = new MemoryStream(Encoding.UTF8.GetBytes(tradesInput));
        var stdoutStream = new MemoryStream();

        try
        {
            await File.WriteAllTextAsync(inputPath, ordersInput);
            await File.WriteAllTextAsync(outputPath, string.Empty);

            StreamReaderFactory.InputOverride = stdinStream;
            StreamReaderFactory.OutputOverride = stdoutStream;

            var args = new[]
            {
                "--query", sql,
                "--input", $"orders={inputPath}",
                "--input", "trades=-",
                "--output", $"orders_out={outputPath}",
                "--output", "trades_out=-"
            };

            var exitCode = await Program.Main(args);
            Assert.Equal(0, exitCode);

            var fileOutput = await File.ReadAllTextAsync(outputPath);
            var fileLines = fileOutput.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
            Assert.Equal(new[] { "{\"v\":1}", "{\"v\":2}" }, fileLines);

            stdoutStream.Position = 0;
            using var reader = new StreamReader(stdoutStream, leaveOpen: true);
            var stdoutText = await reader.ReadToEndAsync();
            var stdoutLines = stdoutText.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
            Assert.Equal(new[] { "{\"v\":5}", "{\"v\":6}" }, stdoutLines);
        }
        finally
        {
            StreamReaderFactory.InputOverride = null;
            StreamReaderFactory.OutputOverride = null;
            stdinStream.Dispose();
            stdoutStream.Dispose();
            File.Delete(inputPath);
            File.Delete(outputPath);
        }
    }

    [Fact]
    public async Task ExecutesMultipleSelectsAgainstSameFileInput()
    {
        var sql = string.Join(Environment.NewLine, new[]
        {
            "SELECT data.value AS v INTO first_out FROM orders;",
            "SELECT COUNT(*) AS count INTO second_out FROM orders;"
        });

        var input = string.Join('\n', new[]
        {
            "{\"data\":{\"value\":4}}",
            "{\"data\":{\"value\":6}}"
        }) + "\n";

        var inputPath = Path.GetTempFileName();
        var firstOutputPath = Path.GetTempFileName();
        var secondOutputPath = Path.GetTempFileName();

        try
        {
            await File.WriteAllTextAsync(inputPath, input);
            await File.WriteAllTextAsync(firstOutputPath, string.Empty);
            await File.WriteAllTextAsync(secondOutputPath, string.Empty);

            var args = new[]
            {
                "--query", sql,
                "--input", $"orders={inputPath}",
                "--output", $"first_out={firstOutputPath}",
                "--output", $"second_out={secondOutputPath}"
            };

            var exitCode = await Program.Main(args);
            Assert.Equal(0, exitCode);

            var firstOutput = await File.ReadAllTextAsync(firstOutputPath);
            var firstLines = firstOutput.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
            Assert.Equal(new[] { "{\"v\":4}", "{\"v\":6}" }, firstLines);

            var secondOutput = await File.ReadAllTextAsync(secondOutputPath);
            var secondLines = secondOutput.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
            Assert.Equal(new[] { "{\"count\":2}" }, secondLines);

            var singleSelectArgs = new[]
            {
                "--query", "SELECT data.value AS v INTO first_out FROM orders;",
                "--input", $"orders={inputPath}",
                "--output", $"first_out={firstOutputPath}"
            };
            await File.WriteAllTextAsync(firstOutputPath, string.Empty);
            var singleExitCode = await Program.Main(singleSelectArgs);
            Assert.Equal(0, singleExitCode);

            var singleOutput = await File.ReadAllTextAsync(firstOutputPath);
            var singleLines = singleOutput.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
            Assert.Equal(firstLines, singleLines);

            var singleCountArgs = new[]
            {
                "--query", "SELECT COUNT(*) AS count INTO second_out FROM orders;",
                "--input", $"orders={inputPath}",
                "--output", $"second_out={secondOutputPath}"
            };
            await File.WriteAllTextAsync(secondOutputPath, string.Empty);
            var singleCountExitCode = await Program.Main(singleCountArgs);
            Assert.Equal(0, singleCountExitCode);

            var singleCountOutput = await File.ReadAllTextAsync(secondOutputPath);
            var singleCountLines = singleCountOutput.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
            Assert.Equal(secondLines, singleCountLines);
        }
        finally
        {
            File.Delete(inputPath);
            File.Delete(firstOutputPath);
            File.Delete(secondOutputPath);
        }
    }

    [Fact]
    public async Task FailsWhenMultipleSelectsUseStdin()
    {
        var sql = string.Join(Environment.NewLine, new[]
        {
            "SELECT data.value INTO first_out FROM input;",
            "SELECT data.value INTO second_out FROM input;"
        });

        var args = new[]
        {
            "--query", sql,
            "--input", "input=-",
            "--output", "first_out=-",
            "--output", "second_out=second.json"
        };

        var previousError = Console.Error;
        var errorWriter = new StringWriter();
        Console.SetError(errorWriter);

        try
        {
            var exitCode = await Program.Main(args);
            Assert.Equal(1, exitCode);
            Assert.Contains("stdin", errorWriter.ToString(), StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            Console.SetError(previousError);
        }
    }

    [Fact]
    public async Task FailsWhenSelectReferencesUnknownInput()
    {
        var sql = "SELECT data.value INTO out1 FROM missing;";
        var args = new[] { "--query", sql, "--output", "out1=-" };

        var previousError = Console.Error;
        var errorWriter = new StringWriter();
        Console.SetError(errorWriter);

        try
        {
            var exitCode = await Program.Main(args);
            Assert.Equal(1, exitCode);
            Assert.Contains("unknown input 'missing'", errorWriter.ToString(), StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            Console.SetError(previousError);
        }
    }

    [Fact]
    public async Task FailsWhenSelectReferencesUnknownOutput()
    {
        var sql = "SELECT data.value INTO missing FROM input;";
        var args = new[] { "--query", sql };

        var previousError = Console.Error;
        var errorWriter = new StringWriter();
        Console.SetError(errorWriter);

        try
        {
            var exitCode = await Program.Main(args);
            Assert.Equal(1, exitCode);
            Assert.Contains("unknown output 'missing'", errorWriter.ToString(), StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            Console.SetError(previousError);
        }
    }
}

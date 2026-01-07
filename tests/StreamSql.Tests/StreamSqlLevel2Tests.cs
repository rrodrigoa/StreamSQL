using System.Text;
using StreamSql;
using StreamSql.Input;
using Xunit;

namespace StreamSql.Tests;

public class StreamSqlLevel2Tests
{
    [Fact]
    public async Task AllowsMultipleSelectsToWriteToSameOutput()
    {
        var sql = string.Join(Environment.NewLine, new[]
        {
            "SELECT data.value AS v INTO shared_out FROM input;",
            "SELECT COUNT(*) AS count INTO shared_out FROM input;"
        });

        var inputPath = Path.GetTempFileName();
        var outputPath = Path.GetTempFileName();

        var previousError = Console.Error;
        var errorWriter = new StringWriter();
        Console.SetError(errorWriter);

        try
        {
            await File.WriteAllTextAsync(inputPath, "{\"data\":{\"value\":1}}\n{\"data\":{\"value\":2}}\n");
            await File.WriteAllTextAsync(outputPath, string.Empty);

            var args = new[]
            {
                "--query", sql,
                "--input", $"input={inputPath}",
                "--output", $"shared_out={outputPath}"
            };

            var exitCode = await Program.Main(args);
            Assert.Equal(0, exitCode);

            var output = await File.ReadAllTextAsync(outputPath);
            var lines = output.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
            Assert.Equal(new[] { "{\"count\":2}", "{\"v\":1}", "{\"v\":2}" }, lines);
        }
        finally
        {
            Console.SetError(previousError);
            File.Delete(inputPath);
            File.Delete(outputPath);
        }
    }

    [Fact]
    public async Task FailsWhenMultipleInputsUseStdin()
    {
        var sql = "SELECT data.value INTO out1 FROM input;";
        var args = new[]
        {
            "--query", sql,
            "--input", "first=-",
            "--input", "second=-",
            "--output", "out1=-"
        };

        var previousError = Console.Error;
        var errorWriter = new StringWriter();
        Console.SetError(errorWriter);

        try
        {
            var exitCode = await Program.Main(args);
            Assert.Equal(1, exitCode);
            Assert.Equal("Only one input may use stdin.", errorWriter.ToString().Trim());
        }
        finally
        {
            Console.SetError(previousError);
        }
    }

    [Fact]
    public async Task FailsWhenMultipleOutputsUseStdout()
    {
        var sql = "SELECT data.value INTO out1 FROM input;";
        var args = new[]
        {
            "--query", sql,
            "--output", "out1=-",
            "--output", "out2=-"
        };

        var previousError = Console.Error;
        var errorWriter = new StringWriter();
        Console.SetError(errorWriter);

        try
        {
            var exitCode = await Program.Main(args);
            Assert.Equal(1, exitCode);
            Assert.Equal("Only one output may use stdout.", errorWriter.ToString().Trim());
        }
        finally
        {
            Console.SetError(previousError);
        }
    }

    [Fact]
    public async Task ExecutesMultipleSelectsWithDefaultInputBinding()
    {
        var sql = string.Join(Environment.NewLine, new[]
        {
            "SELECT data.value AS v INTO first_out FROM input;",
            "SELECT data.value AS v INTO second_out FROM input;"
        });

        var input = string.Join('\n', new[]
        {
            "{\"data\":{\"value\":1}}",
            "{\"data\":{\"value\":2}}"
        }) + "\n";

        var outputPath1 = Path.GetTempFileName();
        var outputPath2 = Path.GetTempFileName();
        var stdinStream = new MemoryStream(Encoding.UTF8.GetBytes(input));

        try
        {
            await File.WriteAllTextAsync(outputPath1, string.Empty);
            await File.WriteAllTextAsync(outputPath2, string.Empty);

            StreamReaderFactory.InputOverride = stdinStream;

            var args = new[]
            {
                "--query", sql,
                "--output", $"first_out={outputPath1}",
                "--output", $"second_out={outputPath2}"
            };

            var exitCode = await Program.Main(args);
            Assert.Equal(0, exitCode);

            var firstOutput = await File.ReadAllTextAsync(outputPath1);
            var firstLines = firstOutput.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
            Assert.Equal(new[] { "{\"v\":1}", "{\"v\":2}" }, firstLines);

            var secondOutput = await File.ReadAllTextAsync(outputPath2);
            var secondLines = secondOutput.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
            Assert.Equal(new[] { "{\"v\":1}", "{\"v\":2}" }, secondLines);
        }
        finally
        {
            StreamReaderFactory.InputOverride = null;
            stdinStream.Dispose();
            File.Delete(outputPath1);
            File.Delete(outputPath2);
        }
    }

    [Fact]
    public async Task AllowsUnusedInputAndOutputBindings()
    {
        var sql = "SELECT data.value AS v INTO used_out FROM used_input;";

        var usedInputPath = Path.GetTempFileName();
        var unusedInputPath = Path.GetTempFileName();
        var usedOutputPath = Path.GetTempFileName();
        var unusedOutputPath = Path.GetTempFileName();

        try
        {
            await File.WriteAllTextAsync(usedInputPath, "{\"data\":{\"value\":30}}\n");
            await File.WriteAllTextAsync(unusedInputPath, "{\"data\":{\"value\":99}}\n");
            await File.WriteAllTextAsync(usedOutputPath, string.Empty);
            await File.WriteAllTextAsync(unusedOutputPath, string.Empty);

            var args = new[]
            {
                "--query", sql,
                "--input", $"used_input={usedInputPath}",
                "--input", $"unused_input={unusedInputPath}",
                "--output", $"used_out={usedOutputPath}",
                "--output", $"unused_out={unusedOutputPath}"
            };

            var exitCode = await Program.Main(args);
            Assert.Equal(0, exitCode);

            var usedOutput = await File.ReadAllTextAsync(usedOutputPath);
            var usedLines = usedOutput.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
            Assert.Equal(new[] { "{\"v\":30}" }, usedLines);

            var unusedOutput = await File.ReadAllTextAsync(unusedOutputPath);
            Assert.True(string.IsNullOrEmpty(unusedOutput));
        }
        finally
        {
            File.Delete(usedInputPath);
            File.Delete(unusedInputPath);
            File.Delete(usedOutputPath);
            File.Delete(unusedOutputPath);
        }
    }

    [Fact]
    public async Task ExecutesMultipleSelectsWithMixedInputSources()
    {
        var sql = string.Join(Environment.NewLine, new[]
        {
            "SELECT data.value AS v INTO file_out FROM file_input;",
            "SELECT data.value AS v INTO stdin_out FROM stdin_input;"
        });

        var fileInput = string.Join('\n', new[]
        {
            "{\"data\":{\"value\":1}}",
            "{\"data\":{\"value\":2}}"
        }) + "\n";

        var stdinInput = string.Join('\n', new[]
        {
            "{\"data\":{\"value\":3}}",
            "{\"data\":{\"value\":4}}"
        }) + "\n";

        var fileInputPath = Path.GetTempFileName();
        var fileOutputPath = Path.GetTempFileName();
        var stdinOutputPath = Path.GetTempFileName();

        var stdinStream = new MemoryStream(Encoding.UTF8.GetBytes(stdinInput));

        try
        {
            await File.WriteAllTextAsync(fileInputPath, fileInput);
            await File.WriteAllTextAsync(fileOutputPath, string.Empty);
            await File.WriteAllTextAsync(stdinOutputPath, string.Empty);

            StreamReaderFactory.InputOverride = stdinStream;

            var args = new[]
            {
                "--query", sql,
                "--input", $"file_input={fileInputPath}",
                "--input", "stdin_input=-",
                "--output", $"file_out={fileOutputPath}",
                "--output", $"stdin_out={stdinOutputPath}"
            };

            var exitCode = await Program.Main(args);
            Assert.Equal(0, exitCode);

            var fileOutput = await File.ReadAllTextAsync(fileOutputPath);
            var fileLines = fileOutput.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
            Assert.Equal(new[] { "{\"v\":1}", "{\"v\":2}" }, fileLines);

            var stdinOutput = await File.ReadAllTextAsync(stdinOutputPath);
            var stdinLines = stdinOutput.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
            Assert.Equal(new[] { "{\"v\":3}", "{\"v\":4}" }, stdinLines);
        }
        finally
        {
            StreamReaderFactory.InputOverride = null;
            stdinStream.Dispose();
            File.Delete(fileInputPath);
            File.Delete(fileOutputPath);
            File.Delete(stdinOutputPath);
        }
    }

    [Fact]
    public async Task ExecutesSingleSelectWithExplicitStdinInputAndFileOutput()
    {
        var sql = "SELECT data.value AS v INTO out1 FROM stdin_input;";

        var input = string.Join('\n', new[]
        {
            "{\"data\":{\"value\":7}}",
            "{\"data\":{\"value\":8}}"
        }) + "\n";

        var outputPath = Path.GetTempFileName();
        var stdinStream = new MemoryStream(Encoding.UTF8.GetBytes(input));

        try
        {
            await File.WriteAllTextAsync(outputPath, string.Empty);
            StreamReaderFactory.InputOverride = stdinStream;

            var args = new[]
            {
                "--query", sql,
                "--input", "stdin_input=-",
                "--output", $"out1={outputPath}"
            };

            var exitCode = await Program.Main(args);
            Assert.Equal(0, exitCode);

            var output = await File.ReadAllTextAsync(outputPath);
            var lines = output.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
            Assert.Equal(new[] { "{\"v\":7}", "{\"v\":8}" }, lines);
        }
        finally
        {
            StreamReaderFactory.InputOverride = null;
            stdinStream.Dispose();
            File.Delete(outputPath);
        }
    }

    [Fact]
    public async Task ExecutesSingleSelectWithExplicitFileInputAndStdoutOutput()
    {
        var sql = "SELECT data.value AS v INTO out1 FROM file_input;";

        var input = string.Join('\n', new[]
        {
            "{\"data\":{\"value\":9}}",
            "{\"data\":{\"value\":10}}"
        }) + "\n";

        var inputPath = Path.GetTempFileName();
        var stdoutStream = new MemoryStream();

        try
        {
            await File.WriteAllTextAsync(inputPath, input);

            StreamReaderFactory.OutputOverride = stdoutStream;

            var args = new[]
            {
                "--query", sql,
                "--input", $"file_input={inputPath}",
                "--output", "out1=-"
            };

            var exitCode = await Program.Main(args);
            Assert.Equal(0, exitCode);

            stdoutStream.Position = 0;
            using var reader = new StreamReader(stdoutStream, leaveOpen: true);
            var stdoutText = await reader.ReadToEndAsync();
            var stdoutLines = stdoutText.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
            Assert.Equal(new[] { "{\"v\":9}", "{\"v\":10}" }, stdoutLines);
        }
        finally
        {
            StreamReaderFactory.OutputOverride = null;
            stdoutStream.Dispose();
            File.Delete(inputPath);
        }
    }

    [Fact]
    public async Task ExecutesSingleSelectWithDefaultStdinStdoutBindings()
    {
        var sql = "SELECT data.value AS v FROM input;";

        var input = string.Join('\n', new[]
        {
            "{\"data\":{\"value\":11}}",
            "{\"data\":{\"value\":12}}"
        }) + "\n";

        var stdinStream = new MemoryStream(Encoding.UTF8.GetBytes(input));
        var stdoutStream = new MemoryStream();

        try
        {
            StreamReaderFactory.InputOverride = stdinStream;
            StreamReaderFactory.OutputOverride = stdoutStream;

            var args = new[] { "--query", sql };

            var exitCode = await Program.Main(args);
            Assert.Equal(0, exitCode);

            stdoutStream.Position = 0;
            using var reader = new StreamReader(stdoutStream, leaveOpen: true);
            var stdoutText = await reader.ReadToEndAsync();
            var stdoutLines = stdoutText.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
            Assert.Equal(new[] { "{\"v\":11}", "{\"v\":12}" }, stdoutLines);
        }
        finally
        {
            StreamReaderFactory.InputOverride = null;
            StreamReaderFactory.OutputOverride = null;
            stdinStream.Dispose();
            stdoutStream.Dispose();
        }
    }

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
    public async Task ExecutesMultipleSelectsAgainstSameStdinInput()
    {
        var sql = string.Join(Environment.NewLine, new[]
        {
            "SELECT data.value AS v INTO first_out FROM input;",
            "SELECT COUNT(*) AS count INTO second_out FROM input;"
        });

        var input = string.Join('\n', new[]
        {
            "{\"data\":{\"value\":10}}",
            "{\"data\":{\"value\":20}}"
        }) + "\n";

        var firstOutputPath = Path.GetTempFileName();
        var secondOutputPath = Path.GetTempFileName();
        var stdinStream = new MemoryStream(Encoding.UTF8.GetBytes(input));

        try
        {
            await File.WriteAllTextAsync(firstOutputPath, string.Empty);
            await File.WriteAllTextAsync(secondOutputPath, string.Empty);

            StreamReaderFactory.InputOverride = stdinStream;

            var args = new[]
            {
                "--query", sql,
                "--input", "input=-",
                "--output", $"first_out={firstOutputPath}",
                "--output", $"second_out={secondOutputPath}"
            };

            var exitCode = await Program.Main(args);
            Assert.Equal(0, exitCode);

            var firstOutput = await File.ReadAllTextAsync(firstOutputPath);
            var firstLines = firstOutput.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
            Assert.Equal(new[] { "{\"v\":10}", "{\"v\":20}" }, firstLines);

            var secondOutput = await File.ReadAllTextAsync(secondOutputPath);
            var secondLines = secondOutput.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
            Assert.Equal(new[] { "{\"count\":2}" }, secondLines);
        }
        finally
        {
            StreamReaderFactory.InputOverride = null;
            stdinStream.Dispose();
            File.Delete(firstOutputPath);
            File.Delete(secondOutputPath);
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
            Assert.Equal("SELECT 1 references unknown input 'missing'.", errorWriter.ToString().Trim());
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
            Assert.Equal("SELECT 1 references unknown output 'missing'.", errorWriter.ToString().Trim());
        }
        finally
        {
            Console.SetError(previousError);
        }
    }
}

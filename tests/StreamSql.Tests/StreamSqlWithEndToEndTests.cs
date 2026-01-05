using System.Text;
using StreamSql;
using StreamSql.Input;
using Xunit;

namespace StreamSql.Tests;

public class StreamSqlWithEndToEndTests
{
    [Fact]
    public async Task ExecutesWithFanOutToMultipleSelects()
    {
        var sql = string.Join(Environment.NewLine, new[]
        {
            "WITH filtered AS (SELECT data.value AS v FROM input)",
            "SELECT v INTO first_out FROM filtered;",
            "SELECT COUNT(*) AS count INTO second_out FROM filtered;"
        });

        var input = string.Join('\n', new[]
        {
            "{\"data\":{\"value\":1}}",
            "{\"data\":{\"value\":2}}"
        }) + "\n";

        var inputPath = Path.GetTempFileName();
        var firstOut = Path.GetTempFileName();
        var secondOut = Path.GetTempFileName();

        try
        {
            await File.WriteAllTextAsync(inputPath, input);
            await File.WriteAllTextAsync(firstOut, string.Empty);
            await File.WriteAllTextAsync(secondOut, string.Empty);

            var args = new[]
            {
                "--query", sql,
                "--input", $"input={inputPath}",
                "--output", $"first_out={firstOut}",
                "--output", $"second_out={secondOut}"
            };

            var exitCode = await Program.Main(args);
            Assert.Equal(0, exitCode);

            var firstLines = (await File.ReadAllTextAsync(firstOut))
                .Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
            var secondLines = (await File.ReadAllTextAsync(secondOut))
                .Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);

            Assert.Equal(new[] { "{\"v\":1}", "{\"v\":2}" }, firstLines);
            Assert.Equal(new[] { "{\"count\":2}" }, secondLines);
        }
        finally
        {
            File.Delete(inputPath);
            File.Delete(firstOut);
            File.Delete(secondOut);
        }
    }

    [Fact]
    public async Task ExecutesChainedWithDefinitions()
    {
        var sql = string.Join(Environment.NewLine, new[]
        {
            "WITH first AS (SELECT data.value AS v FROM input),",
            "second AS (SELECT v AS v2 FROM first)",
            "SELECT v2 INTO out FROM second;"
        });

        var input = string.Join('\n', new[]
        {
            "{\"data\":{\"value\":3}}",
            "{\"data\":{\"value\":4}}"
        }) + "\n";

        var inputPath = Path.GetTempFileName();
        var outputPath = Path.GetTempFileName();

        try
        {
            await File.WriteAllTextAsync(inputPath, input);
            await File.WriteAllTextAsync(outputPath, string.Empty);

            var args = new[]
            {
                "--query", sql,
                "--input", $"input={inputPath}",
                "--output", $"out={outputPath}"
            };

            var exitCode = await Program.Main(args);
            Assert.Equal(0, exitCode);

            var lines = (await File.ReadAllTextAsync(outputPath))
                .Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
            Assert.Equal(new[] { "{\"v2\":3}", "{\"v2\":4}" }, lines);
        }
        finally
        {
            File.Delete(inputPath);
            File.Delete(outputPath);
        }
    }

    [Fact]
    public async Task ExecutesWithAndDirectSelectFromSameInput()
    {
        var sql = string.Join(Environment.NewLine, new[]
        {
            "WITH filtered AS (SELECT data.value AS v FROM input)",
            "SELECT v INTO with_out FROM filtered;",
            "SELECT data.value AS v INTO direct_out FROM input;"
        });

        var input = string.Join('\n', new[]
        {
            "{\"data\":{\"value\":7}}",
            "{\"data\":{\"value\":8}}"
        }) + "\n";

        var inputPath = Path.GetTempFileName();
        var withOut = Path.GetTempFileName();
        var directOut = Path.GetTempFileName();

        try
        {
            await File.WriteAllTextAsync(inputPath, input);
            await File.WriteAllTextAsync(withOut, string.Empty);
            await File.WriteAllTextAsync(directOut, string.Empty);

            var args = new[]
            {
                "--query", sql,
                "--input", $"input={inputPath}",
                "--output", $"with_out={withOut}",
                "--output", $"direct_out={directOut}"
            };

            var exitCode = await Program.Main(args);
            Assert.Equal(0, exitCode);

            var withLines = (await File.ReadAllTextAsync(withOut))
                .Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
            var directLines = (await File.ReadAllTextAsync(directOut))
                .Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);

            Assert.Equal(new[] { "{\"v\":7}", "{\"v\":8}" }, withLines);
            Assert.Equal(new[] { "{\"v\":7}", "{\"v\":8}" }, directLines);
        }
        finally
        {
            File.Delete(inputPath);
            File.Delete(withOut);
            File.Delete(directOut);
        }
    }

    [Fact]
    public async Task FansOutStdinAcrossWithsAndSelects()
    {
        var sql = string.Join(Environment.NewLine, new[]
        {
            "WITH first AS (SELECT data.value AS v FROM input),",
            "second AS (SELECT data.value AS v FROM input)",
            "SELECT v INTO first_out FROM first;",
            "SELECT v INTO second_out FROM second;",
            "SELECT COUNT(*) AS count INTO count_out FROM input;"
        });

        var input = string.Join('\n', new[]
        {
            "{\"data\":{\"value\":5}}",
            "{\"data\":{\"value\":6}}"
        }) + "\n";

        var firstOut = Path.GetTempFileName();
        var secondOut = Path.GetTempFileName();
        var countOut = Path.GetTempFileName();
        var stdinStream = new MemoryStream(Encoding.UTF8.GetBytes(input));

        try
        {
            await File.WriteAllTextAsync(firstOut, string.Empty);
            await File.WriteAllTextAsync(secondOut, string.Empty);
            await File.WriteAllTextAsync(countOut, string.Empty);

            StreamReaderFactory.InputOverride = stdinStream;

            var args = new[]
            {
                "--query", sql,
                "--input", "input=-",
                "--output", $"first_out={firstOut}",
                "--output", $"second_out={secondOut}",
                "--output", $"count_out={countOut}"
            };

            var exitCode = await Program.Main(args);
            Assert.Equal(0, exitCode);

            var firstLines = (await File.ReadAllTextAsync(firstOut))
                .Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
            var secondLines = (await File.ReadAllTextAsync(secondOut))
                .Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
            var countLines = (await File.ReadAllTextAsync(countOut))
                .Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);

            Assert.Equal(new[] { "{\"v\":5}", "{\"v\":6}" }, firstLines);
            Assert.Equal(new[] { "{\"v\":5}", "{\"v\":6}" }, secondLines);
            Assert.Equal(new[] { "{\"count\":2}" }, countLines);
        }
        finally
        {
            StreamReaderFactory.InputOverride = null;
            stdinStream.Dispose();
            File.Delete(firstOut);
            File.Delete(secondOut);
            File.Delete(countOut);
        }
    }

    [Fact]
    public async Task InheritsTimestampFromWithDefinition()
    {
        var sql = string.Join(Environment.NewLine, new[]
        {
            "WITH timed AS (SELECT data.value AS v FROM input TIMESTAMP BY input.ts)",
            "SELECT v INTO out FROM timed;"
        });

        var input = string.Join('\n', new[]
        {
            "{\"ts\":1000,\"data\":{\"value\":1}}",
            "{\"ts\":2000,\"data\":{\"value\":2}}"
        }) + "\n";

        var inputPath = Path.GetTempFileName();
        var outputPath = Path.GetTempFileName();

        try
        {
            await File.WriteAllTextAsync(inputPath, input);
            await File.WriteAllTextAsync(outputPath, string.Empty);

            var args = new[]
            {
                "--query", sql,
                "--input", $"input={inputPath}",
                "--output", $"out={outputPath}"
            };

            var exitCode = await Program.Main(args);
            Assert.Equal(0, exitCode);

            var lines = (await File.ReadAllTextAsync(outputPath))
                .Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
            Assert.Equal(new[] { "{\"v\":1}", "{\"v\":2}" }, lines);
        }
        finally
        {
            File.Delete(inputPath);
            File.Delete(outputPath);
        }
    }

    [Fact]
    public async Task RejectsTimestampConflictBetweenWithAndSelect()
    {
        var sql = string.Join(Environment.NewLine, new[]
        {
            "WITH timed AS (SELECT data.value AS v FROM input TIMESTAMP BY input.ts)",
            "SELECT v INTO out FROM timed TIMESTAMP BY v;"
        });

        var inputPath = Path.GetTempFileName();
        var outputPath = Path.GetTempFileName();

        var previousError = Console.Error;
        var errorWriter = new StringWriter();
        Console.SetError(errorWriter);

        try
        {
            await File.WriteAllTextAsync(inputPath, "{\"ts\":1000,\"data\":{\"value\":1}}\n");
            await File.WriteAllTextAsync(outputPath, string.Empty);

            var args = new[]
            {
                "--query", sql,
                "--input", $"input={inputPath}",
                "--output", $"out={outputPath}"
            };

            var exitCode = await Program.Main(args);
            Assert.Equal(1, exitCode);
            Assert.Equal(
                "SELECT 1 cannot define TIMESTAMP BY because upstream stream already has a timestamp.",
                errorWriter.ToString().Trim());
        }
        finally
        {
            Console.SetError(previousError);
            File.Delete(inputPath);
            File.Delete(outputPath);
        }
    }

    [Fact]
    public async Task RejectsTimestampDefinitionsWhenWithFansOut()
    {
        var sql = string.Join(Environment.NewLine, new[]
        {
            "WITH base_stream AS (SELECT data.value AS v FROM input)",
            "SELECT v INTO first_out FROM base_stream TIMESTAMP BY v;",
            "SELECT v INTO second_out FROM base_stream TIMESTAMP BY v;"
        });

        var inputPath = Path.GetTempFileName();
        var firstOut = Path.GetTempFileName();
        var secondOut = Path.GetTempFileName();

        var previousError = Console.Error;
        var errorWriter = new StringWriter();
        Console.SetError(errorWriter);

        try
        {
            await File.WriteAllTextAsync(inputPath, "{\"data\":{\"value\":1}}\n");
            await File.WriteAllTextAsync(firstOut, string.Empty);
            await File.WriteAllTextAsync(secondOut, string.Empty);

            var args = new[]
            {
                "--query", sql,
                "--input", $"input={inputPath}",
                "--output", $"first_out={firstOut}",
                "--output", $"second_out={secondOut}"
            };

            var exitCode = await Program.Main(args);
            Assert.Equal(1, exitCode);
            Assert.Equal(
                "SELECT 1 cannot define TIMESTAMP BY because upstream stream fans out to multiple consumers.",
                errorWriter.ToString().Trim());
        }
        finally
        {
            Console.SetError(previousError);
            File.Delete(inputPath);
            File.Delete(firstOut);
            File.Delete(secondOut);
        }
    }

    [Fact]
    public async Task RejectsTimestampDefinitionDownstreamOfTimedWith()
    {
        var sql = string.Join(Environment.NewLine, new[]
        {
            "WITH timed AS (SELECT data.value AS v FROM input TIMESTAMP BY input.ts),",
            "retimed AS (SELECT v AS v2 FROM timed TIMESTAMP BY v)",
            "SELECT v2 INTO out FROM retimed;"
        });

        var inputPath = Path.GetTempFileName();
        var outputPath = Path.GetTempFileName();

        var previousError = Console.Error;
        var errorWriter = new StringWriter();
        Console.SetError(errorWriter);

        try
        {
            await File.WriteAllTextAsync(inputPath, "{\"ts\":1000,\"data\":{\"value\":1}}\n");
            await File.WriteAllTextAsync(outputPath, string.Empty);

            var args = new[]
            {
                "--query", sql,
                "--input", $"input={inputPath}",
                "--output", $"out={outputPath}"
            };

            var exitCode = await Program.Main(args);
            Assert.Equal(1, exitCode);
            Assert.Equal(
                "WITH 'retimed' cannot define TIMESTAMP BY because upstream stream already has a timestamp.",
                errorWriter.ToString().Trim());
        }
        finally
        {
            Console.SetError(previousError);
            File.Delete(inputPath);
            File.Delete(outputPath);
        }
    }

    [Fact]
    public async Task RejectsDuplicateWithAliases()
    {
        var sql = string.Join(Environment.NewLine, new[]
        {
            "WITH dup AS (SELECT data.value AS v FROM input),",
            "dup AS (SELECT data.value AS v FROM input)",
            "SELECT v INTO out FROM dup;"
        });

        var previousError = Console.Error;
        var errorWriter = new StringWriter();
        Console.SetError(errorWriter);

        try
        {
            var args = new[] { "--query", sql, "--output", "out=-" };
            var exitCode = await Program.Main(args);
            Assert.Equal(1, exitCode);
            Assert.Equal("WITH alias 'dup' is defined more than once.", errorWriter.ToString().Trim());
        }
        finally
        {
            Console.SetError(previousError);
        }
    }

    [Fact]
    public async Task RejectsUnknownWithReference()
    {
        var sql = string.Join(Environment.NewLine, new[]
        {
            "WITH source AS (SELECT data.value AS v FROM input)",
            "SELECT v INTO out FROM missing;"
        });

        var inputPath = Path.GetTempFileName();
        var outputPath = Path.GetTempFileName();

        var previousError = Console.Error;
        var errorWriter = new StringWriter();
        Console.SetError(errorWriter);

        try
        {
            await File.WriteAllTextAsync(inputPath, "{\"data\":{\"value\":1}}\n");
            await File.WriteAllTextAsync(outputPath, string.Empty);

            var args = new[]
            {
                "--query", sql,
                "--input", $"input={inputPath}",
                "--output", $"out={outputPath}"
            };

            var exitCode = await Program.Main(args);
            Assert.Equal(1, exitCode);
            Assert.Equal("SELECT 1 references unknown input 'missing'.", errorWriter.ToString().Trim());
        }
        finally
        {
            Console.SetError(previousError);
            File.Delete(inputPath);
            File.Delete(outputPath);
        }
    }

    [Fact]
    public async Task RejectsWithCycles()
    {
        var sql = string.Join(Environment.NewLine, new[]
        {
            "WITH first AS (SELECT data.value AS v FROM second),",
            "second AS (SELECT data.value AS v FROM first)",
            "SELECT v INTO out FROM first;"
        });

        var previousError = Console.Error;
        var errorWriter = new StringWriter();
        Console.SetError(errorWriter);

        try
        {
            var args = new[] { "--query", sql, "--output", "out=-" };
            var exitCode = await Program.Main(args);
            Assert.Equal(1, exitCode);
            Assert.Contains("WITH definitions contain a cycle", errorWriter.ToString().Trim());
        }
        finally
        {
            Console.SetError(previousError);
        }
    }

    [Fact]
    public async Task AllowsDuplicateSelectOutputsWithWithDefinitions()
    {
        var sql = string.Join(Environment.NewLine, new[]
        {
            "WITH source AS (SELECT data.value AS v FROM input)",
            "SELECT v INTO shared_out FROM source;",
            "SELECT v INTO shared_out FROM source;"
        });

        var inputPath = Path.GetTempFileName();
        var outputPath = Path.GetTempFileName();

        try
        {
            await File.WriteAllTextAsync(inputPath, "{\"data\":{\"value\":1}}\n");
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
            Assert.Equal(new[] { "{\"v\":1}", "{\"v\":1}" }, lines);
        }
        finally
        {
            File.Delete(inputPath);
            File.Delete(outputPath);
        }
    }

    [Fact]
    public async Task SupportsImplicitStdinBindingWithWith()
    {
        var sql = string.Join(Environment.NewLine, new[]
        {
            "WITH filtered AS (SELECT data.value AS v FROM input)",
            "SELECT v FROM filtered;"
        });

        var input = string.Join('\n', new[]
        {
            "{\"data\":{\"value\":9}}",
            "{\"data\":{\"value\":10}}"
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
            var lines = stdoutText.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
            Assert.Equal(new[] { "{\"v\":9}", "{\"v\":10}" }, lines);
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
    public async Task AllowsImplicitStdoutForMultipleSelects()
    {
        var sql = string.Join(Environment.NewLine, new[]
        {
            "WITH source AS (SELECT data.value AS v FROM input)",
            "SELECT v FROM source;",
            "SELECT COUNT(*) AS count FROM source;"
        });

        var stdinStream = new MemoryStream();
        var stdoutStream = new MemoryStream();

        try
        {
            var input = string.Join('\n', new[]
            {
                "{\"data\":{\"value\":9}}",
                "{\"data\":{\"value\":10}}"
            }) + "\n";

            stdinStream = new MemoryStream(Encoding.UTF8.GetBytes(input));

            StreamReaderFactory.InputOverride = stdinStream;
            StreamReaderFactory.OutputOverride = stdoutStream;

            var args = new[] { "--query", sql };
            var exitCode = await Program.Main(args);
            Assert.Equal(0, exitCode);

            stdoutStream.Position = 0;
            using var reader = new StreamReader(stdoutStream, leaveOpen: true);
            var stdoutText = await reader.ReadToEndAsync();
            var lines = stdoutText.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
            Assert.Equal(new[] { "{\"v\":9}", "{\"v\":10}", "{\"count\":2}" }, lines);
        }
        finally
        {
            StreamReaderFactory.InputOverride = null;
            StreamReaderFactory.OutputOverride = null;
            stdinStream.Dispose();
            stdoutStream.Dispose();
        }
    }
}

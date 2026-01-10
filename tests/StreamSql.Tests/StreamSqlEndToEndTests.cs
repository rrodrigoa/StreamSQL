using StreamSql;
using Xunit;

namespace StreamSql.Tests;

public class StreamSqlEndToEndTests
{
    [Fact]
    public async Task ExecutesProjectionIntoFile()
    {
        var sql = "SELECT data.value INTO output FROM input";
        var input = string.Join('\n', new[]
        {
            "{\"data\":{\"value\":1}}",
            "{\"data\":{\"value\":2}}"
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
                "--output", $"output={outputPath}"
            };

            var exitCode = await Program.Main(args);
            Assert.Equal(0, exitCode);

            var lines = (await File.ReadAllTextAsync(outputPath))
                .Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
            Assert.Equal(new[] { "{\"value\":1}", "{\"value\":2}" }, lines);
        }
        finally
        {
            File.Delete(inputPath);
            File.Delete(outputPath);
        }
    }

    [Fact]
    public async Task ExecutesProjectionWithTimestampBy()
    {
        var sql = "SELECT data.value INTO output FROM input TIMESTAMP BY ts";
        var input = string.Join('\n', new[]
        {
            "{\"ts\":1000,\"data\":{\"value\":3}}",
            "{\"ts\":2000,\"data\":{\"value\":4}}"
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
                "--output", $"output={outputPath}"
            };

            var exitCode = await Program.Main(args);
            Assert.Equal(0, exitCode);

            var lines = (await File.ReadAllTextAsync(outputPath))
                .Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
            Assert.Equal(new[] { "{\"value\":3}", "{\"value\":4}" }, lines);
        }
        finally
        {
            File.Delete(inputPath);
            File.Delete(outputPath);
        }
    }

    [Fact]
    public async Task RejectsMultipleStatements()
    {
        var sql = string.Join(Environment.NewLine, new[]
        {
            "SELECT data.value INTO output FROM input;",
            "SELECT data.value INTO output FROM input;"
        });

        var input = "{\"data\":{\"value\":1}}\n";
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
                "--output", $"output={outputPath}"
            };

            var exitCode = await Program.Main(args);
            Assert.Equal(1, exitCode);
        }
        finally
        {
            File.Delete(inputPath);
            File.Delete(outputPath);
        }
    }
}

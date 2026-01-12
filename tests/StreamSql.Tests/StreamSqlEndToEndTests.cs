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
    public async Task ExecutesWhereClause()
    {
        var sql = "SELECT data.value INTO output FROM input WHERE data.value > 10";
        var input = string.Join('\n', new[]
        {
            "{\"data\":{\"value\":5}}",
            "{\"data\":{\"value\":12}}",
            "{\"data\":{\"value\":20}}"
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
            Assert.Equal(new[] { "{\"value\":12}", "{\"value\":20}" }, lines);
        }
        finally
        {
            File.Delete(inputPath);
            File.Delete(outputPath);
        }
    }

    [Fact]
    public async Task ExecutesMultipleStatements()
    {
        var sql = string.Join(Environment.NewLine, new[]
        {
            "SELECT data.value INTO output_one FROM input_one;",
            "SELECT payload.name INTO output_two FROM input_two;"
        });

        var inputOne = string.Join('\n', new[]
        {
            "{\"data\":{\"value\":10}}",
            "{\"data\":{\"value\":11}}"
        }) + "\n";
        var inputTwo = string.Join('\n', new[]
        {
            "{\"payload\":{\"name\":\"alpha\"}}",
            "{\"payload\":{\"name\":\"beta\"}}"
        }) + "\n";

        var inputOnePath = Path.GetTempFileName();
        var inputTwoPath = Path.GetTempFileName();
        var outputOnePath = Path.GetTempFileName();
        var outputTwoPath = Path.GetTempFileName();

        try
        {
            await File.WriteAllTextAsync(inputOnePath, inputOne);
            await File.WriteAllTextAsync(inputTwoPath, inputTwo);
            await File.WriteAllTextAsync(outputOnePath, string.Empty);
            await File.WriteAllTextAsync(outputTwoPath, string.Empty);

            var args = new[]
            {
                "--query", sql,
                "--input", $"input_one={inputOnePath}",
                "--input", $"input_two={inputTwoPath}",
                "--output", $"output_one={outputOnePath}",
                "--output", $"output_two={outputTwoPath}"
            };

            var exitCode = await Program.Main(args);
            Assert.Equal(0, exitCode);

            var outputOneLines = (await File.ReadAllTextAsync(outputOnePath))
                .Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
            var outputTwoLines = (await File.ReadAllTextAsync(outputTwoPath))
                .Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);

            Assert.Equal(new[] { "{\"value\":10}", "{\"value\":11}" }, outputOneLines);
            Assert.Equal(new[] { "{\"name\":\"alpha\"}", "{\"name\":\"beta\"}" }, outputTwoLines);
        }
        finally
        {
            File.Delete(inputOnePath);
            File.Delete(inputTwoPath);
            File.Delete(outputOnePath);
            File.Delete(outputTwoPath);
        }
    }
}

using StreamSql;
using Xunit;

namespace StreamSql.Tests;

public class StreamSqlWithEndToEndTests
{
    [Fact]
    public async Task RejectsUnknownInput()
    {
        var sql = "SELECT data.value INTO output FROM missing";
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

    [Fact]
    public async Task RejectsUnknownOutput()
    {
        var sql = "SELECT data.value INTO missing FROM input";
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

using StreamSql.Cli;
using Xunit;

namespace StreamSql.Tests;

public class CommandLineOptionsTests
{
    [Fact]
    public void ParsesQueryFileAndInputFile()
    {
        var args = new[] { "--file", "input.json", "query.sql" };

        var success = CommandLineOptions.TryParse(args, out var options, out var error);

        Assert.True(success, error);
        Assert.NotNull(options);
        Assert.True(options!.Inputs.TryGetValue(CommandLineOptions.DefaultInputName, out var input));
        Assert.Equal(InputSourceKind.File, input.Kind);
        Assert.Equal("input.json", input.Path);
        Assert.Equal("query.sql", options.QueryFilePath);
    }

    [Fact]
    public void RejectsFollowWithoutFile()
    {
        var args = new[] { "--follow", "query.sql" };

        var success = CommandLineOptions.TryParse(args, out _, out var error);

        Assert.False(success);
        Assert.Equal("Follow and tail modes can only be used with file inputs.", error);
    }

    [Fact]
    public void RejectsMultipleStdinInputs()
    {
        var args = new[]
        {
            "--query", "SELECT data.value FROM input",
            "--input", "first=-",
            "--input", "second=-"
        };

        var success = CommandLineOptions.TryParse(args, out _, out var error);

        Assert.False(success);
        Assert.Equal("Only one input may use stdin.", error);
    }

    [Fact]
    public void RejectsMultipleStdoutOutputs()
    {
        var args = new[]
        {
            "--query", "SELECT data.value FROM input",
            "--output", "first=-",
            "--output", "second=-"
        };

        var success = CommandLineOptions.TryParse(args, out _, out var error);

        Assert.False(success);
        Assert.Equal("Only one output may use stdout.", error);
    }
}

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
        Assert.Equal("input.json", options!.InputFilePath);
        Assert.Equal("query.sql", options.QueryFilePath);
    }

    [Fact]
    public void RejectsFollowWithoutFile()
    {
        var args = new[] { "--follow", "query.sql" };

        var success = CommandLineOptions.TryParse(args, out _, out var error);

        Assert.False(success);
        Assert.Equal("--follow can only be used with --file.", error);
    }

    [Fact]
    public void ParsesWindowDefinition()
    {
        var args = new[] { "--window", "rolling:10s,5s", "--query", "SELECT COUNT(*) FROM input" };

        var success = CommandLineOptions.TryParse(args, out var options, out var error);

        Assert.True(success, error);
        Assert.NotNull(options);
        Assert.NotNull(options!.Window);
        Assert.Equal(WindowType.Rolling, options.Window!.Type);
        Assert.Equal(TimeSpan.FromSeconds(10), options.Window.Size);
        Assert.Equal(TimeSpan.FromSeconds(5), options.Window.Slide);
    }

    [Fact]
    public void ParsesSlidingWindowDefinition()
    {
        var args = new[] { "--window", "sliding:15s", "--query", "SELECT COUNT(*) FROM input" };

        var success = CommandLineOptions.TryParse(args, out var options, out var error);

        Assert.True(success, error);
        Assert.NotNull(options);
        Assert.NotNull(options!.Window);
        Assert.Equal(WindowType.Sliding, options.Window!.Type);
        Assert.Equal(TimeSpan.FromSeconds(15), options.Window.Size);
        Assert.Null(options.Window.Slide);
    }
}

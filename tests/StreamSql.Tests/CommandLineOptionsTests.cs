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
    public void ParsesTimestampBy()
    {
        var args = new[] { "--timestamp-by", "eventTime", "--query", "SELECT COUNT(*) FROM input" };

        var success = CommandLineOptions.TryParse(args, out var options, out var error);

        Assert.True(success, error);
        Assert.NotNull(options);
        Assert.Equal("eventTime", options!.EventTimeField);
    }
}

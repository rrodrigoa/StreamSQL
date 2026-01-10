using ChronosQL.Engine.Sql;
using Xunit;

namespace StreamSql.Tests;

public class SqlSyntaxTests
{
    [Fact]
    public void ParsesSimpleSelectInto()
        => AssertParsesSupportedSql("SELECT data.value INTO output FROM input");

    [Fact]
    public void ParsesTimestampBy()
        => AssertParsesSupportedSql("SELECT data.value INTO output FROM input TIMESTAMP BY ts");

    [Fact]
    public void RejectsMissingInto()
        => AssertRejectsUnsupportedSql("SELECT data.value FROM input", "INTO");

    [Fact]
    public void RejectsWhereClause()
        => AssertRejectsUnsupportedSql("SELECT data.value INTO output FROM input WHERE data.value > 10", "WHERE");

    [Fact]
    public void RejectsMultipleInputs()
        => AssertRejectsUnsupportedSql("SELECT data.value INTO output FROM input, other", "one input");

    private static void AssertParsesSupportedSql(string sql)
    {
        var exception = Record.Exception(() => SqlParser.Parse(sql));
        Assert.Null(exception);
    }

    private static void AssertRejectsUnsupportedSql(string sql, string expectedMessage)
    {
        var exception = Assert.Throws<InvalidOperationException>(() => SqlParser.Parse(sql));
        Assert.Contains(expectedMessage, exception.Message);
    }
}

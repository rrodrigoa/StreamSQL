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
    public void ParsesSelectAll()
        => AssertParsesSupportedSql("SELECT * INTO output FROM input");

    [Fact]
    public void RejectsMissingInto()
        => AssertRejectsUnsupportedSql("SELECT data.value FROM input", "INTO");

    [Fact]
    public void RejectsMultipleInputs()
        => AssertRejectsUnsupportedSql("SELECT data.value INTO output FROM input, other", "one input");

    [Fact]
    public void RejectsSelectAllWithAdditionalFields()
        => AssertRejectsUnsupportedSql("SELECT *, data.value INTO output FROM input", "SELECT * cannot be combined");

    [Theory]
    [InlineData("SELECT data.value INTO output FROM input WHERE data.value = 10")]
    [InlineData("SELECT data.value INTO output FROM input WHERE data.value <> 10")]
    [InlineData("SELECT data.value INTO output FROM input WHERE data.value != 10")]
    [InlineData("SELECT data.value INTO output FROM input WHERE data.value > 10")]
    [InlineData("SELECT data.value INTO output FROM input WHERE data.value >= 10")]
    [InlineData("SELECT data.value INTO output FROM input WHERE data.value < 10")]
    [InlineData("SELECT data.value INTO output FROM input WHERE data.value <= 10")]
    [InlineData("SELECT data.value INTO output FROM input WHERE data.value !< 10")]
    [InlineData("SELECT data.value INTO output FROM input WHERE data.value !> 10")]
    [InlineData("SELECT data.value INTO output FROM input WHERE data.name LIKE 'a%'")]
    [InlineData("SELECT data.value INTO output FROM input WHERE data.name NOT LIKE 'a%'")]
    [InlineData("SELECT data.value INTO output FROM input WHERE data.value BETWEEN 1 AND 5")]
    [InlineData("SELECT data.value INTO output FROM input WHERE data.value NOT BETWEEN 1 AND 5")]
    [InlineData("SELECT data.value INTO output FROM input WHERE data.value IS NULL")]
    [InlineData("SELECT data.value INTO output FROM input WHERE data.value IS NOT NULL")]
    [InlineData("SELECT data.value INTO output FROM input WHERE data.value IN (1, 2, 3)")]
    [InlineData("SELECT data.value INTO output FROM input WHERE data.value NOT IN (1, 2, 3)")]
    public void ParsesWhereClause(string sql)
        => AssertParsesSupportedSql(sql);

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

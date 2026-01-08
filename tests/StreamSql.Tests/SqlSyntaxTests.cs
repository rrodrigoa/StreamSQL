using ChronosQL.Engine.Sql;
using Xunit;

namespace StreamSql.Tests;

public class SqlSyntaxTests
{
    [Fact]
    public void ParsesMultiSelectScriptInOrder()
    {
        var sql = "SELECT data.value INTO out1 FROM first; SELECT data.value INTO out2 FROM second;";

        var scriptPlan = SqlParser.ParseScript(sql);

        Assert.Equal(2, scriptPlan.Statements.Count);
        Assert.Equal("first", scriptPlan.Statements[0].Inputs[0].Name);
        Assert.Equal("second", scriptPlan.Statements[1].Inputs[0].Name);
    }

    [Fact]
    public void ParsesSupportedSql_Projection()
        => AssertParsesSupportedSql("SELECT data.value FROM input");

    [Fact]
    public void ParsesSupportedSql_Alias()
        => AssertParsesSupportedSql("SELECT data.value AS v FROM input");

    [Fact]
    public void ParsesSupportedSql_MultipleFields()
        => AssertParsesSupportedSql("SELECT data.value, data.other FROM input");

    [Fact]
    public void ParsesSupportedSql_WhereAnd()
        => AssertParsesSupportedSql("SELECT data.value FROM input WHERE data.value > 10 AND data.other < 5");

    [Fact]
    public void ParsesSupportedSql_CountStar()
        => AssertParsesSupportedSql("SELECT COUNT(*) FROM input");

    [Fact]
    public void ParsesSupportedSql_Avg()
        => AssertParsesSupportedSql("SELECT AVG(data.value) FROM input");

    [Fact]
    public void ParsesSupportedSql_GroupBy()
        => AssertParsesSupportedSql("SELECT data.category, COUNT(*) FROM input GROUP BY data.category");

    [Fact]
    public void ParsesSupportedSql_HavingWithAggregate()
        => AssertParsesSupportedSql("SELECT data.category, COUNT(*) FROM input GROUP BY data.category HAVING COUNT(*) > 1");

    [Fact]
    public void ParsesSupportedSql_HavingWithGroupedField()
        => AssertParsesSupportedSql("SELECT data.category, COUNT(*) FROM input GROUP BY data.category HAVING data.category = 'a'");

    [Fact]
    public void ParsesSupportedSql_OrderBy()
        => AssertParsesSupportedSql("SELECT data.value FROM input ORDER BY data.value DESC");

    [Fact]
    public void ParsesSupportedSql_WindowInGroupBy()
        => AssertParsesSupportedSql("SELECT COUNT(*) FROM input GROUP BY TUMBLINGWINDOW(second, 5)");

    [Fact]
    public void RejectsUnsupportedSql_InvalidSql()
        => AssertRejectsUnsupportedSql("SELECT FROM input", "SQL parse error");

    [Fact]
    public void RejectsUnsupportedSql_AggregateWithoutGroupBy()
        => AssertRejectsUnsupportedSql("SELECT data.value, COUNT(*) FROM input", "Aggregate queries must GROUP BY all non-aggregated fields.");

    [Fact]
    public void RejectsUnsupportedSql_GroupByMismatch()
        => AssertRejectsUnsupportedSql("SELECT data.a, COUNT(*) FROM input GROUP BY data.b", "GROUP BY mismatch");

    [Fact]
    public void RejectsUnsupportedSql_HavingWithoutAggregate()
        => AssertRejectsUnsupportedSql("SELECT data.value FROM input HAVING COUNT(*) > 1", "Unsupported SQL syntax detected");

    [Fact]
    public void RejectsUnsupportedSql_HavingWithoutGroupBy()
        => AssertRejectsUnsupportedSql("SELECT COUNT(*) FROM input HAVING data.value > 1", "HAVING column requires GROUP BY");

    [Fact]
    public void RejectsUnsupportedSql_HavingGroupByMismatch()
        => AssertRejectsUnsupportedSql("SELECT data.a, COUNT(*) FROM input GROUP BY data.a HAVING data.b > 1", "HAVING column must appear in GROUP BY");

    [Fact]
    public void RejectsUnsupportedSql_WindowOutsideGroupBy()
        => AssertRejectsUnsupportedSql("SELECT TUMBLINGWINDOW(second, 5) FROM input", "Window functions are only supported in GROUP BY.");

    [Fact]
    public void RejectsUnsupportedSql_MultipleWindowFunctions()
        => AssertRejectsUnsupportedSql("SELECT COUNT(*) FROM input GROUP BY TUMBLINGWINDOW(second, 5), SLIDINGWINDOW(second, 1)", "Only one window function can appear in GROUP BY.");

    [Fact]
    public void RejectsUnsupportedSql_InvalidTimeUnit()
        => AssertRejectsUnsupportedSql("SELECT COUNT(*) FROM input GROUP BY TUMBLINGWINDOW(weeks, 5)", "Unsupported time unit");

    [Fact]
    public void RejectsUnsupportedSql_NonNumericWindowSize()
        => AssertRejectsUnsupportedSql("SELECT COUNT(*) FROM input GROUP BY TUMBLINGWINDOW(second, 'five')", "Window size must be a positive integer literal.");

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

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
        Assert.Equal("first", scriptPlan.Statements[0].InputStream);
        Assert.Equal("second", scriptPlan.Statements[1].InputStream);
    }

    [Theory]
    [MemberData(nameof(ParserCases))]
    public void ParsesSupportedSql(string name, string sql)
    {
        var exception = Record.Exception(() => SqlParser.Parse(sql));
        Assert.Null(exception);
    }

    [Theory]
    [MemberData(nameof(ParserErrorCases))]
    public void RejectsUnsupportedSql(string name, string sql, string expectedMessage)
    {
        var exception = Assert.Throws<InvalidOperationException>(() => SqlParser.Parse(sql));
        Assert.Contains(expectedMessage, exception.Message);
    }

    public static IEnumerable<object[]> ParserCases()
    {
        yield return new object[] { "Projection", "SELECT data.value FROM input" };
        yield return new object[] { "Alias", "SELECT data.value AS v FROM input" };
        yield return new object[] { "Multiple fields", "SELECT data.value, data.other FROM input" };
        yield return new object[] { "Where and", "SELECT data.value FROM input WHERE data.value > 10 AND data.other < 5" };
        yield return new object[] { "Count star", "SELECT COUNT(*) FROM input" };
        yield return new object[] { "Avg", "SELECT AVG(data.value) FROM input" };
        yield return new object[] { "Group by", "SELECT data.category, COUNT(*) FROM input GROUP BY data.category" };
        yield return new object[] { "Having with aggregate", "SELECT data.category, COUNT(*) FROM input GROUP BY data.category HAVING COUNT(*) > 1" };
        yield return new object[] { "Having with grouped field", "SELECT data.category, COUNT(*) FROM input GROUP BY data.category HAVING data.category = 'a'" };
        yield return new object[] { "Order by", "SELECT data.value FROM input ORDER BY data.value DESC" };
        yield return new object[] { "Window in group by", "SELECT COUNT(*) FROM input GROUP BY TUMBLINGWINDOW(second, 5)" };
    }

    public static IEnumerable<object[]> ParserErrorCases()
    {
        yield return new object[] { "Invalid SQL", "SELECT FROM input", "SQL parse error" };
        yield return new object[] { "Aggregate without group by", "SELECT data.value, COUNT(*) FROM input", "Aggregate queries must GROUP BY all non-aggregated fields." };
        yield return new object[] { "Group by mismatch", "SELECT data.a, COUNT(*) FROM input GROUP BY data.b", "GROUP BY mismatch" };
        yield return new object[] { "Having without aggregate", "SELECT data.value FROM input HAVING COUNT(*) > 1", "Unsupported SQL syntax detected" };
        yield return new object[] { "Having without group by", "SELECT COUNT(*) FROM input HAVING data.value > 1", "HAVING column requires GROUP BY" };
        yield return new object[] { "Having group by mismatch", "SELECT data.a, COUNT(*) FROM input GROUP BY data.a HAVING data.b > 1", "HAVING column must appear in GROUP BY" };
        yield return new object[] { "Window outside group by", "SELECT TUMBLINGWINDOW(second, 5) FROM input", "Window functions are only supported in GROUP BY." };
        yield return new object[] { "Multiple window functions", "SELECT COUNT(*) FROM input GROUP BY TUMBLINGWINDOW(second, 5), SLIDINGWINDOW(second, 1)", "Only one window function can appear in GROUP BY." };
        yield return new object[] { "Invalid time unit", "SELECT COUNT(*) FROM input GROUP BY TUMBLINGWINDOW(weeks, 5)", "Unsupported time unit" };
        yield return new object[] { "Non-numeric window size", "SELECT COUNT(*) FROM input GROUP BY TUMBLINGWINDOW(second, 'five')", "Window size must be a positive integer literal." };
    }
}

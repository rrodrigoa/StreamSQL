using ChronosQL.Engine.Sql;
using Xunit;

namespace StreamSql.Tests;

public class SqlSyntaxTests
{
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
    }

    public static IEnumerable<object[]> ParserErrorCases()
    {
        yield return new object[] { "Invalid SQL", "SELECT FROM input", "SQL parse error" };
        yield return new object[] { "Aggregate without group by", "SELECT data.value, COUNT(*) FROM input", "Aggregate queries must GROUP BY all non-aggregated fields." };
        yield return new object[] { "Group by mismatch", "SELECT data.a, COUNT(*) FROM input GROUP BY data.b", "GROUP BY mismatch" };
        yield return new object[] { "Having without aggregate", "SELECT data.value FROM input HAVING COUNT(*) > 1", "Unsupported SQL syntax detected" };
        yield return new object[] { "Having without group by", "SELECT COUNT(*) FROM input HAVING data.value > 1", "HAVING column requires GROUP BY" };
        yield return new object[] { "Having group by mismatch", "SELECT data.a, COUNT(*) FROM input GROUP BY data.a HAVING data.b > 1", "HAVING column must appear in GROUP BY" };
    }
}

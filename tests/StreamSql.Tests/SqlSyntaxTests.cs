using StreamSql.Sql;
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
        yield return new object[] { "Boolean filter", "SELECT data.flag FROM input WHERE data.flag = true" };
        yield return new object[] { "Count star", "SELECT COUNT(*) FROM input" };
        yield return new object[] { "Avg", "SELECT AVG(data.value) FROM input" };
        yield return new object[] { "Group by", "SELECT data.category, COUNT(*) FROM input GROUP BY data.category" };
    }

    public static IEnumerable<object[]> ParserErrorCases()
    {
        yield return new object[] { "Invalid SQL", "SELECT FROM input", "SQL parse error" };
        yield return new object[] { "Nested path", "SELECT data.inner.value FROM input", "Nested JSON paths beyond one level require ChronosQL Pro" };
        yield return new object[] { "Aggregate without group by", "SELECT data.value, COUNT(*) FROM input", "Aggregate queries must GROUP BY all non-aggregated fields." };
        yield return new object[] { "Group by mismatch", "SELECT data.a, COUNT(*) FROM input GROUP BY data.b", "GROUP BY mismatch" };
    }
}

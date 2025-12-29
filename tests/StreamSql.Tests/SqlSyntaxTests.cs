using StreamSql;
using Xunit;

namespace StreamSql.Tests;

public class SqlSyntaxTests
{
    [Theory]
    [MemberData(nameof(PositiveCases))]
    public async Task ExecutesAllowedSqlQueries(string sql, string inputPayload, string[] expectedLines, string[]? extraArgs)
    {
        var inputPath = Path.GetTempFileName();
        var outputPath = Path.GetTempFileName();

        try
        {
            await File.WriteAllTextAsync(inputPath, inputPayload);
            await File.WriteAllTextAsync(outputPath, string.Empty);

            var args = new List<string> { "--query", sql, "--file", inputPath, "--out", outputPath };
            if (extraArgs is not null && extraArgs.Length > 0)
            {
                args.AddRange(extraArgs);
            }

            var exitCode = await Program.Main(args.ToArray());

            Assert.Equal(0, exitCode);

            var output = await File.ReadAllTextAsync(outputPath);
            var lines = output.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);

            Assert.Equal(expectedLines.Length, lines.Length);

            for (var i = 0; i < expectedLines.Length; i++)
            {
                Assert.Equal(expectedLines[i], lines[i]);
            }
        }
        finally
        {
            File.Delete(inputPath);
            File.Delete(outputPath);
        }
    }

    [Theory]
    [MemberData(nameof(NegativeCases))]
    public async Task RejectsUnsupportedSqlQueries(string sql, string inputPayload)
    {
        var inputPath = Path.GetTempFileName();
        var outputPath = Path.GetTempFileName();

        try
        {
            await File.WriteAllTextAsync(inputPath, inputPayload);
            await File.WriteAllTextAsync(outputPath, string.Empty);

            await Assert.ThrowsAsync<InvalidOperationException>(
                () => Program.Main(new[] { "--query", sql, "--file", inputPath, "--out", outputPath }));

            var output = await File.ReadAllTextAsync(outputPath);
            Assert.True(string.IsNullOrWhiteSpace(output));
        }
        finally
        {
            File.Delete(inputPath);
            File.Delete(outputPath);
        }
    }

    public static IEnumerable<object[]> PositiveCases()
    {
        yield return new object[]
        {
            "SELECT value FROM input",
            "{\"value\":1}\n{\"value\":2}\n",
            new[] { "{\"value\":1}", "{\"value\":2}" },
            null
        };
        yield return new object[]
        {
            "SELECT data.value FROM input",
            "{\"data\":{\"value\":3}}\n{\"data\":{\"value\":4}}\n",
            new[] { "{\"value\":3}", "{\"value\":4}" },
            null
        };
        yield return new object[]
        {
            "SELECT data.value AS v FROM input",
            "{\"data\":{\"value\":5}}\n",
            new[] { "{\"v\":5}" },
            null
        };
        yield return new object[]
        {
            "SELECT value FROM input WHERE value > 5",
            "{\"value\":4}\n{\"value\":6}\n{\"value\":8}\n",
            new[] { "{\"value\":6}", "{\"value\":8}" },
            null
        };
        yield return new object[]
        {
            "SELECT value FROM input WHERE value > 5.5",
            "{\"value\":5.4}\n{\"value\":5.6}\n",
            new[] { "{\"value\":5.6}" },
            null
        };
        yield return new object[]
        {
            "SELECT data.value FROM input WHERE data.value > 0",
            "{\"data\":{\"value\":-1}}\n{\"data\":{\"value\":2}}\n",
            new[] { "{\"value\":2}" },
            null
        };
        yield return new object[]
        {
            "SELECT value, other FROM input",
            "{\"value\":1,\"other\":2}\n",
            new[] { "{\"value\":1,\"other\":2}" },
            null
        };
        yield return new object[]
        {
            "SELECT data.value, data.other FROM input",
            "{\"data\":{\"value\":1,\"other\":2}}\n",
            new[] { "{\"value\":1,\"other\":2}" },
            null
        };
        yield return new object[]
        {
            "SELECT data.value AS first, data.other AS second FROM input",
            "{\"data\":{\"value\":1,\"other\":2}}\n",
            new[] { "{\"first\":1,\"second\":2}" },
            null
        };
        yield return new object[]
        {
            "SELECT input.value FROM input",
            "{\"value\":3}\n",
            new[] { "{\"value\":3}" },
            null
        };
        yield return new object[]
        {
            "SELECT input.data.value FROM input",
            "{\"data\":{\"value\":7}}\n",
            new[] { "{\"value\":7}" },
            null
        };
        yield return new object[]
        {
            "SELECT value INTO output FROM input",
            "{\"value\":1}\n",
            new[] { "{\"value\":1}" },
            null
        };
        yield return new object[]
        {
            "SELECT value FROM input",
            "{\"timestamp\":1000,\"value\":4}\n",
            new[] { "{\"value\":4}" },
            new[] { "--timestamp-by", "timestamp" }
        };
        yield return new object[]
        {
            "SELECT value FROM input",
            "{\"data\":{\"timestamp\":2000},\"value\":5}\n",
            new[] { "{\"value\":5}" },
            new[] { "--timestamp-by", "data.timestamp" }
        };
        yield return new object[]
        {
            "SELECT value FROM input",
            "{\"ts\":\"2024-01-01T00:00:00Z\",\"value\":6}\n",
            new[] { "{\"value\":6}" },
            new[] { "--timestamp-by", "ts" }
        };
        yield return new object[]
        {
            "SELECT value FROM input WHERE value > 0",
            "{\"ts\":1,\"value\":-1}\n{\"ts\":2,\"value\":1}\n",
            new[] { "{\"value\":1}" },
            new[] { "--timestamp-by", "ts" }
        };
        yield return new object[]
        {
            "SELECT value FROM dbo.input",
            "{\"value\":9}\n",
            new[] { "{\"value\":9}" },
            null
        };
        yield return new object[]
        {
            "SELECT data.value FROM dbo.input",
            "{\"data\":{\"value\":10}}\n",
            new[] { "{\"value\":10}" },
            null
        };
        yield return new object[]
        {
            "SELECT value AS v FROM input WHERE value > 1",
            "{\"value\":1}\n{\"value\":2}\n",
            new[] { "{\"v\":2}" },
            null
        };
        yield return new object[]
        {
            "SELECT value, data.value AS nested FROM input",
            "{\"value\":1,\"data\":{\"value\":2}}\n",
            new[] { "{\"value\":1,\"nested\":2}" },
            null
        };
        yield return new object[]
        {
            "SELECT data.inner.value AS val FROM input",
            "{\"data\":{\"inner\":{\"value\":11}}}\n",
            new[] { "{\"val\":11}" },
            null
        };
        yield return new object[]
        {
            "SELECT value FROM input WHERE value > 0.0",
            "{\"value\":0}\n{\"value\":0.1}\n",
            new[] { "{\"value\":0.1}" },
            null
        };
        yield return new object[]
        {
            "SELECT value FROM input WHERE value > 100",
            "{\"value\":100}\n{\"value\":101}\n",
            new[] { "{\"value\":101}" },
            null
        };
        yield return new object[]
        {
            "SELECT data.value FROM input WHERE data.value > 5.1",
            "{\"data\":{\"value\":5.0,\"ts\":1}}\n{\"data\":{\"value\":5.2,\"ts\":2}}\n",
            new[] { "{\"value\":5.2}" },
            new[] { "--timestamp-by", "data.ts" }
        };
        yield return new object[]
        {
            "SELECT data.value FROM input WHERE data.value > 5",
            "{\"data\":{\"value\":4,\"ts\":\"2024-02-01T00:00:00Z\"}}\n{\"data\":{\"value\":6,\"ts\":\"2024-02-01T00:00:01Z\"}}\n",
            new[] { "{\"value\":6}" },
            new[] { "--timestamp-by", "data.ts" }
        };
        yield return new object[]
        {
            "SELECT value FROM input WHERE value > 1.234",
            "{\"value\":1.233}\n{\"value\":1.235}\n",
            new[] { "{\"value\":1.235}" },
            null
        };
        yield return new object[]
        {
            "SELECT value\nFROM input\nWHERE value > 1",
            "{\"value\":1}\n{\"value\":2}\n",
            new[] { "{\"value\":2}" },
            null
        };
        yield return new object[]
        {
            "SELECT [value] FROM input",
            "{\"value\":12}\n",
            new[] { "{\"value\":12}" },
            null
        };
        yield return new object[]
        {
            "SELECT [data].[value] FROM input",
            "{\"data\":{\"value\":13}}\n",
            new[] { "{\"value\":13}" },
            null
        };
        yield return new object[]
        {
            "SELECT SUM(value) FROM input",
            "{\"value\":1}\n{\"value\":2}\n{\"value\":3}\n",
            new[] { "{\"sum\":6}" },
            null
        };
        yield return new object[]
        {
            "SELECT SUM(value) FROM input",
            "{\"ts\":1000,\"value\":1}\n{\"ts\":1500,\"value\":2}\n{\"ts\":2500,\"value\":3}\n",
            new[] { "{\"windowStart\":1000,\"windowEnd\":2000,\"sum\":3}", "{\"windowStart\":2000,\"windowEnd\":3000,\"sum\":3}" },
            new[] { "--timestamp-by", "ts", "--tumbling-window", "1000" }
        };
        yield return new object[]
        {
            "SELECT value, other FROM input WHERE other > 1",
            "{\"value\":1,\"other\":1}\n{\"value\":2,\"other\":2}\n",
            new[] { "{\"value\":2,\"other\":2}" },
            null
        };
    }

    public static IEnumerable<object[]> NegativeCases()
    {
        const string payload = "{\"value\":1,\"other\":2,\"id\":1}\n";

        yield return new object[] { "SELCT value FROM input", payload };
        yield return new object[] { "SELECT value FROM", payload };
        yield return new object[] { "SELECT value FROM input WHERE", payload };
        yield return new object[] { "SELECT value FROM input WHERE value >", payload };
        yield return new object[] { "SELECT value FROM input WHERE value > 5.5.5", payload };
        yield return new object[] { "SELECT * FROM input", payload };
        yield return new object[] { "SELECT 1 FROM input", payload };
        yield return new object[] { "SELECT value + 1 FROM input", payload };
        yield return new object[] { "SELECT value, 1 FROM input", payload };
        yield return new object[] { "SELECT value FROM input WHERE value < 5", payload };
        yield return new object[] { "SELECT value FROM input WHERE value = 5", payload };
        yield return new object[] { "SELECT value FROM input WHERE value > other", payload };
        yield return new object[] { "SELECT value FROM input WHERE value > 5 AND value < 10", payload };
        yield return new object[] { "SELECT value FROM input WHERE value > 5 OR value < 10", payload };
        yield return new object[] { "SELECT value FROM input WHERE value > '5'", payload };
        yield return new object[] { "SELECT value FROM input WHERE value > 5 + 1", payload };
        yield return new object[] { "SELECT value FROM input WHERE value > -1", payload };
        yield return new object[] { "SELECT value FROM input JOIN other ON input.id = other.id", payload };
        yield return new object[] { "SELECT value FROM (SELECT value FROM input) AS t", payload };
        yield return new object[] { "SELECT value FROM input UNION SELECT value FROM input", payload };
    }
}

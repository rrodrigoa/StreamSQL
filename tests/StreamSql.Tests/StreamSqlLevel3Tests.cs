using StreamSql;
using Xunit;

namespace StreamSql.Tests;

public class StreamSqlLevel3Tests
{
    [Fact]
    public async Task ExecutesJoinWithDatediffMillisecondTumblingWindow()
    {
        var sql = " SELECT" +
                  "     COUNT(*) AS count" +
                  "     INTO joined_out" +
                  " FROM left_input  AS l TIMESTAMP BY l.ts" +
                  " JOIN right_input AS r TIMESTAMP BY r.ts" +
                  "     ON l.id = r.id" +
                  "     AND DATEDIFF(millisecond, l, r) BETWEEN 0 AND 200" +
                  " GROUP BY TUMBLINGWINDOW(millisecond, 1000); ";

        var lines = await ExecuteJoinAsync(
            sql,
            new[]
            {
                "{\"id\":1,\"ts\":1000}",
                "{\"id\":2,\"ts\":1200}",
                "{\"id\":3,\"ts\":2500}"
            },
            new[]
            {
                "{\"id\":1,\"ts\":1000}",
                "{\"id\":2,\"ts\":1400}",
                "{\"id\":3,\"ts\":2601}"
            });

        Assert.Equal(new[]
        {
            "{\"windowStart\":1000,\"windowEnd\":2000,\"count\":2}",
            "{\"windowStart\":2000,\"windowEnd\":3000,\"count\":1}"
        }, lines);
    }

    [Fact]
    public async Task ExecutesJoinWithDatediffSecondHoppingWindowLargeInput()
    {
        var sql = " SELECT" +
                  "     COUNT(*) AS count" +
                  "     INTO joined_out" +
                  " FROM left_input  AS l TIMESTAMP BY l.ts" +
                  " JOIN right_input AS r TIMESTAMP BY r.ts" +
                  "     ON l.id = r.id" +
                  "     AND DATEDIFF(second, l, r) BETWEEN 0 AND 1" +
                  " GROUP BY HOPPINGWINDOW(second, 4, 2); ";

        var lines = await ExecuteJoinAsync(
            sql,
            new[]
            {
                "{\"id\":1,\"ts\":0}",
                "{\"id\":2,\"ts\":1000}",
                "{\"id\":3,\"ts\":2000}",
                "{\"id\":4,\"ts\":3000}",
                "{\"id\":5,\"ts\":4000}",
                "{\"id\":6,\"ts\":5000}"
            },
            new[]
            {
                "{\"id\":1,\"ts\":1000}",
                "{\"id\":2,\"ts\":1000}",
                "{\"id\":3,\"ts\":3000}",
                "{\"id\":4,\"ts\":3000}",
                "{\"id\":5,\"ts\":5000}",
                "{\"id\":6,\"ts\":5000}"
            });

        Assert.Equal(new[]
        {
            "{\"windowStart\":0,\"windowEnd\":4000,\"count\":4}",
            "{\"windowStart\":2000,\"windowEnd\":6000,\"count\":4}",
            "{\"windowStart\":4000,\"windowEnd\":8000,\"count\":2}"
        }, lines);
    }

    [Fact]
    public async Task ExecutesJoinWithDatediffMinuteSlidingWindow()
    {
        var sql = " SELECT" +
                  "     COUNT(*) AS count" +
                  "     INTO joined_out" +
                  " FROM left_input  AS l TIMESTAMP BY l.ts" +
                  " JOIN right_input AS r TIMESTAMP BY r.ts" +
                  "     ON l.id = r.id" +
                  "     AND DATEDIFF(minute, l, r) BETWEEN 0 AND 1" +
                  " GROUP BY SLIDINGWINDOW(minute, 1)" +
                  " ORDER BY windowEnd ASC; ";

        var lines = await ExecuteJoinAsync(
            sql,
            new[]
            {
                "{\"id\":1,\"ts\":60000}",
                "{\"id\":2,\"ts\":120000}",
                "{\"id\":3,\"ts\":180000}"
            },
            new[]
            {
                "{\"id\":1,\"ts\":60000}",
                "{\"id\":2,\"ts\":180000}",
                "{\"id\":3,\"ts\":180000}"
            });

        Assert.Equal(new[]
        {
            "{\"windowStart\":0,\"windowEnd\":60000,\"count\":1}",
            "{\"windowStart\":60000,\"windowEnd\":120000,\"count\":2}",
            "{\"windowStart\":120000,\"windowEnd\":180000,\"count\":2}"
        }, lines);
    }

    [Fact]
    public async Task ExecutesJoinWithDatediffHourTumblingWindow()
    {
        var sql = " SELECT" +
                  "     COUNT(*) AS count" +
                  "     INTO joined_out" +
                  " FROM left_input  AS l TIMESTAMP BY l.ts" +
                  " JOIN right_input AS r TIMESTAMP BY r.ts" +
                  "     ON l.id = r.id" +
                  "     AND DATEDIFF(hour, l, r) BETWEEN 0 AND 1" +
                  " GROUP BY TUMBLINGWINDOW(hour, 1); ";

        var lines = await ExecuteJoinAsync(
            sql,
            new[]
            {
                "{\"id\":1,\"ts\":0}",
                "{\"id\":2,\"ts\":1800000}",
                "{\"id\":3,\"ts\":3600000}",
                "{\"id\":4,\"ts\":5400000}",
                "{\"id\":5,\"ts\":7200000}"
            },
            new[]
            {
                "{\"id\":1,\"ts\":0}",
                "{\"id\":2,\"ts\":2700000}",
                "{\"id\":3,\"ts\":7200000}",
                "{\"id\":4,\"ts\":5400000}",
                "{\"id\":5,\"ts\":10800000}"
            });

        Assert.Equal(new[]
        {
            "{\"windowStart\":0,\"windowEnd\":3600000,\"count\":2}",
            "{\"windowStart\":3600000,\"windowEnd\":7200000,\"count\":2}",
            "{\"windowStart\":7200000,\"windowEnd\":10800000,\"count\":1}"
        }, lines);
    }

    [Fact]
    public async Task FailsJoinWithReversedDatediffBounds()
    {
        var sql = " SELECT" +
                  "     COUNT(*) AS count" +
                  "     INTO joined_out" +
                  " FROM left_input  AS l TIMESTAMP BY l.ts" +
                  " JOIN right_input AS r TIMESTAMP BY r.ts" +
                  "     ON l.id = r.id" +
                  "     AND DATEDIFF(second, l, r) BETWEEN 5 AND 1" +
                  " GROUP BY TUMBLINGWINDOW(second, 1); ";

        var args = new[]
        {
            "--query", sql,
            "--output", "joined_out=-"
        };

        var previousError = Console.Error;
        var errorWriter = new StringWriter();
        Console.SetError(errorWriter);

        try
        {
            var exitCode = await Program.Main(args);
            Assert.Equal(1, exitCode);
            Assert.Contains("DATEDIFF BETWEEN requires min to be less than or equal to max.", errorWriter.ToString());
        }
        finally
        {
            Console.SetError(previousError);
        }
    }

    private static async Task<string[]> ExecuteJoinAsync(string sql, string[] leftLines, string[] rightLines)
    {
        var leftPath = Path.GetTempFileName();
        var rightPath = Path.GetTempFileName();
        var outputPath = Path.GetTempFileName();

        var leftData = leftLines.Length == 0 ? string.Empty : string.Join('\n', leftLines) + "\n";
        var rightData = rightLines.Length == 0 ? string.Empty : string.Join('\n', rightLines) + "\n";

        try
        {
            await File.WriteAllTextAsync(leftPath, leftData);
            await File.WriteAllTextAsync(rightPath, rightData);
            await File.WriteAllTextAsync(outputPath, string.Empty);

            var args = new[]
            {
                "--query", sql,
                "--input", $"left_input={leftPath}",
                "--input", $"right_input={rightPath}",
                "--output", $"joined_out={outputPath}"
            };

            var exitCode = await Program.Main(args);
            Assert.Equal(0, exitCode);

            var output = await File.ReadAllTextAsync(outputPath);
            return output.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
        }
        finally
        {
            File.Delete(leftPath);
            File.Delete(rightPath);
            File.Delete(outputPath);
        }
    }
}

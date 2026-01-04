using System.Linq;
using System.Text.Json;
using StreamSql;
using StreamSql.Input;
using Xunit;

namespace StreamSql.Tests;

public class StreamSqlLevel1Tests
{
    [Theory]
    [MemberData(nameof(Level1Cases))]
    public async Task ExecutesLevel1Cases(EndToEndCase testCase)
    {
        var lines = await ExecuteAsync(testCase);
        Assert.Equal(testCase.ExpectedOutput, lines);

        foreach (var line in lines)
        {
            using var doc = JsonDocument.Parse(line);
            Assert.True(doc.RootElement.ValueKind is JsonValueKind.Object or JsonValueKind.Array);
        }
    }

    public static IEnumerable<object[]> Level1Cases()
    {
        foreach (var testCase in BuildCases())
        {
            yield return new object[] { testCase };
        }
    }

    private static IEnumerable<EndToEndCase> BuildCases()
    {
        yield return new EndToEndCase(
            "L1 Tumbling window with multiple fields",
            "SELECT data.category, data.region, COUNT(*) AS count, SUM(data.value) AS total FROM input TIMESTAMP BY ts GROUP BY TUMBLINGWINDOW(second, 5), data.category, data.region",
            Array.Empty<string>(),
            string.Join('\n', new[]
            {
                "{\"ts\":1000,\"data\":{\"category\":\"a\",\"region\":\"east\",\"value\":2}}",
                "{\"ts\":2000,\"data\":{\"category\":\"a\",\"region\":\"east\",\"value\":3}}",
                "{\"ts\":2000,\"data\":{\"category\":\"b\",\"region\":\"west\",\"value\":4}}",
                "{\"ts\":5000,\"data\":{\"category\":\"a\",\"region\":\"east\",\"value\":5}}",
                "{\"ts\":7000,\"data\":{\"category\":\"a\",\"region\":\"west\",\"value\":1}}",
                "{\"ts\":9000,\"data\":{\"category\":\"b\",\"region\":\"west\",\"value\":2}}",
                "{\"ts\":12000,\"data\":{\"category\":\"b\",\"region\":\"west\",\"value\":3}}"
            }) + "\n",
            new[]
            {
                "{\"windowStart\":0,\"windowEnd\":5000,\"category\":\"a\",\"region\":\"east\",\"count\":2,\"total\":5}",
                "{\"windowStart\":0,\"windowEnd\":5000,\"category\":\"b\",\"region\":\"west\",\"count\":1,\"total\":4}",
                "{\"windowStart\":5000,\"windowEnd\":10000,\"category\":\"a\",\"region\":\"east\",\"count\":1,\"total\":5}",
                "{\"windowStart\":5000,\"windowEnd\":10000,\"category\":\"a\",\"region\":\"west\",\"count\":1,\"total\":1}",
                "{\"windowStart\":5000,\"windowEnd\":10000,\"category\":\"b\",\"region\":\"west\",\"count\":1,\"total\":2}",
                "{\"windowStart\":10000,\"windowEnd\":15000,\"category\":\"b\",\"region\":\"west\",\"count\":1,\"total\":3}"
            },
            "Validate tumbling windows across multiple fields and shared timestamps.");

        yield return new EndToEndCase(
            "L1 Hopping window with ordering",
            "SELECT data.category, COUNT(*) AS count, MAX(data.value) AS maxValue FROM input TIMESTAMP BY ts GROUP BY HOPPINGWINDOW(second, 10, 5), data.category ORDER BY windowStart DESC, count DESC",
            Array.Empty<string>(),
            string.Join('\n', new[]
            {
                "{\"ts\":0,\"data\":{\"category\":\"a\",\"value\":1}}",
                "{\"ts\":3000,\"data\":{\"category\":\"a\",\"value\":2}}",
                "{\"ts\":5000,\"data\":{\"category\":\"b\",\"value\":5}}",
                "{\"ts\":7000,\"data\":{\"category\":\"a\",\"value\":4}}",
                "{\"ts\":10000,\"data\":{\"category\":\"b\",\"value\":3}}",
                "{\"ts\":12000,\"data\":{\"category\":\"b\",\"value\":6}}",
                "{\"ts\":15000,\"data\":{\"category\":\"a\",\"value\":7}}"
            }) + "\n",
            new[]
            {
                "{\"windowStart\":15000,\"windowEnd\":25000,\"category\":\"a\",\"count\":1,\"maxValue\":7}",
                "{\"windowStart\":10000,\"windowEnd\":20000,\"category\":\"b\",\"count\":2,\"maxValue\":6}",
                "{\"windowStart\":10000,\"windowEnd\":20000,\"category\":\"a\",\"count\":1,\"maxValue\":7}",
                "{\"windowStart\":5000,\"windowEnd\":15000,\"category\":\"b\",\"count\":3,\"maxValue\":6}",
                "{\"windowStart\":5000,\"windowEnd\":15000,\"category\":\"a\",\"count\":1,\"maxValue\":4}",
                "{\"windowStart\":0,\"windowEnd\":10000,\"category\":\"a\",\"count\":3,\"maxValue\":4}",
                "{\"windowStart\":0,\"windowEnd\":10000,\"category\":\"b\",\"count\":1,\"maxValue\":5}"
            },
            "Validate rolling windows with ORDER BY output.");

        yield return new EndToEndCase(
            "L1 Sliding window with complex grouping",
            "SELECT data.category, data.region, COUNT(*) AS count, SUM(data.value) AS total FROM input TIMESTAMP BY ts GROUP BY SLIDINGWINDOW(second, 5), data.category, data.region ORDER BY windowEnd ASC, category ASC",
            Array.Empty<string>(),
            string.Join('\n', new[]
            {
                "{\"ts\":6000,\"data\":{\"category\":\"a\",\"region\":\"east\",\"value\":1}}",
                "{\"ts\":6000,\"data\":{\"category\":\"a\",\"region\":\"east\",\"value\":3}}",
                "{\"ts\":8000,\"data\":{\"category\":\"b\",\"region\":\"west\",\"value\":2}}",
                "{\"ts\":11000,\"data\":{\"category\":\"a\",\"region\":\"east\",\"value\":5}}",
                "{\"ts\":12000,\"data\":{\"category\":\"b\",\"region\":\"west\",\"value\":4}}",
                "{\"ts\":14000,\"data\":{\"category\":\"a\",\"region\":\"east\",\"value\":2}}",
                "{\"ts\":16000,\"data\":{\"category\":\"b\",\"region\":\"west\",\"value\":6}}"
            }) + "\n",
            new[]
            {
                "{\"windowStart\":1000,\"windowEnd\":6000,\"category\":\"a\",\"region\":\"east\",\"count\":2,\"total\":4}",
                "{\"windowStart\":3000,\"windowEnd\":8000,\"category\":\"b\",\"region\":\"west\",\"count\":1,\"total\":2}",
                "{\"windowStart\":6000,\"windowEnd\":11000,\"category\":\"a\",\"region\":\"east\",\"count\":3,\"total\":9}",
                "{\"windowStart\":7000,\"windowEnd\":12000,\"category\":\"b\",\"region\":\"west\",\"count\":2,\"total\":6}",
                "{\"windowStart\":9000,\"windowEnd\":14000,\"category\":\"a\",\"region\":\"east\",\"count\":2,\"total\":7}",
                "{\"windowStart\":11000,\"windowEnd\":16000,\"category\":\"b\",\"region\":\"west\",\"count\":2,\"total\":10}"
            },
            "Validate sliding windows per event timestamp and ordered output.");

        yield return new EndToEndCase(
            "L1 Tumbling window with HAVING",
            "SELECT data.category, COUNT(*) AS count FROM input TIMESTAMP BY ts GROUP BY TUMBLINGWINDOW(second, 5), data.category HAVING COUNT(*) > 1",
            Array.Empty<string>(),
            string.Join('\n', new[]
            {
                "{\"ts\":1000,\"data\":{\"category\":\"a\"}}",
                "{\"ts\":2000,\"data\":{\"category\":\"a\"}}",
                "{\"ts\":3000,\"data\":{\"category\":\"b\"}}",
                "{\"ts\":6000,\"data\":{\"category\":\"b\"}}",
                "{\"ts\":7000,\"data\":{\"category\":\"b\"}}"
            }) + "\n",
            new[]
            {
                "{\"windowStart\":0,\"windowEnd\":5000,\"category\":\"a\",\"count\":2}",
                "{\"windowStart\":5000,\"windowEnd\":10000,\"category\":\"b\",\"count\":2}"
            },
            "Validate HAVING filters windowed aggregates.");

        yield return new EndToEndCase(
            "L1 Tumbling window with WHERE (millisecond)",
            "SELECT COUNT(*) AS count FROM input TIMESTAMP BY ts WHERE data.value > 2 GROUP BY TUMBLINGWINDOW(millisecond, 500)",
            Array.Empty<string>(),
            string.Join('\n', new[]
            {
                "{\"ts\":100,\"data\":{\"value\":1}}",
                "{\"ts\":200,\"data\":{\"value\":3}}",
                "{\"ts\":700,\"data\":{\"value\":4}}"
            }) + "\n",
            new[]
            {
                "{\"windowStart\":0,\"windowEnd\":500,\"count\":1}",
                "{\"windowStart\":500,\"windowEnd\":1000,\"count\":1}"
            },
            "Validate millisecond tumbling windows with WHERE filters.");

        yield return new EndToEndCase(
            "L1 Tumbling window aggregates (minute)",
            "SELECT AVG(data.value) AS avg, MIN(data.value) AS minValue, MAX(data.value) AS maxValue FROM input TIMESTAMP BY ts GROUP BY TUMBLINGWINDOW(minute, 1)",
            Array.Empty<string>(),
            string.Join('\n', new[]
            {
                "{\"ts\":1000,\"data\":{\"value\":1}}",
                "{\"ts\":2000,\"data\":{\"value\":5}}",
                "{\"ts\":3000,\"data\":{\"value\":3}}"
            }) + "\n",
            new[]
            {
                "{\"windowStart\":0,\"windowEnd\":60000,\"avg\":3,\"minValue\":1,\"maxValue\":5}"
            },
            "Validate minute tumbling windows with AVG, MIN, and MAX.");

        yield return new EndToEndCase(
            "L1 Hopping window out-of-order timestamps (millisecond)",
            "SELECT COUNT(*) AS count FROM input TIMESTAMP BY ts GROUP BY HOPPINGWINDOW(millisecond, 1000, 500)",
            Array.Empty<string>(),
            string.Join('\n', new[]
            {
                "{\"ts\":1200}",
                "{\"ts\":200}",
                "{\"ts\":700}"
            }) + "\n",
            new[]
            {
                "{\"windowStart\":0,\"windowEnd\":1000,\"count\":2}",
                "{\"windowStart\":500,\"windowEnd\":1500,\"count\":2}",
                "{\"windowStart\":1000,\"windowEnd\":2000,\"count\":1}"
            },
            "Validate hopping windows with out-of-order events.");

        yield return new EndToEndCase(
            "L1 Hopping window single event (minute)",
            "SELECT COUNT(*) AS count FROM input TIMESTAMP BY ts GROUP BY HOPPINGWINDOW(minute, 2, 1)",
            Array.Empty<string>(),
            "{\"ts\":65000}\n",
            new[]
            {
                "{\"windowStart\":0,\"windowEnd\":120000,\"count\":1}",
                "{\"windowStart\":60000,\"windowEnd\":180000,\"count\":1}"
            },
            "Validate hopping windows with a single event.");

        yield return new EndToEndCase(
            "L1 Sliding window empty input (millisecond)",
            "SELECT COUNT(*) AS count FROM input TIMESTAMP BY ts GROUP BY SLIDINGWINDOW(millisecond, 500)",
            Array.Empty<string>(),
            string.Empty,
            Array.Empty<string>(),
            "Validate sliding windows handle empty input.");

        yield return new EndToEndCase(
            "L1 Sliding window mixed timestamps (minute)",
            "SELECT COUNT(*) AS count FROM input TIMESTAMP BY ts GROUP BY SLIDINGWINDOW(minute, 1)",
            Array.Empty<string>(),
            string.Join('\n', new[]
            {
                "{\"ts\":65000}",
                "{\"ts\":\"1970-01-01T00:01:10Z\"}"
            }) + "\n",
            new[]
            {
                "{\"windowStart\":5000,\"windowEnd\":65000,\"count\":1}",
                "{\"windowStart\":10000,\"windowEnd\":70000,\"count\":2}"
            },
            "Validate sliding windows with mixed timestamp formats.");
    }

    private static async Task<string[]> ExecuteAsync(EndToEndCase testCase)
    {
        var args = new[] { "--query", testCase.Query }.Concat(testCase.Args).ToArray();

        var inputPath = Path.GetTempFileName();
        var outputPath = Path.GetTempFileName();

        try
        {
            await File.WriteAllTextAsync(inputPath, testCase.Input);
            await File.WriteAllTextAsync(outputPath, string.Empty);

            var fullArgs = args.Concat(new[] { "--file", inputPath, "--out", outputPath }).ToArray();
            var previousError = Console.Error;
            var errorWriter = new StringWriter();
            Console.SetError(errorWriter);
            var exitCode = await Program.Main(fullArgs);
            Console.SetError(previousError);
            if (exitCode != 0)
            {
                throw new InvalidOperationException(errorWriter.ToString().Trim());
            }

            var output = await File.ReadAllTextAsync(outputPath);
            return output.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
        }
        finally
        {
            File.Delete(inputPath);
            File.Delete(outputPath);
            StreamReaderFactory.InputOverride = null;
            StreamReaderFactory.OutputOverride = null;
        }
    }
}

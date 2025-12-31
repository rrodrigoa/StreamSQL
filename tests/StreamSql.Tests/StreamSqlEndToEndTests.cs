using System.Text;
using System.Text.Json;
using StreamSql;
using StreamSql.Input;
using Xunit;

namespace StreamSql.Tests;

public class StreamSqlEndToEndTests
{
    [Theory]
    [MemberData(nameof(EndToEndCases))]
    public async Task ExecutesEndToEndCases(EndToEndCase testCase)
    {
        if (testCase.ExpectError)
        {
            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => ExecuteAsync(testCase));
            if (!string.IsNullOrWhiteSpace(testCase.ErrorContains))
            {
                Assert.Contains(testCase.ErrorContains, exception.Message);
            }
            return;
        }

        var lines = await ExecuteAsync(testCase);
        Assert.Equal(testCase.ExpectedOutput, lines);

        if (testCase.ValidateJson)
        {
            foreach (var line in lines)
            {
                using var doc = JsonDocument.Parse(line);
                Assert.True(doc.RootElement.ValueKind is JsonValueKind.Object or JsonValueKind.Array);
            }
        }
    }

    public static IEnumerable<object[]> EndToEndCases()
    {
        foreach (var testCase in BuildCases())
        {
            yield return new object[] { testCase };
        }
    }

    private static IEnumerable<EndToEndCase> BuildCases()
    {
        yield return new EndToEndCase(
            "A1 Single field projection",
            "SELECT data.value FROM input",
            Array.Empty<string>(),
            "{\"data\":{\"value\":1}}\n{\"data\":{\"value\":2}}\n",
            new[] { "{\"value\":1}", "{\"value\":2}" },
            "Project a single JSON field.");

        yield return new EndToEndCase(
            "A2 Projection with alias",
            "SELECT data.value AS v FROM input",
            Array.Empty<string>(),
            "{\"data\":{\"value\":3}}\n",
            new[] { "{\"v\":3}" },
            "Alias projected field.");

        yield return new EndToEndCase(
            "A3 Multiple fields projection",
            "SELECT data.value, data.other FROM input",
            Array.Empty<string>(),
            "{\"data\":{\"value\":1,\"other\":2}}\n",
            new[] { "{\"value\":1,\"other\":2}" },
            "Project multiple fields.");

        yield return new EndToEndCase(
            "A4 Multiple fields with aliases",
            "SELECT data.value AS v, data.other AS o FROM input",
            Array.Empty<string>(),
            "{\"data\":{\"value\":1,\"other\":2}}\n",
            new[] { "{\"v\":1,\"o\":2}" },
            "Alias multiple projections.");

        yield return new EndToEndCase(
            "A5 Mixed alias and non-alias",
            "SELECT data.value, data.other AS o FROM input",
            Array.Empty<string>(),
            "{\"data\":{\"value\":4,\"other\":5}}\n",
            new[] { "{\"value\":4,\"o\":5}" },
            "Mix aliased and default output names.");

        yield return new EndToEndCase(
            "A6 Select root object",
            "SELECT data FROM input",
            Array.Empty<string>(),
            "{\"data\":{\"value\":6,\"other\":7}}\n",
            new[] { "{\"data\":{\"value\":6,\"other\":7}}" },
            "Select the root object.");

        yield return new EndToEndCase(
            "A7 Projection with WHERE",
            "SELECT data.value FROM input WHERE data.value > 10",
            Array.Empty<string>(),
            "{\"data\":{\"value\":9}}\n{\"data\":{\"value\":11}}\n",
            new[] { "{\"value\":11}" },
            "Filter projection with WHERE.");

        yield return new EndToEndCase(
            "A8 Projection with multiple WHERE conditions",
            "SELECT data.value FROM input WHERE data.value > 10 AND data.other < 5",
            Array.Empty<string>(),
            "{\"data\":{\"value\":11,\"other\":6}}\n{\"data\":{\"value\":12,\"other\":4}}\n",
            new[] { "{\"value\":12}" },
            "Filter with multiple AND conditions.");

        yield return new EndToEndCase(
            "A9 Boolean field projection",
            "SELECT data.flag FROM input WHERE data.flag = 1",
            Array.Empty<string>(),
            "{\"data\":{\"flag\":0}}\n{\"data\":{\"flag\":1}}\n",
            new[] { "{\"flag\":1}" },
            "Filter on boolean field.");

        yield return new EndToEndCase(
            "B11 COUNT()",
            "SELECT COUNT() FROM input",
            Array.Empty<string>(),
            "{\"data\":{\"value\":1}}\n{\"data\":{\"value\":2}}\n{\"data\":{\"value\":3}}\n",
            new[] { "{\"count\":3}" },
            "Count all rows.");

        yield return new EndToEndCase(
            "B12 COUNT(field)",
            "SELECT COUNT(data.value) FROM input",
            Array.Empty<string>(),
            "{\"data\":{\"value\":1}}\n{\"data\":{\"value\":2}}\n",
            new[] { "{\"count\":2}" },
            "Count non-null field occurrences.");

        yield return new EndToEndCase(
            "B13 AVG(field)",
            "SELECT AVG(data.value) FROM input",
            Array.Empty<string>(),
            "{\"data\":{\"value\":2}}\n{\"data\":{\"value\":4}}\n",
            new[] { "{\"avg\":3}" },
            "Average numeric values.");

        yield return new EndToEndCase(
            "B14 MIN(field)",
            "SELECT MIN(data.value) FROM input",
            Array.Empty<string>(),
            "{\"data\":{\"value\":4}}\n{\"data\":{\"value\":2}}\n{\"data\":{\"value\":9}}\n",
            new[] { "{\"min\":2}" },
            "Minimum numeric value.");

        yield return new EndToEndCase(
            "B15 MAX(field)",
            "SELECT MAX(data.value) FROM input",
            Array.Empty<string>(),
            "{\"data\":{\"value\":4}}\n{\"data\":{\"value\":2}}\n{\"data\":{\"value\":9}}\n",
            new[] { "{\"max\":9}" },
            "Maximum numeric value.");

        yield return new EndToEndCase(
            "B16 SUM(field)",
            "SELECT SUM(data.value) FROM input",
            Array.Empty<string>(),
            "{\"data\":{\"value\":1}}\n{\"data\":{\"value\":2}}\n{\"data\":{\"value\":3}}\n",
            new[] { "{\"sum\":6}" },
            "Sum numeric values.");

        yield return new EndToEndCase(
            "B17 Multiple aggregates",
            "SELECT COUNT(*), AVG(data.value), MAX(data.value) FROM input",
            Array.Empty<string>(),
            "{\"data\":{\"value\":1}}\n{\"data\":{\"value\":2}}\n{\"data\":{\"value\":3}}\n",
            new[] { "{\"count\":3,\"avg\":2,\"max\":3}" },
            "Multiple aggregate outputs.");

        yield return new EndToEndCase(
            "B18 Aggregates with aliases",
            "SELECT COUNT(*) AS c, AVG(data.value) AS avg FROM input",
            Array.Empty<string>(),
            "{\"data\":{\"value\":1}}\n{\"data\":{\"value\":3}}\n{\"data\":{\"value\":5}}\n",
            new[] { "{\"c\":3,\"avg\":3}" },
            "Aggregates with custom aliases.");

        yield return new EndToEndCase(
            "B19 Aggregate with WHERE",
            "SELECT COUNT(*) FROM input WHERE data.value > 5",
            Array.Empty<string>(),
            "{\"data\":{\"value\":3}}\n{\"data\":{\"value\":6}}\n{\"data\":{\"value\":7}}\n",
            new[] { "{\"count\":2}" },
            "Aggregate after filtering.");

        yield return new EndToEndCase(
            "B20 Aggregate over empty input",
            "SELECT COUNT(*) FROM input",
            Array.Empty<string>(),
            string.Empty,
            new[] { "{\"count\":0}" },
            "Aggregate even when no input is present.");

        yield return new EndToEndCase(
            "B21 Aggregate with null values",
            "SELECT AVG(data.value) FROM input",
            Array.Empty<string>(),
            "{\"data\":{\"value\":null}}\n{\"data\":{\"value\":4}}\n",
            new[] { "{\"avg\":4}" },
            "Ignore nulls in AVG.");

        yield return new EndToEndCase(
            "B22 Mixed numeric types",
            "SELECT AVG(data.value) FROM input",
            Array.Empty<string>(),
            "{\"data\":{\"value\":1}}\n{\"data\":{\"value\":2.5}}\n",
            new[] { "{\"avg\":1.75}" },
            "Average integers and decimals.");

        yield return new EndToEndCase(
            "B23 GROUP BY single field",
            "SELECT data.category, COUNT(*) FROM input GROUP BY data.category",
            Array.Empty<string>(),
            "{\"data\":{\"category\":\"a\"}}\n{\"data\":{\"category\":\"b\"}}\n{\"data\":{\"category\":\"a\"}}\n",
            new[] { "{\"category\":\"a\",\"count\":2}", "{\"category\":\"b\",\"count\":1}" },
            "Group by category.");

        yield return new EndToEndCase(
            "B24 GROUP BY multiple fields",
            "SELECT data.a, data.b, COUNT(*) FROM input GROUP BY data.a, data.b",
            Array.Empty<string>(),
            "{\"data\":{\"a\":1,\"b\":1}}\n{\"data\":{\"a\":1,\"b\":2}}\n{\"data\":{\"a\":2,\"b\":1}}\n{\"data\":{\"a\":1,\"b\":1}}\n",
            new[] { "{\"a\":1,\"b\":1,\"count\":2}", "{\"a\":1,\"b\":2,\"count\":1}", "{\"a\":2,\"b\":1,\"count\":1}" },
            "Group by composite key.");

        yield return new EndToEndCase(
            "B25 GROUP BY with aliases",
            "SELECT data.category AS c, COUNT(*) AS n FROM input GROUP BY data.category",
            Array.Empty<string>(),
            "{\"data\":{\"category\":\"a\"}}\n{\"data\":{\"category\":\"a\"}}\n",
            new[] { "{\"c\":\"a\",\"n\":2}" },
            "Group by with aliased output.");

        yield return new EndToEndCase(
            "C26 Tumbling window COUNT",
            "SELECT COUNT(*) FROM input",
            new[] { "--window", "tumbling:5s", "--timestamp-by", "ts" },
            "{\"ts\":1000}\n{\"ts\":2000}\n{\"ts\":6000}\n",
            new[] { "{\"windowStart\":0,\"windowEnd\":5000,\"count\":2}", "{\"windowStart\":5000,\"windowEnd\":10000,\"count\":1}" },
            "Tumbling window count.");

        yield return new EndToEndCase(
            "C27 Tumbling window AVG",
            "SELECT AVG(data.value) FROM input",
            new[] { "--window", "tumbling:5s", "--timestamp-by", "ts" },
            "{\"ts\":1000,\"data\":{\"value\":2}}\n{\"ts\":2000,\"data\":{\"value\":4}}\n{\"ts\":6000,\"data\":{\"value\":10}}\n",
            new[] { "{\"windowStart\":0,\"windowEnd\":5000,\"avg\":3}", "{\"windowStart\":5000,\"windowEnd\":10000,\"avg\":10}" },
            "Tumbling window average.");

        yield return new EndToEndCase(
            "C28 Rolling window",
            "SELECT COUNT(*) FROM input",
            new[] { "--window", "rolling:10s,5s", "--timestamp-by", "ts" },
            "{\"ts\":1000}\n{\"ts\":6000}\n{\"ts\":12000}\n",
            new[] { "{\"windowStart\":0,\"windowEnd\":10000,\"count\":2}", "{\"windowStart\":5000,\"windowEnd\":15000,\"count\":2}", "{\"windowStart\":10000,\"windowEnd\":20000,\"count\":1}" },
            "Rolling window count.");

        yield return new EndToEndCase(
            "C29 Window with WHERE",
            "SELECT COUNT(*) FROM input WHERE data.value > 5",
            new[] { "--window", "tumbling:5s", "--timestamp-by", "ts" },
            "{\"ts\":1000,\"data\":{\"value\":3}}\n{\"ts\":2000,\"data\":{\"value\":6}}\n{\"ts\":3000,\"data\":{\"value\":7}}\n",
            new[] { "{\"windowStart\":0,\"windowEnd\":5000,\"count\":2}" },
            "Windowed filter.");

        yield return new EndToEndCase(
            "C30 Window with GROUP BY",
            "SELECT data.category, COUNT(*) FROM input GROUP BY data.category",
            new[] { "--window", "tumbling:5s", "--timestamp-by", "ts" },
            "{\"ts\":1000,\"data\":{\"category\":\"a\"}}\n{\"ts\":2000,\"data\":{\"category\":\"b\"}}\n{\"ts\":4000,\"data\":{\"category\":\"a\"}}\n",
            new[] { "{\"windowStart\":0,\"windowEnd\":5000,\"category\":\"a\",\"count\":2}", "{\"windowStart\":0,\"windowEnd\":5000,\"category\":\"b\",\"count\":1}" },
            "Windowed group by.");

        yield return new EndToEndCase(
            "C31 Window with no events",
            "SELECT COUNT(*) FROM input",
            new[] { "--window", "tumbling:5s", "--timestamp-by", "ts" },
            string.Empty,
            Array.Empty<string>(),
            "No window output when no events.");

        yield return new EndToEndCase(
            "C32 Window with exactly one event",
            "SELECT COUNT(*) FROM input",
            new[] { "--window", "tumbling:5s", "--timestamp-by", "ts" },
            "{\"ts\":1000}\n",
            new[] { "{\"windowStart\":0,\"windowEnd\":5000,\"count\":1}" },
            "Single event tumbling window.");

        yield return new EndToEndCase(
            "C33 Window flush on EOF",
            "SELECT COUNT(*) FROM input",
            new[] { "--window", "tumbling:5s", "--timestamp-by", "ts" },
            "{\"ts\":9000}\n{\"ts\":11000}\n",
            new[] { "{\"windowStart\":5000,\"windowEnd\":10000,\"count\":1}", "{\"windowStart\":10000,\"windowEnd\":15000,\"count\":1}" },
            "Flush window on EOF.");

        yield return new EndToEndCase(
            "C34 Unordered input",
            "SELECT COUNT(*) FROM input",
            new[] { "--window", "tumbling:5s", "--timestamp-by", "ts" },
            "{\"ts\":6000}\n{\"ts\":1000}\n{\"ts\":2000}\n",
            new[] { "{\"windowStart\":0,\"windowEnd\":5000,\"count\":2}", "{\"windowStart\":5000,\"windowEnd\":10000,\"count\":1}" },
            "Arrival order does not impact window results.");

        yield return new EndToEndCase(
            "C35 Window with mixed timestamps",
            "SELECT COUNT(*) FROM input",
            new[] { "--window", "tumbling:5s", "--timestamp-by", "ts" },
            "{\"ts\":0}\n{\"ts\":\"1970-01-01T00:00:04Z\"}\n",
            new[] { "{\"windowStart\":0,\"windowEnd\":5000,\"count\":2}" },
            "Handles numeric and ISO timestamps.");

        yield return new EndToEndCase(
            "D36 stdin to stdout basic execution",
            "SELECT data.value FROM input",
            Array.Empty<string>(),
            "{\"data\":{\"value\":1}}\n{\"data\":{\"value\":2}}\n",
            new[] { "{\"value\":1}", "{\"value\":2}" },
            "Read from stdin and write to stdout.",
            UseStdin: true,
            UseQueryFile: true);

        yield return new EndToEndCase(
            "D37 stdin with WHERE filter",
            "SELECT data.value FROM input WHERE data.value > 1",
            Array.Empty<string>(),
            "{\"data\":{\"value\":1}}\n{\"data\":{\"value\":2}}\n",
            new[] { "{\"value\":2}" },
            "Filter on stdin stream.",
            UseStdin: true);

        yield return new EndToEndCase(
            "D38 stdin with aggregation",
            "SELECT COUNT(*) FROM input",
            Array.Empty<string>(),
            "{\"data\":{\"value\":1}}\n{\"data\":{\"value\":2}}\n",
            new[] { "{\"count\":2}" },
            "Aggregate stdin stream.",
            UseStdin: true);

        yield return new EndToEndCase(
            "D39 stdin with window",
            "SELECT COUNT(*) FROM input",
            new[] { "--window", "tumbling:5s", "--timestamp-by", "ts" },
            "{\"ts\":1000}\n{\"ts\":2000}\n",
            new[] { "{\"windowStart\":0,\"windowEnd\":5000,\"count\":2}" },
            "Windowed stdin stream.",
            UseStdin: true);

        yield return new EndToEndCase(
            "D40 stdout JSON Lines format",
            "SELECT data.value FROM input",
            Array.Empty<string>(),
            "{\"data\":{\"value\":1}}\n",
            new[] { "{\"value\":1}" },
            "Output must be valid JSON lines.",
            UseStdin: true,
            ValidateJson: true);

        yield return new EndToEndCase(
            "D41 stdout single-row aggregate",
            "SELECT COUNT(*) FROM input",
            Array.Empty<string>(),
            "{\"data\":{\"value\":1}}\n{\"data\":{\"value\":2}}\n",
            new[] { "{\"count\":2}" },
            "Aggregate outputs single line.",
            UseStdin: true);

        yield return new EndToEndCase(
            "D42 empty stdin",
            "SELECT data.value FROM input",
            Array.Empty<string>(),
            string.Empty,
            Array.Empty<string>(),
            "Empty stdin yields no output.",
            UseStdin: true);

        yield return new EndToEndCase(
            "D43 malformed JSON line",
            "SELECT data.value FROM input",
            Array.Empty<string>(),
            "{\"data\":{\"value\":1}}\n{invalid}\n",
            Array.Empty<string>(),
            "Malformed JSON should throw.",
            UseStdin: true,
            ExpectError: true,
            ErrorContains: "Invalid JSON input");

        yield return new EndToEndCase(
            "D44 large stdin",
            "SELECT COUNT(*) FROM input",
            Array.Empty<string>(),
            string.Join('\n', Enumerable.Repeat("{\"data\":{\"value\":1}}", 1000)) + "\n",
            new[] { "{\"count\":1000}" },
            "Performance sanity on large input.",
            UseStdin: true);

        yield return new EndToEndCase(
            "D45 stdin with aliases",
            "SELECT data.value AS v FROM input",
            Array.Empty<string>(),
            "{\"data\":{\"value\":9}}\n",
            new[] { "{\"v\":9}" },
            "Alias output with stdin.",
            UseStdin: true);

        yield return new EndToEndCase(
            "E46 Invalid SQL syntax",
            "SELECT FROM input",
            Array.Empty<string>(),
            "{\"data\":{\"value\":1}}\n",
            Array.Empty<string>(),
            "Invalid SQL should error.",
            ExpectError: true,
            ErrorContains: "SQL parse error");

        yield return new EndToEndCase(
            "E48 Missing field",
            "SELECT data.missing FROM input",
            Array.Empty<string>(),
            "{\"data\":{\"value\":1}}\n",
            new[] { "{}" },
            "Missing fields yield empty objects.");

        yield return new EndToEndCase(
            "E49 Invalid aggregation usage",
            "SELECT data.value, COUNT(*) FROM input",
            Array.Empty<string>(),
            "{\"data\":{\"value\":1}}\n",
            Array.Empty<string>(),
            "Non-grouped fields with aggregates should error.",
            ExpectError: true,
            ErrorContains: "Aggregate queries must GROUP BY all non-aggregated fields.");

        yield return new EndToEndCase(
            "E50 GROUP BY mismatch",
            "SELECT data.a, COUNT(*) FROM input GROUP BY data.b",
            Array.Empty<string>(),
            "{\"data\":{\"a\":1,\"b\":2}}\n",
            Array.Empty<string>(),
            "Group by mismatch should error.",
            ExpectError: true,
            ErrorContains: "GROUP BY mismatch");
    }

    private static async Task<string[]> ExecuteAsync(EndToEndCase testCase)
    {
        string? queryFile = null;
        string[] args;
        if (testCase.UseQueryFile)
        {
            queryFile = Path.GetTempFileName();
            await File.WriteAllTextAsync(queryFile, testCase.Query);
            args = testCase.Args.Concat(new[] { queryFile }).ToArray();
        }
        else
        {
            args = new[] { "--query", testCase.Query }.Concat(testCase.Args).ToArray();
        }

        if (testCase.UseStdin)
        {
            await using var inputStream = new MemoryStream(Encoding.UTF8.GetBytes(testCase.Input));
            await using var outputStream = new MemoryStream();
            StreamReaderFactory.InputOverride = inputStream;
            StreamReaderFactory.OutputOverride = outputStream;

            try
            {
                var exitCode = await Program.Main(args);
                Assert.Equal(0, exitCode);

                outputStream.Position = 0;
                using var reader = new StreamReader(outputStream, Encoding.UTF8, leaveOpen: true);
                var output = await reader.ReadToEndAsync();
                return output.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
            }
            finally
            {
                StreamReaderFactory.InputOverride = null;
                StreamReaderFactory.OutputOverride = null;
                if (queryFile is not null)
                {
                    File.Delete(queryFile);
                }
            }
        }

        var inputPath = Path.GetTempFileName();
        var outputPath = Path.GetTempFileName();

        try
        {
            await File.WriteAllTextAsync(inputPath, testCase.Input);
            await File.WriteAllTextAsync(outputPath, string.Empty);

            var fullArgs = args.Concat(new[] { "--file", inputPath, "--out", outputPath }).ToArray();
            var exitCode = await Program.Main(fullArgs);
            Assert.Equal(0, exitCode);

            var output = await File.ReadAllTextAsync(outputPath);
            return output.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
        }
        finally
        {
            File.Delete(inputPath);
            File.Delete(outputPath);
            if (queryFile is not null)
            {
                File.Delete(queryFile);
            }
        }
    }
}

public sealed record EndToEndCase(
    string Name,
    string Query,
    string[] Args,
    string Input,
    string[] ExpectedOutput,
    string Notes,
    bool ExpectError = false,
    string? ErrorContains = null,
    bool UseStdin = false,
    bool UseQueryFile = false,
    bool ValidateJson = false);

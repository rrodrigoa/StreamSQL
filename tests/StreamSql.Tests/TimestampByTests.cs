using System.Linq;
using System.Text.Json;
using ChronosQL.Engine;
using ChronosQL.Engine.Sql;
using Xunit;

namespace StreamSql.Tests;

public class TimestampByTests
{
    [Fact]
    public async Task UsesNumericTimestampBy()
    {
        var engine = new ChronosQLEngine();
        var plan = engine.Parse("SELECT COUNT(*) FROM input TIMESTAMP BY ts GROUP BY TUMBLINGWINDOW(second, 5)");

        var events = new[]
        {
            BuildEvent("{\"ts\":1000}", 10),
            BuildEvent("{\"ts\":6000}", 20)
        };

        var results = await ExecuteAsync(engine, plan, events);

        Assert.Equal(
            new[] { "{\"windowStart\":0,\"windowEnd\":5000,\"count\":1}", "{\"windowStart\":5000,\"windowEnd\":10000,\"count\":1}" },
            results);
    }

    [Fact]
    public async Task UsesIsoTimestampBy()
    {
        var engine = new ChronosQLEngine();
        var plan = engine.Parse("SELECT COUNT(*) FROM input TIMESTAMP BY ts GROUP BY TUMBLINGWINDOW(second, 5)");

        var events = new[]
        {
            BuildEvent("{\"ts\":\"1970-01-01T00:00:02Z\"}", 10),
            BuildEvent("{\"ts\":\"1970-01-01T00:00:04Z\"}", 20)
        };

        var results = await ExecuteAsync(engine, plan, events);

        Assert.Equal(
            new[] { "{\"windowStart\":0,\"windowEnd\":5000,\"count\":2}" },
            results);
    }

    [Fact]
    public async Task DefaultsToArrivalTimeWhenTimestampByMissing()
    {
        var engine = new ChronosQLEngine();
        var plan = engine.Parse("SELECT COUNT(*) FROM input GROUP BY TUMBLINGWINDOW(second, 5)");

        var events = new[]
        {
            BuildEvent("{\"ts\":1000}", 1000),
            BuildEvent("{\"ts\":6000}", 6000)
        };

        var results = await ExecuteAsync(engine, plan, events);

        Assert.Equal(
            new[] { "{\"windowStart\":0,\"windowEnd\":5000,\"count\":1}", "{\"windowStart\":5000,\"windowEnd\":10000,\"count\":1}" },
            results);
    }

    [Fact]
    public async Task ThrowsForInvalidTimestampBy()
    {
        var engine = new ChronosQLEngine();
        var plan = engine.Parse("SELECT COUNT(*) FROM input TIMESTAMP BY ts GROUP BY TUMBLINGWINDOW(second, 5)");

        var events = new[]
        {
            BuildEvent("{\"ts\":\"not-a-timestamp\"}", 10)
        };

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => ExecuteAsync(engine, plan, events));
        Assert.Contains("TIMESTAMP BY", exception.Message);
    }

    private static async Task<string[]> ExecuteAsync(ChronosQLEngine engine, SqlPlan plan, IEnumerable<InputEvent> events)
    {
        var results = await engine.ExecuteBatchAsync(plan, events);
        return results.Select(e => JsonSerializer.Serialize(e)).ToArray();
    }

    private static InputEvent BuildEvent(string json, long arrivalTime)
    {
        using var document = JsonDocument.Parse(json);
        return new InputEvent(document.RootElement.Clone(), arrivalTime);
    }
}

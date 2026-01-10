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
        var plan = engine.Parse("SELECT data.value INTO output FROM input TIMESTAMP BY ts");

        var events = new[]
        {
            BuildEvent("{\"ts\":1000,\"data\":{\"value\":1}}", 10),
            BuildEvent("{\"ts\":2000,\"data\":{\"value\":2}}", 20)
        };

        var results = await ExecuteAsync(engine, plan, events);

        Assert.Equal(
            new[] { "{\"value\":1}", "{\"value\":2}" },
            results);
    }

    [Fact]
    public async Task UsesIsoTimestampBy()
    {
        var engine = new ChronosQLEngine();
        var plan = engine.Parse("SELECT data.value INTO output FROM input TIMESTAMP BY ts");

        var events = new[]
        {
            BuildEvent("{\"ts\":\"1970-01-01T00:00:02Z\",\"data\":{\"value\":3}}", 10),
            BuildEvent("{\"ts\":\"1970-01-01T00:00:03Z\",\"data\":{\"value\":4}}", 20)
        };

        var results = await ExecuteAsync(engine, plan, events);

        Assert.Equal(
            new[] { "{\"value\":3}", "{\"value\":4}" },
            results);
    }

    [Fact]
    public async Task ThrowsForInvalidTimestampBy()
    {
        var engine = new ChronosQLEngine();
        var plan = engine.Parse("SELECT data.value INTO output FROM input TIMESTAMP BY ts");

        var events = new[]
        {
            BuildEvent("{\"ts\":\"not-a-timestamp\",\"data\":{\"value\":1}}", 10)
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

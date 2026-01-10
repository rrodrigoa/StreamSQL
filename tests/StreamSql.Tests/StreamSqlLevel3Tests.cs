using System.Linq;
using System.Text.Json;
using ChronosQL.Engine;
using ChronosQL.Engine.Sql;
using Xunit;

namespace StreamSql.Tests;

public class StreamSqlLevel3Tests
{
    [Fact]
    public async Task WritesNullWhenFieldMissing()
    {
        var engine = new ChronosQLEngine();
        var plan = engine.Parse("SELECT data.value INTO output FROM input");

        var events = new[]
        {
            BuildEvent("{\"data\":{\"other\":1}}", 10)
        };

        var results = await engine.ExecuteBatchAsync(plan, events);
        var lines = results.Select(result => JsonSerializer.Serialize(result)).ToArray();

        Assert.Equal(new[] { "{\"value\":null}" }, lines);
    }

    private static InputEvent BuildEvent(string json, long arrivalTime)
    {
        using var document = JsonDocument.Parse(json);
        return new InputEvent(document.RootElement.Clone(), arrivalTime);
    }
}

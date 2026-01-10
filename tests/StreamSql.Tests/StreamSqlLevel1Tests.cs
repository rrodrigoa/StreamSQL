using System.Linq;
using System.Text.Json;
using ChronosQL.Engine;
using ChronosQL.Engine.Sql;
using Xunit;

namespace StreamSql.Tests;

public class StreamSqlLevel1Tests
{
    [Fact]
    public async Task ProjectsMultipleFields()
    {
        var engine = new ChronosQLEngine();
        var plan = engine.Parse("SELECT data.category, data.region INTO output FROM input");

        var events = new[]
        {
            BuildEvent("{\"data\":{\"category\":\"a\",\"region\":\"east\"}}", 10),
            BuildEvent("{\"data\":{\"category\":\"b\",\"region\":\"west\"}}", 20)
        };

        var results = await engine.ExecuteBatchAsync(plan, events);
        var lines = results.Select(result => JsonSerializer.Serialize(result)).ToArray();

        Assert.Equal(
            new[] { "{\"category\":\"a\",\"region\":\"east\"}", "{\"category\":\"b\",\"region\":\"west\"}" },
            lines);
    }

    private static InputEvent BuildEvent(string json, long arrivalTime)
    {
        using var document = JsonDocument.Parse(json);
        return new InputEvent(document.RootElement.Clone(), arrivalTime);
    }
}

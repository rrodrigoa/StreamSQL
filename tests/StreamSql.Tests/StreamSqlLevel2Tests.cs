using System.Linq;
using System.Text.Json;
using ChronosQL.Engine;
using ChronosQL.Engine.Sql;
using Xunit;

namespace StreamSql.Tests;

public class StreamSqlLevel2Tests
{
    [Fact]
    public async Task ProjectsAliasFields()
    {
        var engine = new ChronosQLEngine();
        var plan = engine.Parse("SELECT data.value AS v INTO output FROM input");

        var events = new[]
        {
            BuildEvent("{\"data\":{\"value\":5}}", 10),
            BuildEvent("{\"data\":{\"value\":6}}", 20)
        };

        var results = await engine.ExecuteBatchAsync(plan, events);
        var lines = results.Select(result => JsonSerializer.Serialize(result)).ToArray();

        Assert.Equal(new[] { "{\"v\":5}", "{\"v\":6}" }, lines);
    }

    private static InputEvent BuildEvent(string json, long arrivalTime)
    {
        using var document = JsonDocument.Parse(json);
        return new InputEvent(document.RootElement.Clone(), arrivalTime);
    }
}

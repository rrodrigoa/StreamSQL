using System.Text;
using ChronosQL.Engine;
using StreamSql.Input;
using Xunit;

namespace StreamSql.Tests;

public class JsonLineReaderTests
{
    [Fact]
    public async Task ReadsJsonLinesFromStream()
    {
        var payload = "{\"value\":1}\n{\"value\":2}\n";
        await using var stream = new MemoryStream(Encoding.UTF8.GetBytes(payload));
        var reader = new JsonLineReader(stream, InputReadMode.Normal);

        var results = new List<InputEvent>();
        await foreach (var element in reader.ReadAllAsync())
        {
            results.Add(element);
        }

        Assert.Equal(2, results.Count);
        Assert.Equal(1, results[0].Payload.GetProperty("value").GetInt32());
        Assert.Equal(2, results[1].Payload.GetProperty("value").GetInt32());
    }
}

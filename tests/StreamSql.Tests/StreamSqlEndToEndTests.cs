using System.Reactive.Linq;
using System.Text;
using System.Text.Json;
using Microsoft.StreamProcessing;
using StreamSql;
using StreamSql.Input;
using Xunit;

namespace StreamSql.Tests;

public class StreamSqlEndToEndTests
{
    [Fact]
    public async Task ExecutesQueryWithWhereAndTimestampByUsingDefaultStreams()
    {
        var sql = "SELECT value INTO output FROM input where data.value > 5";
        var payload = "{\"timestamp\":1,\"value\":4}\n{\"timestamp\":2,\"value\":6}";

        await using var inputStream = new MemoryStream(Encoding.UTF8.GetBytes(payload));
        await using var outputStream = new MemoryStream();
        StreamReaderFactory.InputOverride = inputStream;
        StreamReaderFactory.OutputOverride = outputStream;

        try
        {
            var exitCode = await Program.Main(new[] { "--query", sql, "--timestamp-by", "timestamp" });

            Assert.Equal(0, exitCode);

            outputStream.Position = 0;
            using var reader = new StreamReader(outputStream, Encoding.UTF8, leaveOpen: true);
            var output = await reader.ReadToEndAsync();
            var lines = output.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);

            Assert.Single(lines);

            using var document = JsonDocument.Parse(lines[0]);
            Assert.Equal(6, document.RootElement.GetProperty("value").GetInt32());
        }
        finally
        {
            StreamReaderFactory.InputOverride = null;
            StreamReaderFactory.OutputOverride = null;
        }
    }

    [Fact]
    public async Task DirectTest()
    {
        var inputData = new StreamEvent<int>[]
        {
            StreamEvent.CreatePoint<int>(1, 4),
            StreamEvent.CreatePoint<int>(2, 6),
            StreamEvent.CreatePoint<int>(3, 7),
            StreamEvent.CreatePunctuation<int>(4)
        };
        var z = inputData.ToObservable().ToStreamable();

        var output = z
            .Where(payload => payload > 5)
            .Select(payload => payload).ToStreamEventObservable().ToEnumerable().ToArray();

        Console.WriteLine(output.Length);
    }
}

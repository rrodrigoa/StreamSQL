using System.Text;
using System.Text.Json;

namespace StreamSql.Output;

public sealed class JsonLineWriter
{
    private readonly Stream _stream;

    public JsonLineWriter(Stream stream)
    {
        _stream = stream;
    }

    public async Task WriteAllAsync(IAsyncEnumerable<JsonElement> events, CancellationToken cancellationToken = default)
    {
        await using var writer = new StreamWriter(_stream, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false), leaveOpen: true)
        {
            AutoFlush = true
        };

        await foreach (var element in events.WithCancellation(cancellationToken))
        {
            var json = JsonSerializer.Serialize(element);
            await writer.WriteLineAsync(json.AsMemory(), cancellationToken);
        }

        await writer.FlushAsync(cancellationToken);
    }
}

using System.Text.Json;
using ChronosQL.Engine;

namespace StreamSql.Input;

public sealed class JsonLineReader
{
    private readonly Stream _stream;
    private readonly bool _follow;

    public JsonLineReader(Stream stream, bool follow)
    {
        _stream = stream;
        _follow = follow;
    }

    public async IAsyncEnumerable<InputEvent> ReadAllAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using var reader = new StreamReader(_stream, leaveOpen: true);

        while (true)
        {
            var line = await reader.ReadLineAsync(cancellationToken);
            if (line is null)
            {
                if (_follow)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(250), cancellationToken);
                    continue;
                }

                yield break;
            }

            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }

            // Capture arrival time at read so downstream timestamp fallback uses the actual ingestion time.
            var arrivalTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            using var document = JsonDocument.Parse(line);
            yield return new InputEvent(document.RootElement.Clone(), arrivalTime);
        }
    }
}

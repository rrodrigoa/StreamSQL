using System.Text.Json;
using System.Threading.Channels;

namespace ChronosQL.Engine;

public sealed class StreamingQuery : IAsyncDisposable
{
    private readonly Channel<InputEvent> _channel;
    private readonly TrillPipelineBuilder _pipeline;
    private readonly ChannelReader<InputEvent> _reader;

    internal StreamingQuery(TrillPipelineBuilder pipeline)
    {
        _pipeline = pipeline;
        _channel = Channel.CreateUnbounded<InputEvent>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false
        });
        _reader = _channel.Reader;
        Results = _pipeline.ExecuteAsync(_reader.ReadAllAsync());
    }

    public IAsyncEnumerable<JsonElement> Results { get; }

    public async ValueTask EnqueueAsync(JsonElement payload, long? arrivalTime = null, CancellationToken cancellationToken = default)
    {
        var timestamp = arrivalTime ?? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        await _channel.Writer.WriteAsync(new InputEvent(payload, timestamp), cancellationToken);
    }

    public void Complete() => _channel.Writer.TryComplete();

    public async IAsyncEnumerable<string> ResultsAsJson(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var element in Results.WithCancellation(cancellationToken))
        {
            yield return JsonSerializer.Serialize(element);
        }
    }

    public ValueTask DisposeAsync()
    {
        _channel.Writer.TryComplete();
        return ValueTask.CompletedTask;
    }
}

using System.Text.Json;
using System.Threading.Channels;
using ChronosQL.Engine.Compilation;

namespace ChronosQL.Engine;

public sealed class StreamingQuery : IAsyncDisposable
{
    private readonly Channel<InputEvent> _input;
    private readonly Channel<JsonElement> _output;
    private readonly CompiledQuery _compiledQuery;

    internal StreamingQuery(CompiledQuery compiledQuery)
    {
        _compiledQuery = compiledQuery;
        _input = Channel.CreateUnbounded<InputEvent>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false
        });
        _output = Channel.CreateUnbounded<JsonElement>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false
        });

        Results = _output.Reader.ReadAllAsync();
        _ = _compiledQuery.ExecuteAsync(_input.Reader.ReadAllAsync(), _output.Writer);
    }

    public IAsyncEnumerable<JsonElement> Results { get; }

    public async ValueTask EnqueueAsync(JsonElement payload, long? arrivalTime = null, CancellationToken cancellationToken = default)
    {
        var timestamp = arrivalTime ?? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        await _input.Writer.WriteAsync(new InputEvent(payload, timestamp), cancellationToken);
    }

    public void Complete() => _input.Writer.TryComplete();

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
        _input.Writer.TryComplete();
        _output.Writer.TryComplete();
        return ValueTask.CompletedTask;
    }
}

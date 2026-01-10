using System.Text.Json;
using System.Threading.Channels;

namespace ChronosQL.Engine.Compilation;

public sealed class CompiledQuery
{
    private readonly Func<IAsyncEnumerable<InputEvent>, ChannelWriter<JsonElement>, CancellationToken, Task> _runner;

    public CompiledQuery(
        Func<IAsyncEnumerable<InputEvent>, ChannelWriter<JsonElement>, CancellationToken, Task> runner,
        string source)
    {
        _runner = runner;
        Source = source;
    }

    public string Source { get; }

    public Task ExecuteAsync(IAsyncEnumerable<InputEvent> input, ChannelWriter<JsonElement> output, CancellationToken cancellationToken = default)
        => _runner(input, output, cancellationToken);
}

using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Channels;

namespace ChronosQL.Engine.Compilation;

public sealed class CompiledScriptQuery
{
    private readonly Func<IReadOnlyDictionary<string, IAsyncEnumerable<InputEvent>>, IReadOnlyDictionary<string, ChannelWriter<JsonElement>>, CancellationToken, Task> _runner;

    public CompiledScriptQuery(
        Func<IReadOnlyDictionary<string, IAsyncEnumerable<InputEvent>>, IReadOnlyDictionary<string, ChannelWriter<JsonElement>>, CancellationToken, Task> runner,
        string source)
    {
        _runner = runner;
        Source = source;
    }

    public string Source { get; }

    public Task ExecuteAsync(
        IReadOnlyDictionary<string, IAsyncEnumerable<InputEvent>> inputs,
        IReadOnlyDictionary<string, ChannelWriter<JsonElement>> outputs,
        CancellationToken cancellationToken = default)
        => _runner(inputs, outputs, cancellationToken);
}

using System.Text.Json;

namespace ChronosQL.Engine.Compilation;

public sealed class CompiledQuery
{
    private readonly Func<IAsyncEnumerable<InputEvent>, CancellationToken, IAsyncEnumerable<JsonElement>> _runner;

    public CompiledQuery(
        Func<IAsyncEnumerable<InputEvent>, CancellationToken, IAsyncEnumerable<JsonElement>> runner,
        string source)
    {
        _runner = runner;
        Source = source;
    }

    public string Source { get; }

    public IAsyncEnumerable<JsonElement> ExecuteAsync(IAsyncEnumerable<InputEvent> input, CancellationToken cancellationToken = default)
        => _runner(input, cancellationToken);
}

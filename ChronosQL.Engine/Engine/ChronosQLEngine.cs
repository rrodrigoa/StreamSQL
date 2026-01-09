using System.Text.Json;
using ChronosQL.Engine.Compilation;
using ChronosQL.Engine.Sql;

namespace ChronosQL.Engine;

public sealed class ChronosQLEngine
{
    private readonly QueryCompiler _compiler;

    public ChronosQLEngine(EngineExecutionOptions? options = null)
    {
        Options = options ?? new EngineExecutionOptions();
        _compiler = new QueryCompiler();
    }

    public EngineExecutionOptions Options { get; }

    public SqlPlan Parse(string sql) => SqlParser.Parse(sql);

    public SqlScriptPlan ParseScript(string sql) => SqlParser.ParseScript(sql);

    public IAsyncEnumerable<JsonElement> ExecuteAsync(
        string sql,
        IAsyncEnumerable<InputEvent> input,
        CancellationToken cancellationToken = default)
    {
        var plan = SqlParser.Parse(sql);
        return ExecuteAsync(plan, input, cancellationToken);
    }

    public IAsyncEnumerable<JsonElement> ExecuteAsync(
        SqlPlan plan,
        IAsyncEnumerable<InputEvent> input,
        CancellationToken cancellationToken = default)
    {
        var compiled = _compiler.Compile(plan, Options.Follow);
        return compiled.ExecuteAsync(input, cancellationToken);
    }

    public async Task<IReadOnlyList<JsonElement>> ExecuteBatchAsync(
        string sql,
        IEnumerable<JsonElement> input,
        CancellationToken cancellationToken = default)
    {
        var plan = SqlParser.Parse(sql);
        return await ExecuteBatchAsync(plan, input, cancellationToken);
    }

    public async Task<IReadOnlyList<JsonElement>> ExecuteBatchAsync(
        SqlPlan plan,
        IEnumerable<JsonElement> input,
        CancellationToken cancellationToken = default)
    {
        var events = input.Select(element => new InputEvent(element, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()));
        return await ExecuteBatchAsync(plan, events, cancellationToken);
    }

    public async Task<IReadOnlyList<JsonElement>> ExecuteBatchAsync(
        string sql,
        IEnumerable<string> jsonLines,
        CancellationToken cancellationToken = default)
    {
        var plan = SqlParser.Parse(sql);
        return await ExecuteBatchAsync(plan, jsonLines, cancellationToken);
    }

    public async Task<IReadOnlyList<JsonElement>> ExecuteBatchAsync(
        SqlPlan plan,
        IEnumerable<string> jsonLines,
        CancellationToken cancellationToken = default)
    {
        var events = jsonLines
            .Where(line => !string.IsNullOrWhiteSpace(line))
            .Select(line =>
            {
                using var document = JsonDocument.Parse(line);
                return new InputEvent(document.RootElement.Clone(), DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
            })
            .ToList();

        return await ExecuteBatchAsync(plan, events, cancellationToken);
    }

    public async Task<IReadOnlyList<JsonElement>> ExecuteBatchAsync(
        SqlPlan plan,
        IEnumerable<InputEvent> input,
        CancellationToken cancellationToken = default)
    {
        var results = new List<JsonElement>();
        await foreach (var result in ExecuteAsync(plan, ToAsyncEnumerable(input), cancellationToken)
            .WithCancellation(cancellationToken))
        {
            results.Add(result);
        }

        return results;
    }

    public async Task<IReadOnlyList<string>> ExecuteBatchAsJsonAsync(
        string sql,
        IEnumerable<JsonElement> input,
        CancellationToken cancellationToken = default)
    {
        var results = await ExecuteBatchAsync(sql, input, cancellationToken);
        return results.Select(e => JsonSerializer.Serialize(e)).ToList();
    }

    public async Task<IReadOnlyList<string>> ExecuteBatchAsJsonAsync(
        string sql,
        IEnumerable<string> jsonLines,
        CancellationToken cancellationToken = default)
    {
        var results = await ExecuteBatchAsync(sql, jsonLines, cancellationToken);
        return results.Select(e => JsonSerializer.Serialize(e)).ToList();
    }

    public StreamingQuery CreateStreamingQuery(string sql)
    {
        var plan = SqlParser.Parse(sql);
        return CreateStreamingQuery(plan);
    }

    public StreamingQuery CreateStreamingQuery(SqlPlan plan)
    {
        var compiled = _compiler.Compile(plan, Options.Follow);
        return new StreamingQuery(compiled);
    }

    public CompiledQuery Compile(SqlPlan plan)
        => _compiler.Compile(plan, Options.Follow);

    public StreamingQuery CreateStreamingQuery(CompiledQuery compiledQuery)
        => new StreamingQuery(compiledQuery);

    private static async IAsyncEnumerable<InputEvent> ToAsyncEnumerable(
        IEnumerable<InputEvent> input,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (var item in input)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return item;
            await Task.Yield();
        }
    }
}

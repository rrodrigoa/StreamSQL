using System.Text.Json;
using ChronosQL.Engine;

namespace StreamSql.Input;

public sealed class JsonLineReader
{
    private readonly Stream _stream;
    private readonly bool _follow;
    private readonly StreamReader _reader;
    private readonly string? _filePath;
    private readonly FileStream? _fileStream;

    public JsonLineReader(Stream stream, bool follow, string? filePath = null)
    {
        _stream = stream;
        _follow = follow;
        _filePath = filePath;
        _fileStream = stream as FileStream;
        _reader = new StreamReader(_stream, leaveOpen: true);
    }

    public async IAsyncEnumerable<InputEvent> ReadAllAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        while (true)
        {
            var line = await _reader.ReadLineAsync(cancellationToken);
            if (line is null)
            {
                if (_follow && IsFileInput)
                {
                    await WaitForMoreFileDataAsync(cancellationToken);
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

    public async Task<BatchReadResult> ReadAvailableBatchAsync(CancellationToken cancellationToken = default)
    {
        var events = new List<InputEvent>();

        while (true)
        {
            string? line;
            try
            {
                line = await _reader.ReadLineAsync(cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return new BatchReadResult(events, IsCompleted: true);
            }

            if (line is null)
            {
                if (_follow && IsFileInput)
                {
                    await WaitForMoreFileDataAsync(cancellationToken);
                    continue;
                }

                return new BatchReadResult(events, IsCompleted: true);
            }

            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }

            events.Add(ParseLine(line));
            break;
        }

        while (HasBufferedFileData())
        {
            var nextLine = await _reader.ReadLineAsync(cancellationToken);
            if (nextLine is null)
            {
                if (_follow && IsFileInput)
                {
                    await WaitForMoreFileDataAsync(cancellationToken);
                    continue;
                }

                break;
            }

            if (string.IsNullOrWhiteSpace(nextLine))
            {
                continue;
            }

            events.Add(ParseLine(nextLine));
        }

        return new BatchReadResult(events, IsCompleted: false);
    }

    private bool IsFileInput => _fileStream is not null && !string.IsNullOrWhiteSpace(ResolvedFilePath);

    private string? ResolvedFilePath => _filePath ?? _fileStream?.Name;

    private bool HasBufferedFileData()
    {
        if (!IsFileInput || _fileStream is null)
        {
            return false;
        }

        if (_fileStream.CanSeek && _fileStream.Position < _fileStream.Length)
        {
            return true;
        }

        return _reader.Peek() >= 0;
    }

    private async Task WaitForMoreFileDataAsync(CancellationToken cancellationToken)
    {
        var filePath = ResolvedFilePath;
        if (string.IsNullOrWhiteSpace(filePath))
        {
            return;
        }

        var directory = Path.GetDirectoryName(filePath);
        var fileName = Path.GetFileName(filePath);
        if (string.IsNullOrWhiteSpace(directory) || string.IsNullOrWhiteSpace(fileName))
        {
            return;
        }

        var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var watcher = new FileSystemWatcher(directory, fileName)
        {
            NotifyFilter = NotifyFilters.Size | NotifyFilters.LastWrite
        };

        void OnChange(object? sender, FileSystemEventArgs args) => completion.TrySetResult();
        void OnRename(object? sender, RenamedEventArgs args) => completion.TrySetResult();

        watcher.Changed += OnChange;
        watcher.Created += OnChange;
        watcher.Renamed += OnRename;
        watcher.EnableRaisingEvents = true;

        using var registration = cancellationToken.Register(() => completion.TrySetCanceled(cancellationToken));
        await completion.Task;
    }

    private static InputEvent ParseLine(string line)
    {
        var arrivalTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        using var document = JsonDocument.Parse(line);
        return new InputEvent(document.RootElement.Clone(), arrivalTime);
    }
}

public readonly record struct BatchReadResult(IReadOnlyList<InputEvent> Events, bool IsCompleted);

using StreamSql.Cli;

namespace StreamSql.Input;

public static class StreamReaderFactory
{
    public static Stream? InputOverride { get; set; }
    public static Stream? OutputOverride { get; set; }

    public static Stream OpenInput(InputSource source)
    {
        if (source.Kind == InputSourceKind.Stdin)
        {
            if (InputOverride is not null)
            {
                return new NonDisposingStream(InputOverride);
            }

            return new NonDisposingStream(Console.OpenStandardInput());
        }

        if (string.IsNullOrWhiteSpace(source.Path))
        {
            throw new InvalidOperationException("Input file path cannot be empty.");
        }

        return new FileStream(source.Path, new FileStreamOptions
        {
            Access = FileAccess.Read,
            Mode = FileMode.Open,
            Share = FileShare.ReadWrite,
            Options = FileOptions.Asynchronous | FileOptions.SequentialScan
        });
    }

    public static Stream OpenOutput(OutputDestination destination)
    {
        if (destination.Kind == OutputDestinationKind.Stdout)
        {
            if (OutputOverride is not null)
            {
                return new NonDisposingStream(OutputOverride);
            }

            return new NonDisposingStream(Console.OpenStandardOutput());
        }

        if (string.IsNullOrWhiteSpace(destination.Path))
        {
            throw new InvalidOperationException("Output file path cannot be empty.");
        }

        return new FileStream(destination.Path, new FileStreamOptions
        {
            Access = FileAccess.Write,
            Mode = FileMode.Create,
            Share = FileShare.Read,
            Options = FileOptions.Asynchronous | FileOptions.SequentialScan
        });
    }

    private sealed class NonDisposingStream : Stream
    {
        private readonly Stream _inner;

        public NonDisposingStream(Stream inner)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        }

        public override bool CanRead => _inner.CanRead;
        public override bool CanSeek => _inner.CanSeek;
        public override bool CanWrite => _inner.CanWrite;
        public override long Length => _inner.Length;
        public override long Position
        {
            get => _inner.Position;
            set => _inner.Position = value;
        }

        public override void Flush() => _inner.Flush();

        public override Task FlushAsync(CancellationToken cancellationToken) =>
            _inner.FlushAsync(cancellationToken);

        public override int Read(byte[] buffer, int offset, int count) =>
            _inner.Read(buffer, offset, count);

        public override int Read(Span<byte> buffer) => _inner.Read(buffer);

        public override ValueTask<int> ReadAsync(
            Memory<byte> buffer,
            CancellationToken cancellationToken = default) =>
            _inner.ReadAsync(buffer, cancellationToken);

        public override Task<int> ReadAsync(
            byte[] buffer,
            int offset,
            int count,
            CancellationToken cancellationToken) =>
            _inner.ReadAsync(buffer, offset, count, cancellationToken);

        public override long Seek(long offset, SeekOrigin origin) =>
            _inner.Seek(offset, origin);

        public override void SetLength(long value) => _inner.SetLength(value);

        public override void Write(byte[] buffer, int offset, int count) =>
            _inner.Write(buffer, offset, count);

        public override void Write(ReadOnlySpan<byte> buffer) =>
            _inner.Write(buffer);

        public override ValueTask WriteAsync(
            ReadOnlyMemory<byte> buffer,
            CancellationToken cancellationToken = default) =>
            _inner.WriteAsync(buffer, cancellationToken);

        public override Task WriteAsync(
            byte[] buffer,
            int offset,
            int count,
            CancellationToken cancellationToken) =>
            _inner.WriteAsync(buffer, offset, count, cancellationToken);

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _inner.Flush();
            }
        }

        public override async ValueTask DisposeAsync()
        {
            await _inner.FlushAsync().ConfigureAwait(false);
        }
    }
}

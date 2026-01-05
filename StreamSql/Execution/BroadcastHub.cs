using System.Threading.Channels;

namespace StreamSql.Execution;

public sealed class BroadcastHub<T>
{
    private readonly List<Channel<T>> _channels = new();
    private readonly int _capacity;
    private bool _completed;

    public BroadcastHub(int capacity)
    {
        _capacity = capacity;
    }

    public ChannelReader<T> AddConsumer()
    {
        var channel = Channel.CreateBounded<T>(new BoundedChannelOptions(_capacity)
        {
            SingleReader = true,
            SingleWriter = true,
            FullMode = BoundedChannelFullMode.Wait
        });
        _channels.Add(channel);
        return channel.Reader;
    }

    public async Task BroadcastAsync(T value, CancellationToken cancellationToken)
    {
        foreach (var channel in _channels)
        {
            await channel.Writer.WriteAsync(value, cancellationToken);
        }
    }

    public void Complete(Exception? error = null)
    {
        if (_completed)
        {
            return;
        }

        _completed = true;
        foreach (var channel in _channels)
        {
            channel.Writer.TryComplete(error);
        }
    }
}

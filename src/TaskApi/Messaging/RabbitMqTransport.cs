using System.Text.Json;
using System.Collections.Concurrent;
using System.Net.Sockets;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace TaskApi.Messaging;

// Owned by RabbitMqService's single worker; never used concurrently.
public sealed class RabbitMqTransport : IRabbitMqTransport
{
    public const string Exchange = "task.events";
    public const string RoutingKey = "task.completed";
    private IConnection? _connection;
    private IChannel? _channel;
    private readonly IOptions<RabbitMqOptions> _options;
    private readonly Func<ConnectionFactory, CancellationToken, Task<IConnection>> _connect;
    private readonly ConcurrentBag<ITcpClient> _sockets = [];

    public RabbitMqTransport(IOptions<RabbitMqOptions> options)
        : this(options, (factory, ct) => factory.CreateConnectionAsync(ct)) { }

    internal RabbitMqTransport(IOptions<RabbitMqOptions> options,
        Func<ConnectionFactory, CancellationToken, Task<IConnection>> connect)
    { _options = options; _connect = connect; }

    public async Task PublishAsync(TaskCompletedEvent message, CancellationToken cancellationToken)
    {
        using var deadline = cancellationToken.Register(ForceCloseSockets);
        if (_connection?.IsOpen != true)
        {
            await CloseAsync(cancellationToken);
            var settings = _options.Value;
            var factory = new ConnectionFactory
            {
                HostName = settings.HostName, Port = settings.Port, UserName = settings.UserName,
                Password = settings.Password, VirtualHost = settings.VirtualHost,
                AutomaticRecoveryEnabled = false, RequestedConnectionTimeout = TimeSpan.FromSeconds(2),
                ContinuationTimeout = TimeSpan.FromSeconds(2), ClientProvidedName = "TaskApi publisher",
                SocketFactory = family =>
                {
                    var socket = ConnectionFactoryBase.DefaultSocketFactory(family);
                    _sockets.Add(socket);
                    if (cancellationToken.IsCancellationRequested) socket.Close();
                    return socket;
                }
            };
            _connection = await _connect(factory, cancellationToken);
        }
        if (_channel?.IsOpen != true)
        {
            if (_channel is not null)
            {
                var previous = _channel;
                _channel = null;
                await previous.DisposeAsync();
            }
            var channel = await _connection.CreateChannelAsync(cancellationToken: cancellationToken);
            try
            {
                await channel.ExchangeDeclareAsync(Exchange, ExchangeType.Topic, durable: true,
                    autoDelete: false, cancellationToken: cancellationToken);
                _channel = channel; // Reusable only once topology initialization succeeds.
            }
            catch
            {
                ForceCloseSockets();
                await channel.DisposeAsync();
                throw;
            }
        }
        var body = JsonSerializer.SerializeToUtf8Bytes(message, TaskCompletedEvent.JsonOptions);
        await _channel.BasicPublishAsync(Exchange, RoutingKey, mandatory: false,
            basicProperties: new BasicProperties { ContentType = "application/json", Persistent = true },
            body: body, cancellationToken: cancellationToken);
    }

    public async Task CloseAsync(CancellationToken cancellationToken)
    {
        // RabbitMQ 7.2 ignores cancellation on an open connection/channel close. Closing owned
        // sockets at the deadline unblocks the AMQP operations; still await their cleanup.
        using var deadline = cancellationToken.Register(ForceCloseSockets);
        var channel = _channel;
        var connection = _connection;
        _channel = null;
        _connection = null;
        try
        {
            if (channel is not null)
            {
                try { if (channel.IsOpen) await channel.CloseAsync(cancellationToken); }
                finally { await channel.DisposeAsync(); }
            }
        }
        finally
        {
            try
            {
                if (connection is not null)
                {
                    try { if (connection.IsOpen) await connection.CloseAsync(cancellationToken); }
                    finally { await connection.DisposeAsync(); }
                }
            }
            finally
            {
                ForceCloseSockets();
                while (_sockets.TryTake(out var socket)) socket.Dispose();
            }
        }
    }

    private void ForceCloseSockets()
    {
        foreach (var socket in _sockets)
        {
            try { socket.Close(); }
            catch (ObjectDisposedException)
            {
                // An already disposed socket needs no further shutdown cleanup.
            }
            catch (SocketException)
            {
                // Socket shutdown is best effort; continue closing the remaining sockets.
            }
        }
    }
}

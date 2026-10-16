using System.Net.Sockets;
using Microsoft.Extensions.Options;
using Moq;
using RabbitMQ.Client;
using TaskApi.Messaging;
using Xunit;

namespace TaskApi.Tests;

public class RabbitMqTransportTests
{
    [Fact]
    public async Task CloseAsync_ForcesSocketClosedWhenClientIgnoresCancellation()
    {
        var connection = Connection(out var channel);
        Socket? socket = null;
        var transport = new RabbitMqTransport(Options.Create(new RabbitMqOptions()), (factory, _) =>
        {
            socket = factory.SocketFactory(AddressFamily.InterNetwork).Client;
            return Task.FromResult(connection.Object);
        });
        await transport.PublishAsync(Message(), default);
        connection.Setup(c => c.CloseAsync(It.IsAny<ushort>(), It.IsAny<string>(), It.IsAny<TimeSpan>(),
                It.IsAny<bool>(), It.IsAny<CancellationToken>()))
            .Returns(async () =>
            {
                // Models RabbitMQ 7.2 ignoring the caller token until the socket closes.
                while (!socket!.SafeHandle.IsClosed) await Task.Delay(5);
            });
        using var deadline = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
        var closing = transport.CloseAsync(deadline.Token);
        try { await closing.WaitAsync(TimeSpan.FromSeconds(1)); }
        finally { socket!.Dispose(); await closing; }
        Assert.True(socket.SafeHandle.IsClosed);
        connection.Verify(c => c.DisposeAsync(), Times.Once);
        channel.Verify(c => c.DisposeAsync(), Times.Once);
    }

    [Fact]
    public async Task FailedExchangeDeclaration_IsRetriedBeforeNextPublish()
    {
        var connection = Connection(out var channel);
        channel.SetupSequence(c => c.ExchangeDeclareAsync(RabbitMqTransport.Exchange, ExchangeType.Topic,
                true, false, null, false, false, It.IsAny<CancellationToken>()))
            .ThrowsAsync(new OperationCanceledException("Declaration timed out before sending"))
            .Returns(Task.CompletedTask);
        var transport = new RabbitMqTransport(Options.Create(new RabbitMqOptions()),
            (_, _) => Task.FromResult(connection.Object));
        await Assert.ThrowsAsync<OperationCanceledException>(() => transport.PublishAsync(Message(), default));
        await transport.PublishAsync(Message(), default);
        channel.Verify(c => c.ExchangeDeclareAsync(RabbitMqTransport.Exchange, ExchangeType.Topic,
            true, false, null, false, false, It.IsAny<CancellationToken>()), Times.Exactly(2));
        channel.Verify(c => c.BasicPublishAsync(RabbitMqTransport.Exchange, RabbitMqTransport.RoutingKey,
            false, It.IsAny<BasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()), Times.Once);
        await transport.CloseAsync(default);
    }

    private static Mock<IConnection> Connection(out Mock<IChannel> channel)
    {
        channel = new Mock<IChannel>();
        channel.SetupGet(c => c.IsOpen).Returns(true);
        var connection = new Mock<IConnection>();
        connection.SetupGet(c => c.IsOpen).Returns(true);
        connection.Setup(c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(channel.Object);
        return connection;
    }

    private static TaskCompletedEvent Message() => new(Guid.NewGuid(), "Buy milk", DateTimeOffset.UtcNow, Priority.High);
}

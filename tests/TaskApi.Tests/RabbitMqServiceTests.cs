using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using TaskApi.Messaging;
using Xunit;

namespace TaskApi.Tests;

public class RabbitMqServiceTests
{
    [Fact]
    public async Task StopAsync_DrainsStartedAndQueuedMessagesBeforeClosing()
    {
        var transport = new ControlledTransport();
        var publisher = Create(transport);
        await publisher.StartAsync(default);
        publisher.Publish(Message());
        await transport.Started.Task.WaitAsync(TimeSpan.FromSeconds(3));
        publisher.Publish(Message());
        var stopping = publisher.StopAsync(default);
        Assert.False(stopping.IsCompleted);
        Assert.False(transport.Closed);
        transport.Release.TrySetResult();
        await stopping;
        Assert.Equal(2, transport.Published);
        Assert.True(transport.Closed);
    }

    [Fact]
    public async Task StopAsync_CancelsStalledPublicationWithinFiveSeconds()
    {
        var transport = new ControlledTransport();
        var publisher = Create(transport);
        await publisher.StartAsync(default);
        publisher.Publish(Message());
        await transport.Started.Task.WaitAsync(TimeSpan.FromSeconds(3));
        var timer = Stopwatch.StartNew();
        await publisher.StopAsync(default);
        Assert.True(transport.Cancelled);
        Assert.True(transport.Closed);
        Assert.True(timer.Elapsed < TimeSpan.FromSeconds(5.5));
    }

    [Fact]
    public async Task FullOrStoppedQueue_LogsUndeliveredEvent()
    {
        var transport = new ControlledTransport();
        var logger = new Mock<ILogger<RabbitMqService>>();
        var publisher = Create(transport, logger.Object, capacity: 1);
        await publisher.StartAsync(default);
        publisher.Publish(Message());
        await transport.Started.Task.WaitAsync(TimeSpan.FromSeconds(3));
        publisher.Publish(Message());
        publisher.Publish(Message());
        transport.Release.TrySetResult();
        await publisher.StopAsync(default);
        publisher.Publish(Message());
        Assert.Equal(2, transport.Published);
        logger.Verify(l => l.Log(LogLevel.Warning, It.IsAny<EventId>(),
            It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("taskId")),
            It.IsAny<Exception?>(), It.IsAny<Func<It.IsAnyType, Exception?, string>>()), Times.Exactly(2));
    }

    [Fact]
    public async Task FailedPublication_LogsPayloadAndContinues()
    {
        var transport = new Mock<IRabbitMqTransport>();
        transport.Setup(t => t.PublishAsync(It.IsAny<TaskCompletedEvent>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new IOException("Broker is unavailable"));
        transport.Setup(t => t.CloseAsync(It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
        var logger = new Mock<ILogger<RabbitMqService>>();
        var publisher = Create(transport.Object, logger.Object);
        await publisher.StartAsync(default);
        publisher.Publish(Message());
        publisher.Publish(Message());
        await publisher.StopAsync(default);
        transport.Verify(t => t.PublishAsync(It.IsAny<TaskCompletedEvent>(), It.IsAny<CancellationToken>()), Times.Exactly(2));
        logger.Verify(l => l.Log(LogLevel.Warning, It.IsAny<EventId>(),
            It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("taskId") && v.ToString()!.Contains("task.completed")),
            It.IsAny<Exception?>(), It.IsAny<Func<It.IsAnyType, Exception?, string>>()), Times.Exactly(2));
    }

    private static RabbitMqService Create(IRabbitMqTransport transport,
        ILogger<RabbitMqService>? logger = null, int capacity = 256) => new(transport,
        Options.Create(new RabbitMqOptions { Capacity = capacity }),
        logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<RabbitMqService>.Instance);

    private static TaskCompletedEvent Message() => new(Guid.NewGuid(), "Buy milk", DateTimeOffset.UtcNow, Priority.High);

    private sealed class ControlledTransport : IRabbitMqTransport
    {
        public TaskCompletionSource Started { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int Published { get; private set; }
        public bool Closed { get; private set; }
        public bool Cancelled { get; private set; }
        public async Task PublishAsync(TaskCompletedEvent message, CancellationToken cancellationToken)
        {
            Started.TrySetResult();
            try { await Release.Task.WaitAsync(cancellationToken); Published++; }
            catch (OperationCanceledException) { Cancelled = true; throw; }
        }
        public Task CloseAsync(CancellationToken cancellationToken) { Closed = true; return Task.CompletedTask; }
    }
}

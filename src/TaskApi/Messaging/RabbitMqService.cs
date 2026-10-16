using Microsoft.Extensions.Options;
using System.Text.Json;
using System.Threading.Channels;

namespace TaskApi.Messaging;

public sealed class RabbitMqService : ITaskEventPublisher, IHostedService, IDisposable
{
    private readonly IRabbitMqTransport _transport;
    private readonly ILogger<RabbitMqService> _logger;
    private readonly Channel<TaskCompletedEvent> _queue;
    private readonly CancellationTokenSource _abort = new();
    private Task? _worker;

    public RabbitMqService(IRabbitMqTransport transport, IOptions<RabbitMqOptions> options,
        ILogger<RabbitMqService> logger)
    {
        _transport = transport;
        _logger = logger;
        _queue = Channel.CreateBounded<TaskCompletedEvent>(new BoundedChannelOptions(options.Value.Capacity)
        { SingleReader = true, FullMode = BoundedChannelFullMode.Wait, AllowSynchronousContinuations = false });
    }

    public void Publish(TaskCompletedEvent message)
    {
        if (!_queue.Writer.TryWrite(message))
            LogUndelivered(message, "Publisher queue is full or closed", null);
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _worker = RunAsync();
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _queue.Writer.TryComplete();
        if (_worker is null) return;
        // Four seconds for draining, one second reserved for closing AMQP resources.
        _abort.CancelAfter(TimeSpan.FromSeconds(4));
        using var registration = cancellationToken.Register(() => _abort.Cancel());
        try { await _worker.WaitAsync(TimeSpan.FromSeconds(5)); }
        catch (TimeoutException ex) { await _abort.CancelAsync(); _logger.LogWarning(ex, "Publisher shutdown deadline exceeded"); }
    }

    private async Task RunAsync()
    {
        try
        {
            await foreach (var message in _queue.Reader.ReadAllAsync(_abort.Token))
            {
                using var attempt = CancellationTokenSource.CreateLinkedTokenSource(_abort.Token);
                attempt.CancelAfter(TimeSpan.FromSeconds(2));
                try { await _transport.PublishAsync(message, attempt.Token); }
                catch (Exception ex) { LogUndelivered(message, "RabbitMQ publication failed", ex); }
                _abort.Token.ThrowIfCancellationRequested();
            }
        }
        catch (OperationCanceledException) when (_abort.IsCancellationRequested)
        {
            // Shutdown cancellation is expected; finally logs pending events and closes the transport.
        }
        finally
        {
            while (_queue.Reader.TryRead(out var message))
                LogUndelivered(message, "Shutdown deadline reached", null);
            using var close = new CancellationTokenSource(TimeSpan.FromSeconds(1));
            try { await _transport.CloseAsync(close.Token); }
            catch (Exception ex) { _logger.LogWarning(ex, "Failed to close RabbitMQ resources within shutdown budget"); }
        }
    }

    private void LogUndelivered(TaskCompletedEvent message, string reason, Exception? exception) =>
        _logger.LogWarning(exception, "{Reason}; exchange=task.events routingKey=task.completed payload={Payload}",
            reason, JsonSerializer.Serialize(message, TaskCompletedEvent.JsonOptions));

    public void Dispose() => _abort.Dispose();
}

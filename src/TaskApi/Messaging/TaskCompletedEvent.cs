using System.Text.Json;
using System.Text.Json.Serialization;

namespace TaskApi.Messaging;

public sealed record TaskCompletedEvent(Guid TaskId, string Title, DateTimeOffset CompletedAt, Priority Priority)
{
    public static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter<Priority>(allowIntegerValues: false) }
    };
}

public interface ITaskEventPublisher
{
    void Publish(TaskCompletedEvent message);
}

public interface IRabbitMqTransport
{
    Task PublishAsync(TaskCompletedEvent message, CancellationToken cancellationToken);
    Task CloseAsync(CancellationToken cancellationToken);
}

public sealed class RabbitMqOptions
{
    public string HostName { get; set; } = "localhost";
    public int Port { get; set; } = 5672;
    public string UserName { get; set; } = "taskapi";
    public string Password { get; set; } = "taskapi";
    public string VirtualHost { get; set; } = "/";
    public int Capacity { get; set; } = 256;
}

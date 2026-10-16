using System.Net;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;
using TaskApi.Messaging;
using Microsoft.Extensions.Options;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net.Sockets;
using Xunit;

namespace TaskApi.Tests;

public sealed class CompleteTaskIntegrationTest(InfrastructureFixture infrastructure)
    : IClassFixture<InfrastructureFixture>
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    { Converters = { new JsonStringEnumConverter<Priority>() } };

    [Fact]
    public async Task CreateComplete_PublishesSavedEventThroughInterface()
    {
        await using var app = new ApiFactory(infrastructure);
        using var client = app.CreateClient();
        var created = await CreateAsync(client, "Buy milk", "High");
        Assert.False(created.IsCompleted);
        Assert.Null(created.CompletedAt);
        Assert.Empty(app.Events.Messages);
        var response = await client.PutAsync($"/tasks/{created.Id}/complete", null);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var completed = await ReadAsync(response);
        Assert.True(completed.IsCompleted);
        Assert.NotNull(completed.CompletedAt);
        Assert.True(completed.CompletedAt >= created.CreatedAt);
        Assert.Equal(TimeSpan.Zero, completed.CompletedAt.Value.Offset);
        var message = Assert.Single(app.Events.Messages);
        Assert.Equal(created.Id, message.TaskId);
        Assert.Equal(created.Title, message.Title);
        Assert.Equal(completed.CompletedAt, message.CompletedAt);
        Assert.Equal(Priority.High, message.Priority);
        using var scope = app.Services.CreateScope();
        var saved = await scope.ServiceProvider.GetRequiredService<TaskDbContext>().Tasks.FindAsync(created.Id);
        Assert.True(saved!.IsCompleted);
        Assert.Equal(message.CompletedAt, saved.CompletedAt);
    }

    [Fact]
    public async Task CreateComplete_RealRabbitMqReceivesExpectedJson()
    {
        await using var connection = await infrastructure.ConnectRabbitAsync();
        await using var channel = await connection.CreateChannelAsync();
        await channel.ExchangeDeclareAsync(RabbitMqTransport.Exchange, ExchangeType.Topic, durable: true);
        var queue = await channel.QueueDeclareAsync();
        await channel.QueueBindAsync(queue.QueueName, RabbitMqTransport.Exchange, RabbitMqTransport.RoutingKey);
        await using var app = new ApiFactory(infrastructure, realPublisher: true);
        using var client = app.CreateClient();
        var created = await CreateAsync(client, "Buy milk", "High");
        var completed = await ReadAsync(await client.PutAsync($"/tasks/{created.Id}/complete", null));
        // Host shutdown must drain the accepted event and close the real AMQP resources.
        await app.DisposeAsync();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        BasicGetResult? delivery;
        while ((delivery = await channel.BasicGetAsync(queue.QueueName, autoAck: true, timeout.Token)) is null)
            await Task.Delay(25, timeout.Token);
        Assert.Equal(RabbitMqTransport.Exchange, delivery.Exchange);
        Assert.Equal(RabbitMqTransport.RoutingKey, delivery.RoutingKey);
        Assert.Equal("application/json", delivery.BasicProperties.ContentType);
        using var json = JsonDocument.Parse(delivery.Body);
        Assert.Equal(4, json.RootElement.EnumerateObject().Count());
        Assert.Equal(created.Id, json.RootElement.GetProperty("taskId").GetGuid());
        Assert.Equal("Buy milk", json.RootElement.GetProperty("title").GetString());
        Assert.Equal(completed.CompletedAt, json.RootElement.GetProperty("completedAt").GetDateTimeOffset());
        Assert.Equal("High", json.RootElement.GetProperty("priority").GetString());
    }

    [Fact]
    public async Task PausedRealBroker_ShutdownDeadlineClosesOwnedSockets()
    {
        var sockets = new ConcurrentBag<Socket>();
        var settings = Options.Create(new RabbitMqOptions
        { HostName = infrastructure.RabbitHost, Port = infrastructure.RabbitPort });
        var transport = new RabbitMqTransport(settings, async (factory, ct) =>
        {
            var original = factory.SocketFactory;
            factory.SocketFactory = family =>
            {
                var socket = original(family);
                sockets.Add(socket.Client);
                return socket;
            };
            return await factory.CreateConnectionAsync(ct);
        });
        using (var connect = new CancellationTokenSource(TimeSpan.FromSeconds(5)))
            await transport.PublishAsync(new(Guid.NewGuid(), "Shutdown test", DateTimeOffset.UtcNow, Priority.Low), connect.Token);
        await infrastructure.PauseRabbitAsync();
        try
        {
            using var deadline = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));
            var timer = Stopwatch.StartNew();
            var error = await Record.ExceptionAsync(() => transport.CloseAsync(deadline.Token).WaitAsync(TimeSpan.FromSeconds(2)));
            Assert.IsNotType<TimeoutException>(error);
            Assert.True(timer.Elapsed < TimeSpan.FromSeconds(2));
            Assert.NotEmpty(sockets);
            Assert.All(sockets, socket => Assert.True(socket.SafeHandle.IsClosed));
        }
        finally
        {
            await infrastructure.ResumeRabbitAsync();
            await transport.CloseAsync(default);
        }
    }

    [Fact]
    public async Task ConcurrentCompletion_Returns200And409AndPublishesExactlyOnce()
    {
        await using var app = new ApiFactory(infrastructure);
        using var client = app.CreateClient();
        var created = await CreateAsync(client);
        app.Barrier.Enabled = true;
        var responses = await Task.WhenAll(client.PutAsync($"/tasks/{created.Id}/complete", null),
            client.PutAsync($"/tasks/{created.Id}/complete", null));
        Assert.Single(responses, r => r.StatusCode == HttpStatusCode.OK);
        Assert.Single(responses, r => r.StatusCode == HttpStatusCode.Conflict);
        Assert.Single(app.Events.Messages);
    }

    [Fact]
    public async Task RepeatCompletion_DoesNotChangeTimestampOrPublishAgain()
    {
        await using var app = new ApiFactory(infrastructure);
        using var client = app.CreateClient();
        var created = await CreateAsync(client);
        var first = await ReadAsync(await client.PutAsync($"/tasks/{created.Id}/complete", null));
        Assert.Equal(HttpStatusCode.Conflict, (await client.PutAsync($"/tasks/{created.Id}/complete", null)).StatusCode);
        var all = await client.GetFromJsonAsync<TaskResponse[]>("/tasks", JsonOptions);
        Assert.Equal(first.CompletedAt, Assert.Single(all!, t => t.Id == created.Id).CompletedAt);
        Assert.Single(app.Events.Messages);
    }

    [Fact]
    public async Task DatabaseFailure_DoesNotPublishOrPersistCompletion()
    {
        await using var app = new ApiFactory(infrastructure);
        using var client = app.CreateClient();
        var created = await CreateAsync(client);
        app.Barrier.FailCompletion = true;
        Assert.Equal(HttpStatusCode.InternalServerError, (await client.PutAsync($"/tasks/{created.Id}/complete", null)).StatusCode);
        Assert.Empty(app.Events.Messages);
        var saved = Assert.Single((await client.GetFromJsonAsync<TaskResponse[]>("/tasks", JsonOptions))!, t => t.Id == created.Id);
        Assert.False(saved.IsCompleted);
        Assert.Null(saved.CompletedAt);
    }

    [Fact]
    public async Task UnavailableRabbitMq_TaskStillCompletes()
    {
        // No external service is stopped: an unused AMQP port simulates an outage.
        await using var app = new ApiFactory(infrastructure, realPublisher: true, rabbitPort: 1);
        using var client = app.CreateClient();
        var created = await CreateAsync(client);
        Assert.Equal(HttpStatusCode.OK, (await client.PutAsync($"/tasks/{created.Id}/complete", null)).StatusCode);
        var saved = Assert.Single((await client.GetFromJsonAsync<TaskResponse[]>("/tasks", JsonOptions))!, t => t.Id == created.Id);
        Assert.True(saved.IsCompleted);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("\t\r\n")]
    public async Task InvalidTitle_Returns400(string? title)
    {
        await using var app = new ApiFactory(infrastructure);
        using var client = app.CreateClient();
        Assert.Equal(HttpStatusCode.BadRequest, (await client.PostAsJsonAsync("/tasks", new { title })).StatusCode);
    }

    [Theory]
    [InlineData(200, HttpStatusCode.Created)]
    [InlineData(201, HttpStatusCode.BadRequest)]
    public async Task TitleLengthBoundary_IsValidated(int length, HttpStatusCode expected)
    {
        await using var app = new ApiFactory(infrastructure);
        using var client = app.CreateClient();
        Assert.Equal(expected, (await client.PostAsJsonAsync("/tasks", new { title = new string('a', length) })).StatusCode);
    }

    [Theory]
    [InlineData("{\"title\":\"x\",\"priority\":\"Urgent\"}")]
    [InlineData("{\"title\":\"x\",\"priority\":123}")]
    [InlineData("{\"title\":\"x\",\"priority\":null}")]
    [InlineData("{}")]
    [InlineData("null")]
    public async Task InvalidCreateBody_Returns400(string body)
    {
        await using var app = new ApiFactory(infrastructure);
        using var client = app.CreateClient();
        var response = await client.PostAsync("/tasks", new StringContent(body, Encoding.UTF8, "application/json"));
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task DefaultValues_ListAndHardDelete_Work()
    {
        await using var app = new ApiFactory(infrastructure);
        using var client = app.CreateClient();
        var created = await CreateAsync(client);
        Assert.Equal(Priority.Medium, created.Priority);
        Assert.False(created.IsCompleted);
        Assert.Null(created.CompletedAt);
        Assert.InRange(created.CreatedAt, DateTimeOffset.UtcNow.AddMinutes(-1), DateTimeOffset.UtcNow);
        Assert.Contains((await client.GetFromJsonAsync<TaskResponse[]>("/tasks", JsonOptions))!, t => t.Id == created.Id);
        Assert.Equal(HttpStatusCode.NoContent, (await client.DeleteAsync($"/tasks/{created.Id}")).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await client.DeleteAsync($"/tasks/{created.Id}")).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await client.PutAsync($"/tasks/{created.Id}/complete", null)).StatusCode);
        using var scope = app.Services.CreateScope();
        Assert.False(await scope.ServiceProvider.GetRequiredService<TaskDbContext>().Tasks.AnyAsync(t => t.Id == created.Id));
    }

    [Theory]
    [InlineData("not-a-guid", HttpStatusCode.BadRequest)]
    [InlineData("00000000-0000-0000-0000-000000000001", HttpStatusCode.NotFound)]
    public async Task InvalidOrMissingId_IsHandled(string id, HttpStatusCode expected)
    {
        await using var app = new ApiFactory(infrastructure);
        using var client = app.CreateClient();
        Assert.Equal(expected, (await client.PutAsync($"/tasks/{id}/complete", null)).StatusCode);
        Assert.Equal(expected, (await client.DeleteAsync($"/tasks/{id}")).StatusCode);
    }

    private static async Task<TaskResponse> CreateAsync(HttpClient client, string title = "Buy milk", string? priority = null)
    {
        object body = priority is null ? new { title } : new { title, priority };
        var response = await client.PostAsJsonAsync("/tasks", body);
        Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        return await ReadAsync(response);
    }

    private static async Task<TaskResponse> ReadAsync(HttpResponseMessage response)
    {
        response.EnsureSuccessStatusCode();
        return (await response.Content.ReadFromJsonAsync<TaskResponse>(JsonOptions))!;
    }
}

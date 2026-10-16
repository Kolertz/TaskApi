using System.Collections.Concurrent;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using RabbitMQ.Client;
using TaskApi.Messaging;
using Testcontainers.PostgreSql;
using Xunit;

namespace TaskApi.Tests;

public sealed class InfrastructureFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder()
        .WithImage("postgres:17-alpine").Build();
    private readonly IContainer _rabbit = new ContainerBuilder().WithImage("rabbitmq:4.1-alpine")
        .WithEnvironment("RABBITMQ_DEFAULT_USER", "taskapi")
        .WithEnvironment("RABBITMQ_DEFAULT_PASS", "taskapi")
        .WithPortBinding(5672, true)
        .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(5672)).Build();
    public string ConnectionString => _postgres.GetConnectionString();
    public string RabbitHost => _rabbit.Hostname;
    public int RabbitPort => _rabbit.GetMappedPublicPort(5672);
    public Task PauseRabbitAsync() => ((DockerContainer)_rabbit).PauseAsync();
    public Task ResumeRabbitAsync() => ((DockerContainer)_rabbit).UnpauseAsync();
    public Task InitializeAsync() => Task.WhenAll(_postgres.StartAsync(), _rabbit.StartAsync());
    public async Task DisposeAsync() { await _rabbit.DisposeAsync(); await _postgres.DisposeAsync(); }
    public Task<IConnection> ConnectRabbitAsync() => new ConnectionFactory
    { HostName = RabbitHost, Port = RabbitPort, UserName = "taskapi", Password = "taskapi" }.CreateConnectionAsync();
}

public sealed class ApiFactory(InfrastructureFixture infrastructure, bool realPublisher = false, int? rabbitPort = null)
    : WebApplicationFactory<Program>
{
    public RecordingPublisher Events { get; } = new();
    public CompletionSaveBarrier Barrier { get; } = new();
    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseEnvironment("Testing");
        builder.ConfigureAppConfiguration((_, config) => config.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["ConnectionStrings:Tasks"] = infrastructure.ConnectionString,
            ["RabbitMQ:HostName"] = infrastructure.RabbitHost,
            ["RabbitMQ:Port"] = (rabbitPort ?? infrastructure.RabbitPort).ToString(),
            ["RabbitMQ:UserName"] = "taskapi", ["RabbitMQ:Password"] = "taskapi"
        }));
        builder.ConfigureServices(services =>
        {
            services.AddDbContext<TaskDbContext>(options => options.AddInterceptors(Barrier));
            if (!realPublisher)
            {
                services.RemoveAll<ITaskEventPublisher>();
                services.AddSingleton<ITaskEventPublisher>(Events);
            }
        });
    }
}

public sealed class RecordingPublisher : ITaskEventPublisher
{
    public ConcurrentQueue<TaskCompletedEvent> Messages { get; } = new();
    public void Publish(TaskCompletedEvent message) => Messages.Enqueue(message);
}

// Both requests reach SaveChanges having read the same xmin, without timing-dependent sleeps.
public sealed class CompletionSaveBarrier : SaveChangesInterceptor
{
    private readonly TaskCompletionSource _both = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _arrivals;
    public bool Enabled { get; set; }
    public bool FailCompletion { get; set; }
    public override async ValueTask<InterceptionResult<int>> SavingChangesAsync(DbContextEventData eventData,
        InterceptionResult<int> result, CancellationToken cancellationToken = default)
    {
        if (eventData.Context!.ChangeTracker.Entries<TaskItem>()
            .Any(e => e.State == EntityState.Modified && e.Entity.IsCompleted))
        {
            if (FailCompletion) throw new DbUpdateException("Simulated database failure");
            if (Enabled)
            {
                if (Interlocked.Increment(ref _arrivals) == 2) _both.TrySetResult();
                await _both.Task.WaitAsync(TimeSpan.FromSeconds(10), cancellationToken);
            }
        }
        return result;
    }
}

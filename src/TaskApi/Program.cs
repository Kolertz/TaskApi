using System.Text.Json.Serialization;
using Microsoft.EntityFrameworkCore;
using TaskApi;
using TaskApi.Messaging;

var builder = WebApplication.CreateBuilder(args);
builder.Services.ConfigureHttpJsonOptions(options =>
    options.SerializerOptions.Converters.Add(new JsonStringEnumConverter<Priority>(allowIntegerValues: false)));
builder.Services.AddProblemDetails();
builder.Services.AddDbContext<TaskDbContext>((services, options) =>
    options.UseNpgsql(services.GetRequiredService<IConfiguration>().GetConnectionString("Tasks")));
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddOptions<RabbitMqOptions>().BindConfiguration("RabbitMQ")
    .Validate(o => o.Capacity > 0, "RabbitMQ:Capacity must be positive.").ValidateOnStart();
builder.Services.AddSingleton<IRabbitMqTransport, RabbitMqTransport>();
builder.Services.AddSingleton<RabbitMqService>();
builder.Services.AddSingleton<ITaskEventPublisher>(sp => sp.GetRequiredService<RabbitMqService>());
builder.Services.AddHostedService(sp => sp.GetRequiredService<RabbitMqService>());

var app = builder.Build();
app.UseExceptionHandler();
await using (var scope = app.Services.CreateAsyncScope())
    await scope.ServiceProvider.GetRequiredService<TaskDbContext>().Database.MigrateAsync();
app.MapTaskEndpoints();
await app.RunAsync();

/// <summary>
/// Exposes the application entry point to integration tests.
/// </summary>
public partial class Program;

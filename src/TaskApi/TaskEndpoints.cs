using Microsoft.EntityFrameworkCore;
using TaskApi.Messaging;

namespace TaskApi;

public sealed record CreateTaskRequest(string? Title, Priority Priority = Priority.Medium);
public sealed record TaskResponse(Guid Id, string Title, bool IsCompleted,
    DateTimeOffset CreatedAt, DateTimeOffset? CompletedAt, Priority Priority)
{
    public static TaskResponse From(TaskItem task) => new(task.Id, task.Title, task.IsCompleted,
        task.CreatedAt, task.CompletedAt, task.Priority);
}

public static class TaskEndpoints
{
    public static void MapTaskEndpoints(this WebApplication app)
    {
        app.MapPost("/tasks", CreateAsync);
        app.MapGet("/tasks", async (TaskDbContext db, CancellationToken ct) =>
            Results.Ok((await db.Tasks.AsNoTracking().OrderBy(t => t.CreatedAt).ThenBy(t => t.Id)
                .ToListAsync(ct)).Select(TaskResponse.From)));
        // No :guid route constraint: malformed values must bind to a 400, not route to a 404.
        app.MapPut("/tasks/{id}/complete", CompleteAsync);
        app.MapDelete("/tasks/{id}", DeleteAsync);
    }

    private static async Task<IResult> CreateAsync(CreateTaskRequest request, TaskDbContext db,
        TimeProvider clock, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(request.Title) || request.Title.Length > 200)
            return Results.ValidationProblem(new Dictionary<string, string[]>
                { ["title"] = ["Title must contain 1–200 characters and cannot be whitespace."] });
        if (!Enum.IsDefined(request.Priority))
            return Results.ValidationProblem(new Dictionary<string, string[]>
                { ["priority"] = ["Priority must be Low, Medium or High."] });

        var task = new TaskItem { Id = Guid.NewGuid(), Title = request.Title,
            Priority = request.Priority, CreatedAt = DatabaseUtcNow(clock) };
        db.Tasks.Add(task);
        await db.SaveChangesAsync(ct);
        return Results.Created($"/tasks/{task.Id}", TaskResponse.From(task));
    }

    private static async Task<IResult> CompleteAsync(Guid id, TaskDbContext db,
        ITaskEventPublisher publisher, TimeProvider clock, CancellationToken ct)
    {
        var task = await db.Tasks.FindAsync([id], ct);
        if (task is null) return Results.NotFound();
        if (task.IsCompleted) return Results.Conflict(new { error = "Task is already completed." });
        task.IsCompleted = true;
        task.CompletedAt = DatabaseUtcNow(clock);
        try { await db.SaveChangesAsync(ct); }
        catch (DbUpdateConcurrencyException)
        {
            return Results.Conflict(new { error = "Task was changed by another request." });
        }

        // The in-memory queue accepts independently of HTTP cancellation, AFTER the commit.
        publisher.Publish(new(task.Id, task.Title, task.CompletedAt.Value, task.Priority));
        return Results.Ok(TaskResponse.From(task));
    }

    private static async Task<IResult> DeleteAsync(Guid id, TaskDbContext db, CancellationToken ct)
    {
        var task = await db.Tasks.FindAsync([id], ct);
        if (task is null) return Results.NotFound();
        db.Tasks.Remove(task);
        try { await db.SaveChangesAsync(ct); }
        catch (DbUpdateConcurrencyException)
        {
            return Results.Conflict(new { error = "Task was changed by another request." });
        }
        return Results.NoContent();
    }

    private static DateTimeOffset DatabaseUtcNow(TimeProvider clock)
    {
        // PostgreSQL stores microseconds, while .NET ticks are 100 ns. Keep API/event/DB identical.
        var ticks = clock.GetUtcNow().UtcTicks;
        return new DateTimeOffset(ticks - ticks % 10, TimeSpan.Zero);
    }
}

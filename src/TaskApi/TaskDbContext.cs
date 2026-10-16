using Microsoft.EntityFrameworkCore;

namespace TaskApi;

public sealed class TaskDbContext(DbContextOptions<TaskDbContext> options) : DbContext(options)
{
    public DbSet<TaskItem> Tasks => Set<TaskItem>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        var task = modelBuilder.Entity<TaskItem>();
        task.ToTable("Tasks", table =>
        {
            table.HasCheckConstraint("CK_Tasks_Title", "char_length(btrim(\"Title\")) > 0");
            table.HasCheckConstraint("CK_Tasks_Priority", "\"Priority\" IN ('Low', 'Medium', 'High')");
            table.HasCheckConstraint("CK_Tasks_Completion", "\"IsCompleted\" = (\"CompletedAt\" IS NOT NULL)");
        });
        task.HasKey(t => t.Id);
        task.Property(t => t.Title).HasMaxLength(200).IsRequired();
        task.Property(t => t.Priority).HasConversion<string>().HasMaxLength(6);
        // PostgreSQL's native xmin changes on every update; no SQL Server byte[] timestamp.
        task.Property(t => t.RowVersion).IsRowVersion();
    }
}

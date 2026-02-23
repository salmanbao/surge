# Surge

**Production-grade multi-tenant distributed job queue for Go**

Surge is a distributed, in-memory job queue designed for multi-tenant Platform and SaaS systems.

It provides first-class namespace isolation, real-time monitoring, and the reliability controls needed to safely scale to millions of jobs.

## Core Capabilities

- **Multi-Tenancy (Namespaces)**: First-class tenant isolation with independent queues, metrics, and DLQs
- **Distributed Processing**: Horizontal scaling with multiple workers
- **Priority Queues**: Fine-grained job prioritization (Critical → Low)
- **Scheduled Jobs**: Delay execution or schedule for specific times
- **Automatic Retries**: Exponential backoff with configurable limits
- **Dead Letter Queue (DLQ)**: Capture and retry failed jobs
- **Job Uniqueness**: Prevent duplicate job execution within time windows
- **Graceful Shutdown**: Clean worker termination with timeout controls
- **Recovery System**: Automatic detection and recovery of stuck jobs
- **Web Dashboard**: Real-time monitoring and management UI

---

## Installation

```bash
go get github.com/olamilekan000/surge
```

---

## Quick Start

### 1. Define Your Job

```go
type SendEmail struct {
    To      string
    Subject string
    Body    string
}
```

### 2. Create Client and Register Handler

```go
package main

import (
    "context"
    "encoding/json"
    "log"

    "github.com/olamilekan000/surge/surge"
    "github.com/olamilekan000/surge/surge/config"
    "github.com/olamilekan000/surge/surge/job"
)

func main() {
    ctx := context.Background()

    cfg := &config.Config{
        RedisOptions: &redis.Options{
            Addr: "localhost:6379",
        },
        MaxWorkers: 50,
    }

    client, err := surge.NewClient(ctx, cfg)
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    client.Handle(SendEmail{}, func(ctx context.Context, job *job.JobEnvelope) error {
        var email SendEmail
        if err := json.Unmarshal(job.Args, &email); err != nil {
            return err
        }

        log.Printf("Sending email to %s: %s", email.To, email.Subject)
        return nil
    })

    if err := client.Consume(ctx); err != nil {
        log.Fatal(err)
    }
}
```

### 3. Enqueue Jobs

```go
err := client.Job(SendEmail{
    To:      "user@example.com",
    Subject: "Welcome",
    Body:    "Thanks for signing up!",
}).Ns("shop_123").Enqueue(ctx)
```

This enqueues the job into the `shop_123` namespace, so all queue keys and metrics for this job are isolated to that tenant while still sharing the same Surge cluster.

### String Topics With Structured Payloads

```go
client.Handle("email.sent", func(ctx context.Context, j *job.JobEnvelope) error {
    var email SendEmail
    return json.Unmarshal(j.Args, &email)
})

client.JobWithTopic("email.sent", SendEmail{
    To:      "user@example.com",
    Subject: "Welcome",
    Body:    "Thanks for signing up!",
}).Enqueue(ctx)
```

**Important:** If you register a handler using a **string topic** (like `"email.sent"`), you **must** use `JobWithTopic()` when enqueuing jobs. This ensures the topic matches your handler registration and allows you to send structured payloads instead of just strings.

---

## Multi-Tenancy

Surge was built around namespace-based isolation from day one. Every job belongs to a namespace, which isolates queue keys, metrics, DLQs, and processing state — all while sharing the same Redis cluster and worker pool.

**Use cases:**

- **Per-tenant isolation**: `shop_123`, `org_456`, `user_789`
- **Environment separation**: `staging`, `production`, `dev`
- **Domain boundaries**: `billing`, `analytics`, `notifications`

**How it works:**

- Queue keys use the configurable Redis prefix (default `surge`): `{prefix}:{namespace}:queue:{queue_name}`. Set `RedisPrefix` in config to run multiple Surge instances on the same Redis without key collisions.
- Each namespace has its own DLQ, processing sets, and stats
- Workers consume from all namespaces automatically (no configuration needed)
- Perfect for SaaS platforms where each customer needs isolated job processing

**Example:**

```go
// Tenant A's jobs
client.Job(SendEmail{}).Ns("shop_123").Enqueue(ctx)

// Tenant B's jobs
client.Job(SendEmail{}).Ns("shop_456").Enqueue(ctx)

// Both use the same Surge client and Redis, but queues are completely isolated
```

---

## Configuration

### Essential Settings

```go
cfg := &config.Config{
    RedisOptions: &redis.Options{
        Addr: "localhost:6379",
        DB:   0,
    },
    MaxWorkers: 50,
}
```

### Production Configuration

```go
cfg := &config.Config{
    RedisOptions: &redis.Options{
        Addr:            "redis.production.com:6379",
        Password:        os.Getenv("REDIS_PASSWORD"),
        PoolSize:        100,
        MaxRetries:      3,
        ConnMaxIdleTime: 5 * time.Minute,
    },

    MaxWorkers:       100,
    MaxRetries:       25,
    DefaultNamespace: "production",

    ShutdownTimeout:  30 * time.Second,
    DefaultJobTimeout: 5 * time.Minute,

    RedisRecoveryInterval: 30 * time.Second,
    RedisRecoveryTimeout:  10 * time.Minute,

    HeartbeatInterval: 5 * time.Second,
    HeartbeatTTL:      30 * time.Second,
}
```

### High Availability (Redis Sentinel)

For HA setups, you can pass a `redis.FailoverOptions` instead of `redis.Options`:

```go
cfg := &config.Config{
    // Use RedisFailover instead of RedisOptions
    RedisFailover: &redis.FailoverOptions{
        MasterName:    "mymaster",
        SentinelAddrs: []string{"sentinel1:26379", "sentinel2:26379"},
        Password:      os.Getenv("REDIS_PASSWORD"),
    },

    MaxWorkers: 50,
}
```

### Configuration Reference

| Parameter               | Default   | Description                                                                                                                                          |
| ----------------------- | --------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| `RedisOptions`          | `nil`     | Standard go-redis options (`*redis.Options`). Mutually exclusive with `RedisFailover`.                                                               |
| `RedisFailover`         | `nil`     | go-redis failover options for Sentinel (`*redis.FailoverOptions`). Mutually exclusive with `RedisOptions`.                                           |
| `RedisPingTimeout`      | `5s`      | Redis ping timeout                                                                                                                                   |
| `RedisPrefix`           | `surge`   | Prefix for all Redis keys (e.g. `surge:namespaces`, `surge:{ns}:queue:{queue}`). Use a custom prefix to share Redis across multiple Surge instances. |
| `MaxWorkers`            | `25`      | Concurrent worker limit                                                                                                                              |
| `MaxRetries`            | `25`      | Max retry attempts per job                                                                                                                           |
| `DefaultNamespace`      | `default` | Default queue namespace                                                                                                                              |
| `ShutdownTimeout`       | `30s`     | Graceful shutdown timeout                                                                                                                            |
| `DefaultJobTimeout`     | `5m`      | Default job execution timeout                                                                                                                        |
| `RedisRecoveryInterval` | `30s`     | Stuck job check frequency                                                                                                                            |
| `RedisRecoveryTimeout`  | `10m`     | Job considered stuck after                                                                                                                           |
| `HeartbeatInterval`     | `5s`      | Worker heartbeat frequency                                                                                                                           |
| `HeartbeatTTL`          | `30s`     | Worker heartbeat TTL                                                                                                                                 |
| `PopTimeout`            | `5s`      | Queue pop timeout                                                                                                                                    |
| `NackTimeout`           | `5s`      | Nack operation timeout                                                                                                                               |

---

## Job Builder API

### Job Options Reference

| Option          | Method                      | Description                                    |
| --------------- | --------------------------- | ---------------------------------------------- |
| **Priority**    | `.Priority(job.Priority)`   | Set job priority (Critical, High, Normal, Low) |
| **Schedule**    | `.Schedule(time.Duration)`  | Delay job execution by a relative duration     |
| **ScheduleAt**  | `.ScheduleAt(time.Time)`    | Schedule job execution at a specific time      |
| **Namespace**   | `.Ns(string)`               | Route job to a specific namespace              |
| **Uniqueness**  | `.UniqueFor(time.Duration)` | Prevent duplicate jobs for a duration          |
| **Max Retries** | `.MaxRetries(int)`          | Override default max retry count               |
| **Metadata**    | `.Metadata(map[string]any)` | Attach arbitrary key-value data to the job     |

### Topics vs Queues: Understanding the Difference

Surge uses two related but distinct concepts for routing and storing jobs:

**Topic** (Logical Routing)

- The **semantic identifier** that determines which handler processes a job
- Used for **routing** jobs to the correct handler function
- Examples: `"email.sent"`, `"order.processed"`, `"SendEmail"` (type name)
- Set via: `Handle("email.sent", handler)` or `JobWithTopic("email.sent", payload)`
- Think of it as: "What kind of work is this?"

**Queue** (Physical Storage)

- The **physical Redis data structure** (sorted set/list) where jobs are stored
- Used for **storage and ordering** of jobs in Redis
- Defaults to the topic name, but can be overridden
- Format: `{prefix}:{namespace}:queue:{queue_name}` (prefix defaults to `surge`, configurable via `RedisPrefix`)
- Think of it as: "Where is this job stored in Redis?"

**Default Behavior:**

```go
// Topic = "SendEmail", Queue = "SendEmail" (defaults to topic)
client.Job(SendEmail{}).Enqueue(ctx)

// Topic = "email.sent", Queue = "email.sent" (defaults to topic)
client.JobWithTopic("email.sent", SendEmail{}).Enqueue(ctx)
```

**Key Insight:**

- Currently, queue always defaults to the topic name (one-to-one mapping)
- One topic maps to one handler
- The queue is an implementation detail; the topic is your application's contract

**When to Use Each:**

- Use **topics** when you want to route jobs to specific handlers (most common)
- The queue name is automatically derived from the topic, so you typically don't need to think about it

### Basic Enqueue

```go
client.Job(SendEmail{
    To:      "user@example.com",
    Subject: "Hello",
}).Enqueue(ctx)
```

### Priority

```go
client.Job(CriticalTask{}).
    Priority(job.PriorityCritical).
    Enqueue(ctx)
```

**Priority Levels:**

- `PriorityCritical` (100)
- `PriorityHigh` (50)
- `PriorityNormal` (0) - default
- `PriorityLow` (-50)

Higher-priority jobs are pulled from the queue before lower-priority ones, allowing you to fast-track critical work under load.

### Scheduled Jobs

```go
client.Job(SendReminder{}).
    Schedule(24 * time.Hour).
    Enqueue(ctx)

client.Job(MonthlyReport{}).
    ScheduleAt(time.Date(2024, 2, 1, 9, 0, 0, 0, time.UTC)).
    Enqueue(ctx)
```

Schedules jobs to run in the future instead of immediately, either after a relative delay (`Schedule`) or at an absolute time (`ScheduleAt`).

### Job Uniqueness

```go
client.Job(ProcessPayment{OrderID: "12345"}).
    UniqueFor(1 * time.Hour).
    Enqueue(ctx)
```

Prevents duplicate jobs with the same payload within the specified window.

### Namespace

```go
client.Job(AnalyticsEvent{}).
    Ns("analytics").
    Enqueue(ctx)
```

Routes the job into a specific namespace, allowing you to isolate tenants, environments, or domains while still using a shared Surge cluster.

### Metadata

```go
client.Job(ProcessImage{}).
    Metadata(map[string]any{
        "source": "upload_api",
        "user_tier": "premium",
        "trace_id": "123-abc",
    }).
    Enqueue(ctx)
```

Attach arbitrary key-value data to a job. This metadata is persisted with the job and available to handlers via `job.Metadata`, useful for tracing, audit logs, or context that doesn't belong in the job payload.

### Max Retries

```go
client.Job(FlakeyAPI{}).
    MaxRetries(10).
    Enqueue(ctx)
```

Overrides the default maximum retry count for this job. After the specified number of failed attempts, the job is moved to the DLQ instead of being retried again.

### Chaining

```go
client.Job(ProcessOrder{ID: "123"}).
    Priority(job.PriorityHigh).
    MaxRetries(5).
    UniqueFor(5 * time.Minute).
    Enqueue(ctx)
```

Builder methods can be chained to compose behavior in a single, readable expression (priority, retries, uniqueness, and more).

---

## Batch Operations

For high-throughput scenarios, use batch enqueuing:

```go
batch := client.Batch()

for i := 0; i < 1000; i++ {
    batch.Job(ProcessRecord{ID: i}).Enqueue(ctx)
}

if err := batch.Commit(ctx); err != nil {
    log.Fatal(err)
}
```

**Performance:** Batch operations use Redis pipelining for 10-100x throughput improvement over individual enqueues.

---

## Error Handling

### Automatic Retries

Failed jobs are automatically retried with exponential backoff:

```
Attempt 1: Immediate
Attempt 2: ~2s delay
Attempt 3: ~4s delay
Attempt 4: ~8s delay
...
Attempt N: ~2^(N-1)s delay (capped at 5 minutes)
```

### Dead Letter Queue

Jobs exceeding `MaxRetries` are moved to the DLQ:

```go
dlqJobs, err := client.Backend().InspectDLQ(ctx, "default", "send_email", 0, 100)
if err != nil {
    log.Fatal(err)
}

for _, job := range dlqJobs {
    log.Printf("Failed job: %s, Error: %s", job.ID, job.LastError)
}

if err := client.Backend().RetryFromDLQ(ctx, job.ID); err != nil {
    log.Fatal(err)
}

count, err := client.Backend().RetryAllDLQ(ctx, "default", "send_email")
log.Printf("Retried %d jobs from DLQ", count)
```

### Error Types

```go
import "github.com/olamilekan000/surge/surge/errors"

if errors.IsQueuePaused(err) {
    log.Println("Queue is paused")
}

if errors.IsUniquenessViolation(err) {
    log.Println("Duplicate job detected")
}

if errors.IsJobNotFound(err) {
    log.Println("Job not found in DLQ")
}
```

---

## Queue Management

### Pause/Resume

```go
if err := client.Pause(ctx, "default", "send_email"); err != nil {
    log.Fatal(err)
}

if err := client.Resume(ctx, "default", "send_email"); err != nil {
    log.Fatal(err)
}
```

### Drain Queue

```go
count, err := client.Drain(ctx, "default", "send_email")
log.Printf("Drained %d pending jobs", count)
```

### Queue Stats

```go
stats, err := client.Backend().QueueStats(ctx, "default", "send_email")
if err != nil {
    log.Fatal(err)
}

log.Printf("Pending: %d, Processing: %d, Processed: %d, Failed: %d",
    stats.Pending, stats.Processing, stats.Processed, stats.Failed)
```

---

## Web Dashboard

### Standalone Server

```go
import "github.com/olamilekan000/surge/surge/server"

dashboard := server.NewDashboardServer(client, 8080)
if err := dashboard.Start(); err != nil {
    log.Fatal(err)
}
```

Access at `http://localhost:8080`

### Embedded in Existing Server

```go
dashboard := server.NewDashboardServer(client, 0)
dashboard.SetRootPath("/admin/queues")

mux := http.NewServeMux()
mux.Handle("/admin/queues/", dashboard.Handler())

http.ListenAndServe(":8080", mux)
```

Access at `http://localhost:8080/admin/queues/`

### Dashboard Features

- Real-time queue metrics
- Job inspection (pending, active, scheduled, failed)
- Pause/resume queues
- Retry failed jobs (individual or bulk)
- Drain queues
- Multi-namespace support
- Live charts and statistics

---

## Production Best Practices

### 1. Graceful Shutdown

```go
import (
    "context"
    "log"
    "os"
    "os/signal"
    "syscall"
    "time"
)

sigChan := make(chan os.Signal, 1)
signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

go func() {
    <-sigChan
    log.Println("Shutting down gracefully...")
    shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    if err := client.Shutdown(shutdownCtx); err != nil {
        log.Printf("Shutdown error: %v", err)
    }
    client.Close()
    os.Exit(0)
}()

client.Consume(ctx)
```

### 2. Worker Scaling

```go
cfg := &config.Config{
    MaxWorkers: runtime.NumCPU() * 4,
}
```

**Rule of thumb:**

- CPU-bound jobs: `NumCPU * 1-2`
- I/O-bound jobs: `NumCPU * 4-8`
- Mixed workload: `NumCPU * 2-4`

### 3. Timeout Configuration

```go
cfg := &config.Config{
    DefaultJobTimeout: 5 * time.Minute,
    ShutdownTimeout:   30 * time.Second,
    PopTimeout:        5 * time.Second,
}
```

Set `DefaultJobTimeout` based on your longest expected job duration.

### 4. Redis Connection Pooling

```go
cfg := &config.Config{
    RedisOptions: &redis.Options{
        PoolSize:        100,
        ConnMaxIdleTime: 5 * time.Minute,
    },
}
```

Pool size should accommodate: `MaxWorkers + overhead (20-30%)`

### 5. Recovery Configuration

```go
cfg := &config.Config{
    RedisRecoveryInterval: 30 * time.Second,
    RedisRecoveryTimeout:  10 * time.Minute,
}
```

Jobs stuck longer than `RecoveryTimeout` are automatically recovered.

### 6. Monitoring

```go
stats, _ := client.Backend().QueueStats(ctx, "default", "send_email")

if stats.Processing > cfg.MaxWorkers {
    log.Printf("Warning: More jobs processing than workers - possible stuck jobs")
}

errorRate := float64(stats.Failed) / float64(stats.Processed + stats.Failed)
if errorRate > 0.05 {
    log.Printf("Warning: High error rate detected: %.2f%%", errorRate*100)
}
```

### 7. Namespace Isolation

```go
client.Job(UserEvent{}).Ns("analytics").Enqueue(ctx)
client.Job(SendEmail{}).Ns("notifications").Enqueue(ctx)
client.Job(ProcessPayment{}).Ns("payments").Enqueue(ctx)
```

Use namespaces to:

- Isolate workloads
- Apply different retry policies
- Separate environments (dev/staging/prod)

### 8. Job Idempotency

Always design handlers to be idempotent:

```go
client.Handle(ProcessPayment{}, func(ctx context.Context, job *job.JobEnvelope) error {
    var payment ProcessPayment
    json.Unmarshal(job.Args, &payment)

    if alreadyProcessed(payment.ID) {
        return nil
    }

    return processPayment(payment)
})
```

---

## Architecture

### Components

```
┌─────────────┐
│   Client    │ ← Enqueues jobs
└──────┬──────┘
       │
       ▼
┌─────────────┐
│    Redis    │ ← Job storage & coordination
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   Workers   │ ← Process jobs
└─────────────┘
```

### Job Lifecycle

```
Enqueue → Pending → Active → Processed
                      ↓
                    Failed → Retry → Active
                      ↓
                    DLQ (after max retries)
```

---

### Benchmarks

**Single Worker:**

- Enqueue: ~10,000 jobs/sec
- Process: ~5,000 jobs/sec (simple handlers)

**Batch Operations:**

- Enqueue: ~100,000 jobs/sec (batches of 100)

**Scaling:**

- Linear scaling up to 100 workers
- Tested with 1M+ jobs in queue

### Optimization Tips

1. **Use Batch Operations** for bulk enqueuing
2. **Adjust `RedisPoolSize`** to match worker count
3. **Use Namespaces** to distribute load
4. **Monitor Redis Memory** - jobs are stored in RAM
5. **Tune `PopTimeout`** based on latency requirements

---

## Examples

See `/examples` directory:

- `simple/` - Simple job processing
- `scheduled-jobs/` - Delayed and scheduled jobs
- `embeddable-dashboard/` - Dashboard integration
- `continuous-enqueue/` - High-throughput batch operations

---

## License

MIT License - see LICENSE file for details

---

## Support

- **Issues**: https://github.com/olamilekan000/surge/issues
- **Discussions**: https://github.com/olamilekan000/surge/discussions

package backend

import (
	"context"
	"time"

	"github.com/olamilekan000/surge/surge/job"
)

type Backend interface {
	Push(ctx context.Context, job *job.JobEnvelope) error
	PushBatch(ctx context.Context, jobs []*job.JobEnvelope) error
	Pop(ctx context.Context, queues []string, timeout time.Duration) (*job.JobEnvelope, error)
	GetJob(ctx context.Context, jobID string) (*job.JobEnvelope, error)

	Ack(ctx context.Context, job *job.JobEnvelope) error
	Nack(ctx context.Context, job *job.JobEnvelope, reason error) error

	Retry(ctx context.Context, job *job.JobEnvelope) error
	MoveToDLQ(ctx context.Context, job *job.JobEnvelope) error

	Schedule(ctx context.Context, job *job.JobEnvelope, processAt time.Time) error
	GetScheduledJobs(ctx context.Context, namespace, queue string, offset, limit int) ([]*job.JobEnvelope, int64, error)
	DispatchScheduledJobs(ctx context.Context, limit int) (int, error)

	Pause(ctx context.Context, namespace, queue string) error
	Resume(ctx context.Context, namespace, queue string) error
	Drain(ctx context.Context, namespace, queue string) (int64, error)
	IsPaused(ctx context.Context, namespace, queue string) (bool, error)

	DiscoverQueues(ctx context.Context) ([]string, error)
	GetNamespaces(ctx context.Context) ([]string, error)

	QueueStats(ctx context.Context, namespace, queue string, from, to time.Time) (*QueueStats, error)

	InspectDLQ(ctx context.Context, namespace, queue string, offset, limit int) ([]*job.JobEnvelope, error)
	RetryFromDLQ(ctx context.Context, jobID string) error
	RetryAllDLQ(ctx context.Context, namespace, queue string) (int64, error)

	CheckUnique(ctx context.Context, uniqueKey string, ttl time.Duration) (bool, error)

	RegisterHandler(ctx context.Context, handlerName string) error
	GetRegisteredHandlers(ctx context.Context) ([]string, error)

	GetActiveWorkers(ctx context.Context) ([]string, error)

	Close() error
	IsHealthy() bool
	Heartbeat(ctx context.Context, workerID string, ttl time.Duration) error
}

type QueueInfo struct {
	Namespace string
	Queue     string
	FullKey   string
}

type QueueStats struct {
	Pending     int64 `json:"pending"`
	Processing  int64 `json:"processing"`
	Scheduled   int64 `json:"scheduled"`
	Failed      int64 `json:"failed"`
	Dead        int64 `json:"dead"`
	Processed   int64 `json:"processed"`
	MemoryUsage int64 `json:"memory_usage"`
	Paused      bool  `json:"paused"`
}

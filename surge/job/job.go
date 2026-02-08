package job

import (
	"encoding/json"
	"time"
)

type Priority int

const (
	PriorityCritical Priority = 100
	PriorityHigh     Priority = 50
	PriorityNormal   Priority = 0
	PriorityLow      Priority = -50
)

type JobState string

const (
	StatePending    JobState = "pending"
	StateScheduled  JobState = "scheduled"
	StateProcessing JobState = "processing"
	StateCompleted  JobState = "completed"
	StateFailed     JobState = "failed"
	StateDead       JobState = "dead"
)

type JobEnvelope struct {
	ID          string          `json:"id"`
	Topic       string          `json:"topic"`
	Args        json.RawMessage `json:"args"`
	Namespace   string          `json:"namespace"`
	Queue       string          `json:"queue"`
	State       JobState        `json:"state"`
	CreatedAt   time.Time       `json:"created_at"`
	ScheduledAt *time.Time      `json:"scheduled_at,omitempty"`
	StartedAt   *time.Time      `json:"started_at,omitempty"`
	CompletedAt *time.Time      `json:"completed_at,omitempty"`
	RetryCount  int             `json:"retry_count"`
	MaxRetries  int             `json:"max_retries"`
	NextRetryAt *time.Time      `json:"next_retry_at,omitempty"`
	LastError   string          `json:"last_error,omitempty"`
	Priority    Priority        `json:"priority"`
	UniqueKey   string          `json:"unique_key,omitempty"`
	Timeout     int             `json:"timeout"`
	Metadata    map[string]any  `json:"metadata,omitempty"`
}

type Job struct {
	ID     string
	Name   string
	Data   map[string]any
	Status string
}

func (j *JobEnvelope) Reset() {
	j.ID = ""
	j.Topic = ""
	j.Args = nil
	j.Namespace = ""
	j.Queue = ""
	j.State = ""
	j.CreatedAt = time.Time{}
	j.ScheduledAt = nil
	j.StartedAt = nil
	j.CompletedAt = nil
	j.RetryCount = 0
	j.MaxRetries = 0
	j.NextRetryAt = nil
	j.LastError = ""
	j.Priority = 0
	j.UniqueKey = ""
	j.Timeout = 0
	j.Metadata = nil
}

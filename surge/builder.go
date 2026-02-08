package surge

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/olamilekan000/surge/surge/errors"
	"github.com/olamilekan000/surge/surge/job"
)

type Batch struct {
	client *Client
	jobs   []*job.JobEnvelope
	mu     sync.Mutex
}

type JobBuilder struct {
	client     *Client
	batch      *Batch
	payload    any
	topic      string
	namespace  string
	queue      string
	maxRetries int
	priority   job.Priority
	uniqueFor  *time.Duration
	timeout    time.Duration
	scheduleAt *time.Time
	metadata   map[string]any
}

func (c *Client) Batch() *Batch {
	return &Batch{
		client: c,
		jobs:   make([]*job.JobEnvelope, 0),
	}
}

func (c *Client) Job(payload any) *JobBuilder {
	topic := getTopicName(payload)

	return &JobBuilder{
		client:     c,
		batch:      nil,
		payload:    payload,
		topic:      topic,
		namespace:  c.config.DefaultNamespace,
		queue:      topic,
		maxRetries: c.config.MaxRetries,
		priority:   job.PriorityNormal,
		timeout:    c.config.DefaultJobTimeout,
	}
}

func (c *Client) JobWithTopic(topic string, payload any) *JobBuilder {
	return &JobBuilder{
		client:     c,
		batch:      nil,
		payload:    payload,
		topic:      topic,
		namespace:  c.config.DefaultNamespace,
		queue:      topic,
		maxRetries: c.config.MaxRetries,
		priority:   job.PriorityNormal,
		timeout:    c.config.DefaultJobTimeout,
	}
}

func getTopicName(payload any) string {
	if str, ok := payload.(string); ok {
		return str
	}

	if namer, ok := payload.(JobNamer); ok {
		return namer.JobName()
	}

	t := reflect.TypeOf(payload)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	return t.Name()
}

func (jb *JobBuilder) Priority(priority job.Priority) *JobBuilder {
	jb.priority = priority
	return jb
}

func (jb *JobBuilder) Ns(ns string) *JobBuilder {
	jb.namespace = ns
	return jb
}

func (jb *JobBuilder) UniqueFor(d time.Duration) *JobBuilder {
	jb.uniqueFor = &d
	return jb
}

func (jb *JobBuilder) MaxRetries(maxRetries int) *JobBuilder {
	jb.maxRetries = maxRetries
	return jb
}

func (jb *JobBuilder) Schedule(d time.Duration) *JobBuilder {
	t := time.Now().Add(d)
	jb.scheduleAt = &t
	return jb
}

func (jb *JobBuilder) ScheduleAt(t time.Time) *JobBuilder {
	jb.scheduleAt = &t
	return jb
}

func (jb *JobBuilder) Metadata(md map[string]any) *JobBuilder {
	jb.metadata = md
	return jb
}

func (b *Batch) Job(payload any) *JobBuilder {
	topic := getTopicName(payload)
	return &JobBuilder{
		client:     b.client,
		batch:      b,
		payload:    payload,
		topic:      topic,
		namespace:  b.client.config.DefaultNamespace,
		queue:      topic,
		maxRetries: b.client.config.MaxRetries,
		priority:   job.PriorityNormal,
		timeout:    b.client.config.DefaultJobTimeout,
	}
}

func (b *Batch) Commit(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.jobs) == 0 {
		return nil
	}

	if err := b.client.backend.PushBatch(ctx, b.jobs); err != nil {
		return err
	}

	b.jobs = b.jobs[:0]

	return nil
}

func (jb *JobBuilder) buildEnvelope(ctx context.Context) (*job.JobEnvelope, error) {
	args, err := json.Marshal(jb.payload)
	if err != nil {
		return nil, &errors.ValidationError{
			Field:   "payload",
			Message: fmt.Sprintf("failed to marshal: %v", err),
		}
	}

	uniqueKey := ""
	topic := jb.topic
	if topic == "" {
		topic = getTopicName(jb.payload)
	}

	if jb.uniqueFor != nil {
		hash := sha256.Sum256(args)
		uniqueKey = fmt.Sprintf("%s:%s:%x", jb.namespace, topic, hash)
		isUnique, err := jb.client.backend.CheckUnique(ctx, uniqueKey, *jb.uniqueFor)
		if err != nil {
			return nil, err
		}
		if !isUnique {
			return nil, &errors.UniquenessViolationError{
				UniqueKey: uniqueKey,
			}
		}
	}

	envelope := &job.JobEnvelope{
		ID:         uuid.New().String(),
		Topic:      topic,
		Args:       args,
		Namespace:  jb.namespace,
		Queue:      jb.queue,
		MaxRetries: jb.maxRetries,
		Priority:   jb.priority,
		State:      job.StatePending,
		UniqueKey:  uniqueKey,
		CreatedAt:  time.Now(),
		Timeout:    int(jb.timeout.Seconds()),
		Metadata:   jb.metadata,
	}

	return envelope, nil
}

func (jb *JobBuilder) Enqueue(ctx context.Context) error {
	if jb.batch != nil {
		envelope, err := jb.buildEnvelope(ctx)
		if err != nil {
			return err
		}

		jb.batch.mu.Lock()
		jb.batch.jobs = append(jb.batch.jobs, envelope)
		jb.batch.mu.Unlock()

		return nil
	}

	envelope, err := jb.buildEnvelope(ctx)
	if err != nil {
		return err
	}

	if jb.scheduleAt != nil {
		envelope.State = job.StateScheduled
		envelope.ScheduledAt = jb.scheduleAt
		return jb.client.backend.Schedule(ctx, envelope, *jb.scheduleAt)
	}

	return jb.client.backend.Push(ctx, envelope)
}

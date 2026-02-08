package surge

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/olamilekan000/surge/surge/backend"
	"github.com/olamilekan000/surge/surge/config"
	"github.com/olamilekan000/surge/surge/errors"
	"github.com/olamilekan000/surge/surge/job"
)

var (
	shuffleRand *rand.Rand
	shuffleOnce sync.Once
)

type HandlerFunc func(context.Context, *job.JobEnvelope) error

type JobNamer interface {
	JobName() string
}

type Client struct {
	config         *config.Config
	backend        backend.Backend
	handlers       map[string]HandlerFunc
	mu             sync.RWMutex
	workerSem      chan struct{}
	workerWG       sync.WaitGroup
	shutdownWG     sync.WaitGroup
	shutdownOnce   sync.Once
	isShuttingDown atomic.Bool
	activeWorkers  atomic.Int64
}

func NewClient(ctx context.Context, cfg *config.Config) (*Client, error) {
	cfg.SetDefaults()

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	backend, err := cfg.CreateBackend(ctx)
	if err != nil {
		return nil, err
	}

	maxWorkers := cfg.MaxWorkers
	if maxWorkers <= 0 {
		maxWorkers = 50
	}

	return &Client{
		config:    cfg,
		backend:   backend,
		handlers:  make(map[string]HandlerFunc),
		workerSem: make(chan struct{}, maxWorkers),
	}, nil
}

func (c *Client) Backend() backend.Backend {
	return c.backend
}

func (c *Client) GetJob(ctx context.Context, jobID string) (*job.JobEnvelope, error) {
	return c.backend.GetJob(ctx, jobID)
}

func (c *Client) Close() error {
	return c.backend.Close()
}

func (c *Client) Shutdown(ctx context.Context) error {
	var shutdownErr error
	c.shutdownOnce.Do(func() {
		c.isShuttingDown.Store(true)

		done := make(chan struct{})
		go func() {
			c.workerWG.Wait()
			close(done)
		}()

		timeout := c.config.ShutdownTimeout

		select {
		case <-done:
			log.Printf("All workers finished gracefully")
		case <-time.After(timeout):
			activeCount := c.getActiveWorkerCount()
			shutdownErr = fmt.Errorf("shutdown timeout after %v: %d workers still active", timeout, activeCount)
			log.Printf("Shutdown timeout: %d workers still running", activeCount)
		case <-ctx.Done():
			shutdownErr = fmt.Errorf("shutdown cancelled: %w", ctx.Err())
		}
	})

	return shutdownErr
}

func (c *Client) getActiveWorkerCount() int {
	return int(c.activeWorkers.Load())
}

func (c *Client) Pause(ctx context.Context, namespace, queue string) error {
	return c.backend.Pause(ctx, namespace, queue)
}

func (c *Client) Resume(ctx context.Context, namespace, queue string) error {
	return c.backend.Resume(ctx, namespace, queue)
}

func (c *Client) IsPaused(ctx context.Context, namespace, queue string) (bool, error) {
	return c.backend.IsPaused(ctx, namespace, queue)
}

func (c *Client) Retry(ctx context.Context, job *job.JobEnvelope) error {
	return c.backend.Retry(ctx, job)
}

func (c *Client) Drain(ctx context.Context, namespace, queue string) (int64, error) {
	return c.backend.Drain(ctx, namespace, queue)
}

func (c *Client) Handle(payload any, handler HandlerFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()

	topic := getTopicName(payload)
	c.handlers[topic] = handler

	ctx := context.Background()
	if err := c.backend.RegisterHandler(ctx, topic); err != nil {
		log.Printf("Warning: Failed to register handler %s in backend: %v", topic, err)
	}
}

func (c *Client) GetRegisteredHandlers(ctx context.Context) ([]string, error) {
	handlers, err := c.backend.GetRegisteredHandlers(ctx)
	if err != nil {
		log.Printf("error getting registered handlers: %v", err)
		return []string{}, err
	}
	return handlers, nil
}

func (c *Client) Consume(ctx context.Context) error {
	workerID := uuid.New().String()

	c.shutdownWG.Add(2)
	go func() {
		defer c.shutdownWG.Done()
		c.heartbeat(ctx, workerID)
	}()
	go func() {
		defer c.shutdownWG.Done()
		c.runScheduler(ctx)
	}()

	err := wait.PollUntilContextCancel(
		ctx,
		c.config.PollInterval,
		true,
		func(ctx context.Context) (bool, error) {
			return c.consume(ctx)
		},
	)

	c.shutdownWG.Wait()

	return err
}

func (c *Client) consume(ctx context.Context) (bool, error) {
	if c.isShuttingDown.Load() {
		return true, nil
	}

	keys, err := c.getQueueKeys(ctx)
	if err != nil {
		log.Printf("error discovering queues: %v", err)
		return false, nil
	}

	if len(keys) == 0 {
		return false, nil
	}

	jobEnv, err := c.backend.Pop(ctx, keys, c.config.PopTimeout)
	if err != nil {
		log.Printf("error popping job from queue: %v", err)
		return false, nil
	}

	if jobEnv == nil {
		return false, nil
	}

	c.workerWG.Add(1)
	c.activeWorkers.Add(1)

	select {
	case c.workerSem <- struct{}{}:
		go func() {
			defer func() {
				c.activeWorkers.Add(-1)
				<-c.workerSem
				c.workerWG.Done()

				if r := recover(); r != nil {
					log.Printf("panic in worker processing job %s: %v", jobEnv.ID, r)
					if nackErr := c.backend.Nack(context.Background(), jobEnv,
						fmt.Errorf("panic: %v", r)); nackErr != nil {
						log.Printf("failed to NACK job after panic: %v", nackErr)
					}
				}

				job.PutJobEnvelope(jobEnv)
			}()

			if err := c.processJob(ctx, jobEnv); err != nil {
				log.Printf("error processing job: %v", err)
			}
		}()
	case <-ctx.Done():
		c.activeWorkers.Add(-1)
		c.workerWG.Done()
		return true, ctx.Err()
	}

	return false, nil
}

func (c *Client) heartbeat(ctx context.Context, workerID string) {
	ticker := time.NewTicker(c.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			heartbeatCtx, cancel := context.WithTimeout(context.Background(), c.config.HeartbeatInterval*2)
			err := c.backend.Heartbeat(heartbeatCtx, workerID, c.config.HeartbeatTTL)
			cancel()
			if err != nil {
				log.Printf("Failed to send heartbeat: %v", err)
			}
		}
	}
}

func (c *Client) processJob(ctx context.Context, job *job.JobEnvelope) error {
	c.mu.RLock()
	handler, exists := c.handlers[job.Topic]
	c.mu.RUnlock()

	if !exists {
		return c.backend.Nack(ctx, job, &errors.HandlerNotFoundError{
			Topic: job.Topic,
		})
	}

	jobTimeout := time.Duration(job.Timeout) * time.Second
	if jobTimeout == 0 {
		jobTimeout = c.config.DefaultJobTimeout
	}

	jobCtx, cancel := context.WithTimeout(ctx, jobTimeout)
	defer cancel()

	errChan := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		errChan <- handler(jobCtx, job)
	}()

	select {
	case err := <-errChan:
		if err != nil {
			return c.backend.Nack(jobCtx, job, err)
		}
		return c.backend.Ack(jobCtx, job)
	case <-jobCtx.Done():
		<-done
		nackCtx, nackCancel := context.WithTimeout(context.Background(), c.config.NackTimeout)
		defer nackCancel()

		if jobCtx.Err() == context.DeadlineExceeded {
			return c.backend.Nack(nackCtx, job, fmt.Errorf("job timeout after %v", jobTimeout))
		}

		return c.backend.Nack(nackCtx, job, fmt.Errorf("job cancelled: %v", jobCtx.Err()))
	}
}

func (c *Client) getQueueKeys(ctx context.Context) ([]string, error) {
	keys, err := c.backend.DiscoverQueues(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to discover queues: %w", err)
	}

	weightedShuffleQueues(keys)

	return keys, nil
}

func weightedShuffleQueues(queues []string) {
	if len(queues) <= 1 {
		return
	}

	shuffleOnce.Do(func() {
		shuffleRand = rand.New(rand.NewSource(time.Now().UnixNano()))
	})

	for i := len(queues) - 1; i > 0; i-- {
		j := shuffleRand.Intn(i + 1)
		queues[i], queues[j] = queues[j], queues[i]
	}
}

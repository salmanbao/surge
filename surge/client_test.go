package surge_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/olamilekan000/surge/surge"
	"github.com/olamilekan000/surge/surge/config"
	"github.com/olamilekan000/surge/surge/job"
)

type testContext struct {
	client *surge.Client
	cfg    *config.Config
}

func SetupTestWrapper(t *testing.T) *testContext {
	t.Helper()

	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "redis:7-alpine",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForLog("Ready to accept connections"),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err, "failed to start redis container")

	t.Cleanup(func() {
		if err := container.Terminate(ctx); err != nil {
			t.Logf("failed to terminate container: %v", err)
		}
	})

	host, err := container.Host(ctx)
	require.NoError(t, err)

	mappedPort, err := container.MappedPort(ctx, "6379/tcp")
	require.NoError(t, err)

	port, err := strconv.Atoi(mappedPort.Port())
	require.NoError(t, err)

	cfg := &config.Config{
		RedisOptions: &redis.Options{
			Addr: fmt.Sprintf("%s:%d", host, port),
		},
		RedisPingTimeout: 1 * time.Second,
	}
	cfg.SetDefaults()

	return &testContext{
		cfg: cfg,
	}
}

func TestSurge_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	testCtx := SetupTestWrapper(t)

	tests := []struct {
		name string
		run  func(t *testing.T, cfg *config.Config)
	}{
		{
			name: "Job Consumption",
			run: func(t *testing.T, cfg *config.Config) {
				ctx := context.Background()

				c, err := surge.NewClient(ctx, cfg)
				require.NoError(t, err)
				defer func() {
					shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer shutdownCancel()
					_ = c.Shutdown(shutdownCtx)
					_ = c.Close()
				}()

				type TestJob struct {
					Message string
				}

				processed := make(chan string, 1)
				c.Handle(TestJob{}, func(ctx context.Context, job *job.JobEnvelope) error {
					processed <- "done"
					return nil
				})

				err = c.Job(TestJob{Message: "hello"}).Enqueue(ctx)
				require.NoError(t, err)

				go c.Consume(ctx)

				select {
				case <-processed:

				case <-time.After(5 * time.Second):
					t.Fatal("timeout waiting for job processing")
				}
			},
		},
		{
			name: "Job Retries",
			run: func(t *testing.T, cfg *config.Config) {
				ctx := context.Background()

				c, err := surge.NewClient(ctx, cfg)
				require.NoError(t, err)
				defer func() {
					shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer shutdownCancel()
					_ = c.Shutdown(shutdownCtx)
					_ = c.Close()
				}()

				type RetryJob struct{}

				var attempts atomic.Int32
				done := make(chan struct{})

				c.Handle(RetryJob{}, func(ctx context.Context, job *job.JobEnvelope) error {
					current := attempts.Add(1)
					if current >= 2 {
						select {
						case <-done:
						default:
							close(done)
						}
					}
					return fmt.Errorf("failing job to trigger retry")
				})

				err = c.Job(RetryJob{}).MaxRetries(2).Enqueue(ctx)
				require.NoError(t, err)

				go c.Consume(ctx)

				select {
				case <-done:
					require.GreaterOrEqual(t, attempts.Load(), int32(2))
				case <-time.After(10 * time.Second):
					t.Fatal("timeout waiting for retries")
				}
			},
		},
		{
			name: "Scheduled Job",
			run: func(t *testing.T, cfg *config.Config) {
				ctx := context.Background()

				c, err := surge.NewClient(ctx, cfg)
				require.NoError(t, err)
				defer func() {
					shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer shutdownCancel()
					_ = c.Shutdown(shutdownCtx)
					_ = c.Close()
				}()

				type ScheduledJob struct{}

				processed := make(chan struct{})
				c.Handle(ScheduledJob{}, func(ctx context.Context, job *job.JobEnvelope) error {
					close(processed)
					return nil
				})

				start := time.Now()
				err = c.Job(ScheduledJob{}).ScheduleAt(start.Add(2 * time.Second)).Enqueue(ctx)
				require.NoError(t, err)

				go c.Consume(ctx)

				select {
				case <-processed:
					elapsed := time.Since(start)
					require.GreaterOrEqual(t, elapsed.Seconds(), 1.0, "Job processed too early (should be ~2s)")
				case <-time.After(5 * time.Second):
					t.Fatal("timeout waiting for scheduled job")
				}
			},
		},
		{
			name: "Test String Topic",
			run: func(t *testing.T, cfg *config.Config) {
				ctx := context.Background()

				c, err := surge.NewClient(ctx, cfg)
				require.NoError(t, err)
				defer func() {
					shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer shutdownCancel()
					_ = c.Shutdown(shutdownCtx)
					_ = c.Close()
				}()

				type StringJob struct {
					message string
				}

				processed := make(chan string, 1)
				c.Handle(StringJob{}, func(ctx context.Context, job *job.JobEnvelope) error {
					log.Printf("processing job, %v", job)
					processed <- string(job.Args)
					return nil
				})

				err = c.Job(StringJob{message: "hello from surge"}).Enqueue(ctx)
				require.NoError(t, err)

				go c.Consume(ctx)

				select {
				case <-processed:
				case <-time.After(5 * time.Second):
					t.Fatal("timeout waiting for string job")
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.run(t, testCtx.cfg)
		})
	}
}

type namedJobPayload struct {
	ID string
}

func (n namedJobPayload) JobName() string {
	return "CustomNamedJob"
}

func TestClient_Handle_RegistersHandlerInBackend(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration-style test in short mode")
	}

	testCtx := SetupTestWrapper(t)
	ctx := context.Background()

	c, err := surge.NewClient(ctx, testCtx.cfg)
	require.NoError(t, err)
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		_ = c.Shutdown(shutdownCtx)
		_ = c.Close()
	}()

	type EmailJob struct {
		To string
	}

	type NamedJob struct {
		Name string
	}

	testCases := []struct {
		name         string
		payload      any
		expectedName string
	}{
		{
			name:         "struct payload uses type name",
			payload:      EmailJob{},
			expectedName: "EmailJob",
		},
		{
			name:         "pointer payload uses underlying type name",
			payload:      &NamedJob{},
			expectedName: "NamedJob",
		},
		{
			name:         "string payload uses string directly",
			payload:      "string-topic",
			expectedName: "string-topic",
		},
		{
			name:         "JobNamer payload uses JobName",
			payload:      namedJobPayload{ID: "123"},
			expectedName: "CustomNamedJob",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c.Handle(tc.payload, func(ctx context.Context, j *job.JobEnvelope) error {
				return nil
			})
		})
	}

	handlers, err := c.GetRegisteredHandlers(ctx)
	require.NoError(t, err)

	for _, tc := range testCases {
		require.Containsf(
			t,
			handlers,
			tc.expectedName,
			"handler registry in backend should contain topic %q for test case %q",
			tc.expectedName,
			tc.name,
		)
	}
}

func TestClient_JobWithTopic_StringTopicAndStructuredPayload(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration-style test in short mode")
	}

	testCtx := SetupTestWrapper(t)
	ctx := context.Background()

	c, err := surge.NewClient(ctx, testCtx.cfg)
	require.NoError(t, err)
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		_ = c.Shutdown(shutdownCtx)
		_ = c.Close()
	}()

	type SendEmail struct {
		To      string
		Subject string
		Body    string
	}

	done := make(chan struct{})

	c.Handle("email.sent", func(ctx context.Context, j *job.JobEnvelope) error {
		var email SendEmail
		if err := json.Unmarshal(j.Args, &email); err != nil {
			return err
		}

		if email.To == "user@example.com" && email.Subject == "Welcome" {
			close(done)
		}

		return nil
	})

	err = c.JobWithTopic("email.sent", SendEmail{
		To:      "user@example.com",
		Subject: "Welcome",
		Body:    "Thanks for signing up!",
	}).Enqueue(ctx)
	require.NoError(t, err)

	go c.Consume(ctx)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for JobWithTopic handler")
	}
}

func TestClient_JobWithMetadata(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration-style test in short mode")
	}

	testCtx := SetupTestWrapper(t)
	ctx := context.Background()

	c, err := surge.NewClient(ctx, testCtx.cfg)
	require.NoError(t, err)
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		_ = c.Shutdown(shutdownCtx)
		_ = c.Close()
	}()

	type MetadataJob struct {
		Data string
	}

	done := make(chan struct{})

	c.Handle(MetadataJob{}, func(ctx context.Context, j *job.JobEnvelope) error {
		defer close(done)

		val, ok := j.Metadata["user_id"].(string)
		require.True(t, ok, "metadata user_id should be present and a string")
		require.Equal(t, "12345", val, "metadata user_id should match")

		role, ok := j.Metadata["role"].(string)
		require.True(t, ok, "metadata role should be present and a string")
		require.Equal(t, "admin", role, "metadata role should match")

		return nil
	})

	md := map[string]any{
		"user_id": "12345",
		"role":    "admin",
	}

	err = c.Job(MetadataJob{Data: "test"}).
		Metadata(md).
		Enqueue(ctx)
	require.NoError(t, err)

	go c.Consume(ctx)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for MetadataJob handler")
	}
}

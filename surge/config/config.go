package config

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/olamilekan000/surge/surge/backend"
	"github.com/olamilekan000/surge/surge/driver"
)

type Config struct {
	// Storage driver to use ("redis", "memory", or "custom"). Currently only uses "redis".
	Driver driver.Driver

	// Interval between checking for stalled jobs to recover. Defaults to 30s.
	RedisRecoveryInterval time.Duration

	// How long a job must be stuck in "processing" before it is considered stalled and recovered. Defaults to 10m.
	RedisRecoveryTimeout time.Duration

	// Configuration for connecting to a High-Availability Redis Sentinel cluster.
	RedisFailover *redis.FailoverOptions

	// Configuration for connecting to a standalone Redis instance.
	RedisOptions *redis.Options

	// The default namespace queue where untagged jobs will be routed.
	// If omitted, Surge automatically sets this to "default".
	DefaultNamespace string

	// Maximum number of concurrent worker goroutines polling for jobs within a single consumer instance. Defaults to 25.
	MaxWorkers int

	// How often the consumer should poll Redis for new jobs when idle. Defaults to 100ms.
	PollInterval time.Duration

	// How often the scheduler sweeps for scheduled/delayed jobs to activate. Defaults to 15s.
	ScanInterval time.Duration

	// How long to wait for active workers to finish processing during graceful shutdown before forcefully exiting. Defaults to 30s.
	ShutdownTimeout time.Duration

	// Maximum number of times a failing job will be retried before being dropped. Defaults to 25.
	MaxRetries int

	// Maximum number of Redis commands to batch together before executing a pipeline. Defaults to 100.
	PipelineSize int

	// How often the consumer sends heartbeat signals to Redis to prove it is alive. Defaults to 5s.
	HeartbeatInterval time.Duration

	// How long a consumer's heartbeat is considered valid before the cluster assumes it has crashed or isn't available to consume jobs. Defaults to 30s.
	HeartbeatTTL time.Duration

	// Maximum time to block in Redis waiting for a new job via BLPOP before timing out and trying again. Defaults to 5s.
	PopTimeout time.Duration

	// Maximum time to wait for a NACK (Negative Acknowledge) operation to succeed when a job fails. Defaults to 5s.
	NackTimeout time.Duration

	// Default hard-timeout for how long a job handler can run before its context is cancelled. Defaults to 5m.
	DefaultJobTimeout time.Duration

	// Timeout for verifying the connection to Redis during startup. Defaults to 5s.
	RedisPingTimeout time.Duration

	// Prefix added to all Surge-related keys in Redis (e.g., "surge:queue:default"). Defaults to "surge".
	RedisPrefix string
}

func (c *Config) SetDefaults() {
	if string(c.Driver) == "" {
		c.Driver = driver.DriverRedis
	}
	if c.Driver == driver.DriverRedis {
		if c.RedisFailover == nil && c.RedisOptions == nil {
			c.RedisOptions = &redis.Options{
				Addr:            "localhost:6379",
				PoolSize:        10,
				MaxRetries:      3,
				ConnMaxIdleTime: 5 * time.Minute,
			}
		}
		if c.RedisRecoveryInterval == 0 {
			c.RedisRecoveryInterval = 30 * time.Second
		}
		if c.RedisRecoveryTimeout == 0 {
			c.RedisRecoveryTimeout = 10 * time.Minute
		}
	}
	if c.DefaultNamespace == "" {
		c.DefaultNamespace = "default"
	}
	if c.MaxWorkers == 0 {
		c.MaxWorkers = 25
	}
	if c.PollInterval == 0 {
		c.PollInterval = 100 * time.Millisecond
	}
	if c.ScanInterval == 0 {
		c.ScanInterval = 15 * time.Second
	}
	if c.ShutdownTimeout == 0 {
		c.ShutdownTimeout = 30 * time.Second
	}
	if c.MaxRetries == 0 {
		c.MaxRetries = 25
	}
	if c.PipelineSize == 0 {
		c.PipelineSize = 100
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 5 * time.Second
	}
	if c.HeartbeatTTL == 0 {
		c.HeartbeatTTL = 30 * time.Second
	}
	if c.PopTimeout == 0 {
		c.PopTimeout = 5 * time.Second
	}
	if c.NackTimeout == 0 {
		c.NackTimeout = 5 * time.Second
	}
	if c.DefaultJobTimeout == 0 {
		c.DefaultJobTimeout = 5 * time.Minute
	}
	if c.RedisPingTimeout == 0 {
		c.RedisPingTimeout = 5 * time.Second
	}
	if c.RedisPrefix == "" {
		c.RedisPrefix = "surge"
	}
}

func (c *Config) Validate() error {
	if c.MaxWorkers < 1 {
		return errors.New("max_workers must be >= 1")
	}
	if c.MaxRetries < 0 {
		return errors.New("max_retries must be >= 0")
	}
	if c.PollInterval <= 0 {
		return errors.New("poll_interval must be > 0")
	}
	if c.ScanInterval <= 0 {
		return errors.New("scan_interval must be > 0")
	}
	if c.ShutdownTimeout <= 0 {
		return errors.New("shutdown_timeout must be > 0")
	}
	if c.PipelineSize < 1 {
		return errors.New("pipeline_size must be >= 1")
	}

	switch c.Driver {
	case driver.DriverRedis, "":
		if c.RedisFailover == nil && c.RedisOptions == nil {
			return errors.New("redis_failover or redis_options must be provided")
		}

	default:
		return errors.New("unsupported driver: " + string(c.Driver))
	}

	if c.DefaultNamespace == "" {
		return errors.New("default_namespace cannot be empty")
	}

	return nil
}

func (c *Config) CreateBackend(ctx context.Context) (backend.Backend, error) {
	switch c.Driver {
	case driver.DriverRedis, "":
		redisCfg := backend.RedisConfig{
			RecoveryInterval: c.RedisRecoveryInterval,
			RecoveryTimeout:  c.RedisRecoveryTimeout,
			PingTimeout:      c.RedisPingTimeout,
			Prefix:           c.RedisPrefix,
			Failover:         c.RedisFailover,
			Options:          c.RedisOptions,
		}
		return backend.NewRedisBackend(ctx, redisCfg)
	default:
		return nil, errors.New("unsupported driver: " + string(c.Driver))
	}
}

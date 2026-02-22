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
	Driver driver.Driver // "redis" (default)

	RedisRecoveryInterval time.Duration
	RedisRecoveryTimeout  time.Duration
	RedisFailover         *redis.FailoverOptions
	RedisOptions          *redis.Options

	MemoryMaxJobs     int
	CustomBackend     backend.Backend
	Namespaces        []string
	DefaultNamespace  string
	MaxWorkers        int
	PollInterval      time.Duration
	ScanInterval      time.Duration
	ShutdownTimeout   time.Duration
	MaxRetries        int
	PipelineSize      int
	HeartbeatInterval time.Duration
	HeartbeatTTL      time.Duration
	PopTimeout        time.Duration
	NackTimeout       time.Duration
	DefaultJobTimeout time.Duration
	RedisPingTimeout  time.Duration
	RedisPrefix       string
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
	if c.Driver == driver.DriverMemory {
		if c.MemoryMaxJobs == 0 {
			c.MemoryMaxJobs = 10000
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

	case driver.DriverMemory:
		if c.MemoryMaxJobs < 1 {
			return errors.New("memory_max_jobs must be >= 1")
		}

	case driver.DriverCustom:
		if c.CustomBackend == nil {
			return errors.New("custom_backend must be provided when driver is 'custom'")
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
	case driver.DriverCustom:
		if c.CustomBackend == nil {
			return nil, errors.New("custom_backend must be provided when driver is 'custom'")
		}
		return c.CustomBackend, nil
	default:
		return nil, errors.New("unsupported driver: " + string(c.Driver))
	}
}

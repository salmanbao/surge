package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/olamilekan000/surge/surge"
	"github.com/olamilekan000/surge/surge/config"
	"github.com/olamilekan000/surge/surge/job"
)

type HeartbeatJob struct {
	Timestamp int64 `json:"timestamp"`
}

func (h HeartbeatJob) JobName() string {
	return "sentinel_heartbeat"
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("\nReceived interrupt signal, shutting down gracefully...")
		cancel()
	}()

	cfg := &config.Config{
		RedisFailover: &redis.FailoverOptions{
			MasterName: "mymaster",
			SentinelAddrs: []string{
				"127.0.0.1:26379",
				"127.0.0.1:26380",
				"127.0.0.1:26381",
			},
			DialTimeout:  5 * time.Second,
			ReadTimeout:  3 * time.Second,
			WriteTimeout: 3 * time.Second,
			PoolSize:     10,
			DB:           1,

			// Fix for Docker on Mac/Windows:
			// Sentinels running in Docker will advertise the internal container IPs (e.g. 172.18.0.2)
			// Your Mac/Windows host cannot route to these IPs. This simple dialer intercepts those
			// internal IPs and rewrites them to connect to your localhost mapped ports instead!
			Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
				if strings.HasSuffix(addr, ":6379") {
					addr = "127.0.0.1:6379"
				} else if strings.HasSuffix(addr, ":6380") {
					addr = "127.0.0.1:6380"
				} else if strings.HasSuffix(addr, ":6381") {
					addr = "127.0.0.1:6381"
				}
				return net.DialTimeout(network, addr, 5*time.Second)
			},
		},
		DefaultNamespace: "platform",
		MaxWorkers:       10,
	}

	client, err := surge.NewClient(ctx, cfg)
	if err != nil {
		log.Fatalf("Failed to create client with Sentinel: %v", err)
	}
	defer client.Close()

	log.Println("Successfully connected to Redis via Sentinel!")

	client.Handle(HeartbeatJob{}, func(ctx context.Context, job *job.JobEnvelope) error {
		log.Printf("[Namespace: %s] Heartbeat processed at: %v", job.Namespace, time.Now().Format(time.StampMilli))
		return nil
	})

	go func() {
		log.Println("Starting Sentinel job consumer...")
		if err := client.Consume(ctx); err != nil && err != context.Canceled {
			log.Printf("Consumer error: %v", err)
		}
	}()

	fmt.Println("\nRunning Sentinel HA Test:")
	fmt.Println("1. While this is running, try killing the master container:")
	fmt.Println("   $ docker stop surge-redis-master-1")
	fmt.Println("2. Watch as the jobs momentarily pause, Sentinels elect a new master, and jobs automatically resume!")
	fmt.Println("-------------------------------------------------------------------------------------------------")

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Shutting down...")
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer shutdownCancel()

			if err := client.Shutdown(shutdownCtx); err != nil {
				log.Printf("Shutdown error: %v", err)
			}
			return

		case t := <-ticker.C:
			err := client.Job(HeartbeatJob{Timestamp: t.Unix()}).Enqueue(ctx)
			if err != nil {
				log.Printf("Failed to enqueue string default namespace: %v", err)
			} else {
				log.Println("Enqueued heartbeat to platform")
			}

			err = client.Job(HeartbeatJob{Timestamp: t.Unix()}).Ns("payment").Enqueue(ctx)
			if err != nil {
				log.Printf("Failed to enqueue to payment namespace: %v", err)
			} else {
				log.Println("Enqueued heartbeat to payment")
			}
		}
	}
}

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/olamilekan000/surge/surge"
	"github.com/olamilekan000/surge/surge/config"
	"github.com/olamilekan000/surge/surge/job"
)

// Simple job types for the example
type ProcessOrder struct {
	OrderID string  `json:"order_id"`
	Amount  float64 `json:"amount"`
}

type SendNotification struct {
	UserID  string `json:"user_id"`
	Message string `json:"message"`
}

func (p ProcessOrder) JobName() string {
	return "process_order"
}

func (s SendNotification) JobName() string {
	return "send_notification"
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("Received interrupt signal, shutting down...")
		cancel()
	}()

	cfg := &config.Config{
		RedisOptions: &redis.Options{
			Addr: "localhost:6379",
			DB:   1,
		},
		DefaultNamespace: "payment",
	}

	client, err := surge.NewClient(ctx, cfg)
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	client.Handle(SendNotification{}, func(ctx context.Context, job *job.JobEnvelope) error {
		var notification SendNotification
		if err := json.Unmarshal(job.Args, &notification); err != nil {
			return err
		}

		time.Sleep(10 * time.Millisecond)

		fmt.Printf("Processed notification for user %s: %s\n", notification.UserID, notification.Message)
		return nil
	})

	client.Handle(ProcessOrder{}, func(ctx context.Context, job *job.JobEnvelope) error {
		var order ProcessOrder
		if err := json.Unmarshal(job.Args, &order); err != nil {
			return err
		}

		time.Sleep(2 * time.Millisecond)
		fmt.Printf("Processed order %s: $%.2f\n", order.OrderID, order.Amount)
		return nil
	})

	client.Handle(SendNotification{}, func(ctx context.Context, job *job.JobEnvelope) error {
		var notification SendNotification
		if err := json.Unmarshal(job.Args, &notification); err != nil {
			return err
		}

		time.Sleep(10 * time.Millisecond)
		fmt.Printf("Processed notification for user %s: %s\n", notification.UserID, notification.Message)
		return nil
	})

	go func() {
		log.Println("Starting job consumer...")
		if err := client.Consume(ctx); err != nil && err != context.Canceled {
			log.Printf("Consumer error: %v", err)
		}
	}()

	fmt.Println("Starting continuous job enqueueing + processing...")
	fmt.Println("Jobs will be enqueued and processed with different priorities")
	fmt.Println("🎯 Press Ctrl+C to stop")
	fmt.Println("Enqueuing: 5 jobs every 200ms (~25 jobs/sec)")

	jobCounter := 0
	batchSize := 5
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	go func() {
		log.Println("Starting job consumer...")
		if err := client.Consume(ctx); err != nil && err != context.Canceled {
			log.Printf("Consumer error: %v", err)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			fmt.Printf("\nShutting down gracefully...\n")

			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer shutdownCancel()

			if err := client.Shutdown(shutdownCtx); err != nil {
				log.Printf("Shutdown error: %v", err)
			} else {
				fmt.Printf("All workers finished gracefully. Total jobs processed: %d\n", jobCounter)
			}
			return

		case <-ticker.C:
			batch := client.Batch()

			for i := 0; i < batchSize; i++ {
				jobCounter++

				switch rand.Intn(3) {
				case 0:
					order := &ProcessOrder{
						OrderID: fmt.Sprintf("order-%d", jobCounter),
						Amount:  rand.Float64() * 1000,
					}
					batch.Job(order).
						Priority(job.PriorityHigh).
						Ns("payment").
						Enqueue(ctx)

				case 1:
					notification := &SendNotification{
						UserID:  fmt.Sprintf("user-%d", rand.Intn(1000)),
						Message: fmt.Sprintf("Notification #%d", jobCounter),
					}
					batch.Job(notification).
						Priority(job.PriorityNormal).
						Ns("communications").
						Enqueue(ctx)

				case 2:
					order := &ProcessOrder{
						OrderID: fmt.Sprintf("bulk-order-%d", jobCounter),
						Amount:  rand.Float64() * 100,
					}
					batch.Job(order).
						Priority(job.PriorityHigh).
						Ns("platform").
						Enqueue(ctx)
				}
			}

			start := time.Now()
			if err := batch.Commit(ctx); err != nil {
				log.Printf("Failed to commit batch: %v", err)
			} else {
				elapsed := time.Since(start)
				fmt.Printf("Enqueued batch #%d (%d jobs) in %v\n",
					jobCounter/batchSize, batchSize, elapsed)
			}

			if jobCounter%(batchSize*10) == 0 {
				showQueueStats(ctx, client, jobCounter)
			}
		}
	}
}

func showQueueStats(ctx context.Context, client *surge.Client, totalJobs int) {
	namespaces := []string{"payment", "communications", "platform"}

	fmt.Printf("\nQueue Stats (Total jobs: %d)\n", totalJobs)
	fmt.Println("─" + "────────────────────────────────────")

	var totalPending, totalProcessing, totalProcessed int64

	for _, ns := range namespaces {
		var queueName string
		switch ns {
		case "payment":
			queueName = "process_order"
		case "communications":
			queueName = "send_notification"
		case "platform":
			queueName = "process_order"
		}

		stats, err := client.Backend().QueueStats(ctx, ns, queueName, time.Time{}, time.Time{})
		if err != nil {
			continue
		}

		fmt.Printf("│ %s: %d pending, %d processing, %d processed\n",
			ns, stats.Pending, stats.Processing, stats.Processed)

		totalPending += stats.Pending
		totalProcessing += stats.Processing
		totalProcessed += stats.Processed
	}

	fmt.Printf("│ TOTAL: %d pending, %d processing, %d processed\n", totalPending, totalProcessing, totalProcessed)
	fmt.Println("─" + "────────────────────────────────────")
}

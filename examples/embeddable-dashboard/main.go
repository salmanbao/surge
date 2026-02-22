package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/olamilekan000/surge/surge"
	"github.com/olamilekan000/surge/surge/config"
	"github.com/olamilekan000/surge/surge/job"
	"github.com/olamilekan000/surge/surge/server"
)

// This simulates how web applications would integrate Surge and
// its dashboard into their existing HTTP server.

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
		RedisOptions: &redis.Options{
			Addr: "localhost:6379",
			DB:   1,
		},
		DefaultNamespace: "platform",
		MaxWorkers:       50,
	}

	client, err := surge.NewClient(ctx, cfg)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Register handlers for all job types
	client.Handle(ProcessWebhook{}, func(ctx context.Context, job *job.JobEnvelope) error {
		var webhook ProcessWebhook
		if err := json.Unmarshal(job.Args, &webhook); err != nil {
			return err
		}
		time.Sleep(50 * time.Millisecond)
		log.Printf("Processed webhook %s: %v", webhook.WebhookID, webhook.Payload)
		return nil
	})

	client.Handle(ProcessOrder{}, func(ctx context.Context, job *job.JobEnvelope) error {
		var order ProcessOrder
		if err := json.Unmarshal(job.Args, &order); err != nil {
			return err
		}
		time.Sleep(50 * time.Millisecond)
		log.Printf("Processed order %s: $%.2f", order.OrderID, order.Amount)
		return nil
	})

	client.Handle("send_email", func(ctx context.Context, job *job.JobEnvelope) error {
		var email SendEmail
		if err := json.Unmarshal(job.Args, &email); err != nil {
			return err
		}
		time.Sleep(50 * time.Millisecond)
		log.Printf("Sent email to %s: %s", email.To, email.Subject)
		return nil
	})

	client.Handle(ProcessPayment{}, func(ctx context.Context, job *job.JobEnvelope) error {
		var payment ProcessPayment
		if err := json.Unmarshal(job.Args, &payment); err != nil {
			return err
		}
		time.Sleep(50 * time.Millisecond)
		log.Printf("Processed payment %s: %s %.2f", payment.PaymentID, payment.Currency, payment.Amount)
		return nil
	})

	client.Handle(ProcessRefund{}, func(ctx context.Context, job *job.JobEnvelope) error {
		var refund ProcessRefund
		if err := json.Unmarshal(job.Args, &refund); err != nil {
			return err
		}
		time.Sleep(50 * time.Millisecond)
		log.Printf("Processed refund %s: $%.2f (reason: %s)", refund.RefundID, refund.Amount, refund.Reason)
		return nil
	})

	client.Handle(SendNotification{}, func(ctx context.Context, job *job.JobEnvelope) error {
		var notification SendNotification
		if err := json.Unmarshal(job.Args, &notification); err != nil {
			return err
		}
		time.Sleep(10 * time.Millisecond)
		log.Printf("Sent notification to user %s: %s", notification.UserID, notification.Message)
		return nil
	})

	// Start consumer in background
	go func() {
		log.Println("Starting job consumer...")
		if err := client.Consume(ctx); err != nil && err != context.Canceled {
			log.Printf("Consumer error: %v", err)
		}
	}()

	dashboard := server.NewDashboardServer(client, 0)
	dashboard.SetRootPath("/queues/surge-dashboard")

	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"message": "Main application server",
			"surge":   "Dashboard available at /queues/surge-dashboard/",
		})
	})

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"status": "healthy",
		})
	})

	mux.Handle("/queues/surge-dashboard/", dashboard.Handler())

	serverAddr := ":8080"
	log.Println("Starting combined HTTP server...")
	log.Printf("Main app: http://localhost%s/", serverAddr)
	log.Printf("Surge Dashboard: http://localhost%s/queues/surge-dashboard/", serverAddr)

	serverErrChan := make(chan error, 1)
	go func() {
		serverErrChan <- http.ListenAndServe(serverAddr, mux)
	}()

	log.Println("Enqueuing jobs...")
	for i := 0; i < 10; i++ {
		webhook := &ProcessWebhook{
			WebhookID: fmt.Sprintf("wh-%d", i+1),
			Endpoint:  fmt.Sprintf("https://api.example.com/webhooks/%d", i+1),
			Payload: map[string]any{
				"event":     "user.created",
				"user_id":   fmt.Sprintf("user-%d", i+1),
				"timestamp": time.Now().Unix(),
			},
			Headers: map[string]string{
				"Content-Type": "application/json",
				"X-Webhook-ID": fmt.Sprintf("wh-%d", i+1),
			},
			RetryCount: 0,
		}

		if err := client.Job(webhook).Enqueue(ctx); err != nil {
			log.Printf("failed to enqueue job: %v", err)
		}
	}

	select {
	case <-ctx.Done():
		log.Println("shutting down gracefully...")

		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer shutdownCancel()

		if err := client.Shutdown(shutdownCtx); err != nil {
			log.Printf("shutdown error: %v", err)
		}

	case err := <-serverErrChan:
		log.Fatalf("server error: %v", err)
	}

	log.Println("server stopped")
}

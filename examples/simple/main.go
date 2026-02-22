package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/olamilekan000/surge/surge"
	"github.com/olamilekan000/surge/surge/config"
	"github.com/olamilekan000/surge/surge/job"
	"github.com/olamilekan000/surge/surge/server"
)

type ProcessOrder struct {
	OrderID string  `json:"order_id"`
	Amount  float64 `json:"amount"`
}

type SendEmail struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

type ProcessPayment struct {
	PaymentID string  `json:"payment_id"`
	Amount    float64 `json:"amount"`
	Currency  string  `json:"currency"`
}

type ProcessRefund struct {
	RefundID string  `json:"refund_id"`
	Amount   float64 `json:"amount"`
	Reason   string  `json:"reason"`
}

type SendNotification struct {
	UserID  string `json:"user_id"`
	Message string `json:"message"`
}

func (s SendNotification) JobName() string {
	return "send_notification"
}

func (s SendEmail) JobName() string {
	return "send_email"
}

func (p ProcessOrder) JobName() string {
	return "process_order"
}

func (p ProcessPayment) JobName() string {
	return "process_payment"
}

func (p ProcessRefund) JobName() string {
	return "process_refund"
}

func main() {
	ctx := context.Background()

	cfg := &config.Config{
		RedisOptions: &redis.Options{
			Addr: "localhost:6379",
			DB:   1,
		},
		DefaultNamespace: "platform",
	}

	client, err := surge.NewClient(ctx, cfg)
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	// Start Dashboard API (standalone mode - backward compatible)
	// Dashboard will be available at http://localhost:8080/api/queues
	srv := server.NewDashboardServer(client, 8080)
	go func() {
		if err := srv.Start(); err != nil {
			log.Printf("Dashboard server error: %v", err)
		}
	}()
	log.Println("📊 Dashboard available at http://localhost:8080")
	log.Println("   Try: http://localhost:8080/api/queues")

	// Jobs in default namespace
	client.Job(&ProcessOrder{OrderID: "123", Amount: 100}).Enqueue(ctx)
	client.Job(&SendEmail{To: "test@test.com", Subject: "Test"}).
		UniqueFor(10 * time.Minute). // Example of unique job usage
		Enqueue(ctx)

	// Job in specific namespace
	client.Job(&ProcessPayment{PaymentID: "pay_456", Amount: 99.99, Currency: "USD"}).
		Ns("payment").
		Enqueue(ctx)

	// Job that will fail (to test Nack/retry)
	client.Job(&ProcessRefund{RefundID: "ref_789", Amount: 50.00, Reason: "Customer request"}).
		MaxRetries(3).
		Enqueue(ctx)

	client.Handle(ProcessOrder{}, func(ctx context.Context, job *job.JobEnvelope) error {
		var order ProcessOrder
		if err := json.Unmarshal(job.Args, &order); err != nil {
			return err
		}

		log.Printf("Processing order: %+v", order)
		// time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
		log.Printf("Order processed: %+v", order)
		return nil
	})

	client.Handle(SendEmail{}, func(ctx context.Context, job *job.JobEnvelope) error {
		var email SendEmail
		if err := json.Unmarshal(job.Args, &email); err != nil {
			return err
		}

		log.Printf("Sending email: %+v", email)
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
		log.Printf("Email sent: %+v", email)

		return nil
	})

	client.Handle(ProcessPayment{}, func(ctx context.Context, job *job.JobEnvelope) error {
		var payment ProcessPayment
		if err := json.Unmarshal(job.Args, &payment); err != nil {
			return err
		}

		log.Printf("Processing payment in namespace %s: %+v", job.Namespace, payment)
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
		log.Printf("Payment processed: %+v", payment)

		return nil
	})

	client.Handle(SendNotification{}, func(ctx context.Context, job *job.JobEnvelope) error {
		var notification SendNotification
		if err := json.Unmarshal(job.Args, &notification); err != nil {
			return err
		}

		// Simulate processing time (very fast for notifications)
		time.Sleep(10 * time.Millisecond)

		fmt.Printf("📧 Processed notification for user %s: %s\n", notification.UserID, notification.Message)
		return nil
	})

	// Handler that always returns an error (to test Nack/retry)
	client.Handle(ProcessRefund{}, func(ctx context.Context, job *job.JobEnvelope) error {
		var refund ProcessRefund
		if err := json.Unmarshal(job.Args, &refund); err != nil {
			return err
		}

		log.Printf("Processing refund (attempt %d/%d): %+v", job.RetryCount+1, job.MaxRetries+1, refund)

		// Always fail to test retry logic
		return fmt.Errorf("refund processing failed: insufficient funds")
	})

	// Example: Batch enqueue multiple jobs (like Convoy's fan-out scenario)
	log.Println("=== Batch Enqueue Example ===")
	batch := client.Batch()

	// Simulate 10 webhook deliveries (in real Convoy, this could be 500+)
	for i := 0; i < 20; i++ {
		batch.Job(&SendEmail{
			To:      fmt.Sprintf("user%d@example.com", i),
			Subject: fmt.Sprintf("Batch email #%d", i),
		}).
			Ns("communications").
			Priority(job.PriorityHigh).
			Enqueue(ctx)
	}

	// Single round-trip to Redis for all 10 jobs
	start := time.Now()
	if err := batch.Commit(ctx); err != nil {
		log.Fatalf("Failed to commit batch: %v", err)
	}
	elapsed := time.Since(start)
	log.Printf("✅ Batch committed %d jobs in %v (would take ~%v sequentially)", 10, elapsed, 10*time.Millisecond)

	client.Consume(ctx)
}

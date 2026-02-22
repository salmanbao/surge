package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/olamilekan000/surge/surge"
	"github.com/olamilekan000/surge/surge/config"
)

type EmailJob struct {
	UserID  string `json:"user_id"`
	Subject string `json:"subject"`
}

func (j EmailJob) JobName() string {
	return "send_email"
}

func main() {
	cfg := &config.Config{
		RedisOptions: &redis.Options{
			Addr: "localhost:6379",
			DB:   1,
		},
		DefaultNamespace: "communications",
	}
	client, _ := surge.NewClient(context.Background(), cfg)
	defer client.Close()

	email := EmailJob{UserID: "user_123", Subject: "Welcome!"}

	err := client.Job(email).Schedule(1 * time.Minute).Enqueue(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Job scheduled for 1 minute from now")

	runAt := time.Now().Add(24 * time.Hour)
	err = client.Job(email).ScheduleAt(runAt).Enqueue(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Job scheduled for tomorrow")
}

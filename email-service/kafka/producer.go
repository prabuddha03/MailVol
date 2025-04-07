package kafka

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
}

func NewProducer() *Producer {
	// In Docker, we use the internal address
	broker := "kafka:29092"
	topicName := "email-jobs"

	log.Printf("Initializing Kafka producer with broker: %s for topic: %s", broker, topicName)

	// Initialize the writer with a simple configuration
	writer := &kafka.Writer{
		Addr:         kafka.TCP(broker),
		Topic:        topicName,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 50 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
		Async:        false, // Use synchronous mode for reliability
		// Increase dial timeout for connections
		Transport: &kafka.Transport{
			DialTimeout: 15 * time.Second,
		},
		AllowAutoTopicCreation: true,
	}

	log.Printf("Kafka producer created and ready")
	return &Producer{
		writer: writer,
	}
}

func (p *Producer) SendEmailJob(ctx context.Context, job interface{}) error {
	value, err := json.Marshal(job)
	if err != nil {
		log.Printf("Error marshaling job: %v", err)
		return err
	}

	log.Printf("Sending message to Kafka: %s", string(value))

	// Implement retry logic for sending messages
	maxRetries := 8
	baseRetryDelay := 2 * time.Second

	for i := 0; i < maxRetries; i++ {
		// Create a timeout context for each attempt
		msgCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		err = p.writer.WriteMessages(msgCtx, kafka.Message{Value: value})
		cancel()

		if err == nil {
			log.Printf("Successfully sent message to Kafka")
			return nil
		}

		// Log the error and retry
		log.Printf("Error sending message (attempt %d/%d): %v", i+1, maxRetries, err)

		if i < maxRetries-1 {
			retryDelay := baseRetryDelay * time.Duration(i+1)
			log.Printf("Retrying in %v...", retryDelay)
			time.Sleep(retryDelay)
		}
	}

	log.Printf("Failed to send message after %d attempts", maxRetries)
	return err
}

func (p *Producer) Close() error {
	return p.writer.Close()
}

package kafka

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader   *kafka.Reader
	handlers map[string]func([]byte) error
}

func NewConsumer() *Consumer {
	// In Docker, we use the internal address
	broker := "kafka:29092"
	topicName := "email-jobs"

	log.Printf("Initializing Kafka consumer with broker: %s for topic: %s", broker, topicName)

	// Create reader with a simple configuration
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{broker},
		Topic:       topicName,
		GroupID:     "email-service-group",
		MinBytes:    10e3, // 10KB
		MaxBytes:    10e6, // 10MB
		MaxWait:     2 * time.Second,
		StartOffset: kafka.FirstOffset,
		// Increase dial timeout for connections
		Dialer: &kafka.Dialer{
			Timeout:   15 * time.Second,
			DualStack: true,
		},
	})

	log.Printf("Kafka consumer created and ready")

	return &Consumer{
		reader:   reader,
		handlers: make(map[string]func([]byte) error),
	}
}

func (c *Consumer) RegisterHandler(messageType string, handler func([]byte) error) {
	c.handlers[messageType] = handler
}

func (c *Consumer) Start(ctx context.Context) {
	log.Println("Starting Kafka consumer...")

	// Wait for the topic to be fully created by the kafka-setup service
	log.Println("Consumer waiting for 5 seconds to allow Kafka to fully initialize...")
	time.Sleep(5 * time.Second)
	log.Println("Consumer ready to receive messages")

	for {
		select {
		case <-ctx.Done():
			log.Println("Stopping Kafka consumer due to context cancellation")
			c.reader.Close()
			return
		default:
			// Process messages with simple retry logic
			readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			msg, err := c.reader.FetchMessage(readCtx)
			cancel()

			if err != nil {
				// Don't spam logs for normal timeouts when there are no messages
				if !strings.Contains(err.Error(), "context deadline exceeded") {
					log.Printf("Error reading message: %v. Will retry.", err)
				}
				time.Sleep(1 * time.Second)
				continue
			}

			// Successfully got a message, process it
			log.Printf("Received message: %s", string(msg.Value))

			// Parse message
			var messageData map[string]interface{}
			if err := json.Unmarshal(msg.Value, &messageData); err != nil {
				log.Printf("Error unmarshaling message: %v", err)
				// Still commit the message to avoid getting stuck
				if err := c.reader.CommitMessages(ctx, msg); err != nil {
					log.Printf("Error committing malformed message: %v", err)
				}
				continue
			}

			// Handle the message
			if handler := c.handlers["email"]; handler != nil {
				err := handler(msg.Value)
				if err != nil {
					log.Printf("Error handling message: %v", err)
				} else {
					log.Printf("Successfully processed message")
				}
			} else {
				log.Printf("No handler registered for email messages")
			}

			// Commit the message regardless of processing result
			if err := c.reader.CommitMessages(ctx, msg); err != nil {
				log.Printf("Error committing message: %v", err)
			} else {
				log.Printf("Successfully committed message offset")
			}
		}
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}

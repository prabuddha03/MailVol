package kafka

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
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

	// Initialize the writer with multiple partitions for scaling
	writer := &kafka.Writer{
		Addr:         kafka.TCP(broker),
		Topic:        topicName,
		Balancer:     &kafka.Hash{}, // Use hash-based routing for purpose-based partitioning
		BatchTimeout: 50 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
		Async:        true, // Use async mode for better throughput
		// Increase dial timeout for connections
		Transport: &kafka.Transport{
			DialTimeout: 15 * time.Second,
		},
		AllowAutoTopicCreation: true,
	}

	log.Printf("Kafka producer created and ready with partition-based routing")
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

	// Check if we can extract purpose_id for routing
	var purposeID string
	var priority int

	// Try to parse purpose_id from the job
	var emailJob map[string]interface{}
	if err := json.Unmarshal(value, &emailJob); err == nil {
		if pid, ok := emailJob["purpose_id"].(string); ok {
			purposeID = pid

			// Extract the priority number from purpose_id
			if len(pid) > 0 {
				// Try to parse the first character as a number
				if num, err := strconv.Atoi(string(pid[0])); err == nil {
					priority = num
				}
			}
		}
	}

	// Log routing information
	log.Printf("Sending message to Kafka with purpose_id: %s, priority: %d",
		purposeID, priority)
	log.Printf("Message content: %s", string(value))

	// Create key based on purpose_id for consistent partition routing
	// This ensures all emails with the same purpose go to the same partition
	key := []byte(purposeID)
	if len(key) == 0 {
		key = []byte("default")
	}

	// Implement retry logic for sending messages
	maxRetries := 8
	baseRetryDelay := 2 * time.Second

	for i := 0; i < maxRetries; i++ {
		// Create a timeout context for each attempt
		msgCtx, cancel := context.WithTimeout(ctx, 15*time.Second)

		// Send with key for consistent partition routing
		err = p.writer.WriteMessages(msgCtx, kafka.Message{
			Key:   key,
			Value: value,
			// Headers allow us to include the priority without changing the message format
			Headers: []kafka.Header{
				{Key: "priority", Value: []byte(strconv.Itoa(priority))},
			},
		})
		cancel()

		if err == nil {
			log.Printf("Successfully sent message to Kafka (purpose_id: %s, priority: %d)",
				purposeID, priority)
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

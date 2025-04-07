package kafka

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// Email priorities
const (
	HighPriority    = 1
	MediumPriority  = 2
	LowPriority     = 3
	LowestPriority  = 4
	DefaultPriority = 5 // For any email without a priority
)

// Worker configuration
const (
	NumWorkers          = 10  // Total number of concurrent workers
	HighPriorityRatio   = 0.5 // 50% of workers for high priority (1)
	MediumPriorityRatio = 0.3 // 30% of workers for medium priority (2)
	LowPriorityRatio    = 0.2 // 20% of workers for low priority (3,4)
)

type Consumer struct {
	reader      *kafka.Reader
	handlers    map[string]func([]byte) error
	workerPools map[int]chan kafka.Message
	priorityMap map[string]int
	wg          sync.WaitGroup
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

	// Initialize worker channels for different priorities
	workerPools := make(map[int]chan kafka.Message)

	// Calculate workers per priority
	highPriorityWorkers := int(NumWorkers * HighPriorityRatio)
	mediumPriorityWorkers := int(NumWorkers * MediumPriorityRatio)
	lowPriorityWorkers := NumWorkers - highPriorityWorkers - mediumPriorityWorkers

	// Initialize channels with appropriate buffer sizes
	workerPools[HighPriority] = make(chan kafka.Message, highPriorityWorkers*2)
	workerPools[MediumPriority] = make(chan kafka.Message, mediumPriorityWorkers*2)
	workerPools[LowPriority] = make(chan kafka.Message, lowPriorityWorkers*2)

	// Map purpose IDs to priorities
	priorityMap := make(map[string]int)
	priorityMap["1"] = HighPriority
	priorityMap["2"] = MediumPriority
	priorityMap["3"] = LowPriority
	priorityMap["4"] = LowPriority

	log.Printf("Kafka consumer created with %d workers (%d high, %d medium, %d low priority)",
		NumWorkers, highPriorityWorkers, mediumPriorityWorkers, lowPriorityWorkers)

	return &Consumer{
		reader:      reader,
		handlers:    make(map[string]func([]byte) error),
		workerPools: workerPools,
		priorityMap: priorityMap,
	}
}

func (c *Consumer) RegisterHandler(messageType string, handler func([]byte) error) {
	c.handlers[messageType] = handler
}

func (c *Consumer) extractPriority(msg kafka.Message) int {
	// First try to get priority from headers
	for _, header := range msg.Headers {
		if header.Key == "priority" {
			if p, err := strconv.Atoi(string(header.Value)); err == nil {
				return p
			}
		}
	}

	// Then try to extract from the message
	var emailData map[string]interface{}
	if err := json.Unmarshal(msg.Value, &emailData); err == nil {
		if purposeIDRaw, ok := emailData["purpose_id"]; ok {
			if purposeID, ok := purposeIDRaw.(string); ok && len(purposeID) > 0 {
				if priority, exists := c.priorityMap[purposeID]; exists {
					return priority
				}

				// Try to extract number from the beginning of purpose_id
				if num, err := strconv.Atoi(string(purposeID[0])); err == nil {
					return num
				}
			}
		}
	}

	return DefaultPriority
}

func (c *Consumer) startWorker(ctx context.Context, priority int, id int) {
	c.wg.Add(1)
	defer c.wg.Done()

	priorityName := "unknown"
	switch priority {
	case HighPriority:
		priorityName = "high"
	case MediumPriority:
		priorityName = "medium"
	case LowPriority:
		priorityName = "low"
	}

	log.Printf("Starting worker %d for %s priority messages", id, priorityName)

	// Get the channel for this priority
	ch, ok := c.workerPools[priority]
	if !ok {
		log.Printf("Error: No channel found for priority %d, worker %d exiting", priority, id)
		return
	}

	for {
		select {
		case <-ctx.Done():
			log.Printf("Worker %d for %s priority shutting down due to context cancellation",
				id, priorityName)
			return
		case msg := <-ch:
			startTime := time.Now()
			log.Printf("Worker %d processing message with priority %s", id, priorityName)

			// Handle the message
			if handler := c.handlers["email"]; handler != nil {
				if err := handler(msg.Value); err != nil {
					log.Printf("Worker %d error handling message: %v", id, err)
				} else {
					log.Printf("Worker %d successfully processed message in %.2fs",
						id, time.Since(startTime).Seconds())
				}
			} else {
				log.Printf("Worker %d: No handler registered for email messages", id)
			}

			// Commit the message
			if err := c.reader.CommitMessages(ctx, msg); err != nil {
				log.Printf("Worker %d error committing message: %v", id, err)
			}
		}
	}
}

func (c *Consumer) Start(ctx context.Context) {
	log.Println("Starting Kafka consumer with priority-based concurrent processing...")

	// Start worker goroutines for each priority
	workerID := 0

	// Calculate workers per priority
	highPriorityWorkers := int(NumWorkers * HighPriorityRatio)
	mediumPriorityWorkers := int(NumWorkers * MediumPriorityRatio)
	lowPriorityWorkers := NumWorkers - highPriorityWorkers - mediumPriorityWorkers

	// Start high priority workers
	for i := 0; i < highPriorityWorkers; i++ {
		go c.startWorker(ctx, HighPriority, workerID)
		workerID++
	}

	// Start medium priority workers
	for i := 0; i < mediumPriorityWorkers; i++ {
		go c.startWorker(ctx, MediumPriority, workerID)
		workerID++
	}

	// Start low priority workers
	for i := 0; i < lowPriorityWorkers; i++ {
		go c.startWorker(ctx, LowPriority, workerID)
		workerID++
	}

	// Main consumer loop to read from Kafka and distribute to workers
	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Println("Stopping Kafka message distributor due to context cancellation")
				c.reader.Close()

				// Close all worker channels
				for _, ch := range c.workerPools {
					close(ch)
				}

				// Wait for all workers to finish
				c.wg.Wait()
				log.Println("All workers have shut down")
				return

			default:
				// Read message from Kafka
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

				// Determine message priority
				priority := c.extractPriority(msg)

				// Get appropriate channel based on priority
				ch, ok := c.workerPools[priority]
				if !ok {
					// Fallback to low priority if not found
					ch = c.workerPools[LowPriority]
				}

				// Try to send to the worker channel without blocking
				select {
				case ch <- msg:
					// Message sent to worker successfully
				default:
					// Channel is full, log and wait
					log.Printf("Warning: Worker channel for priority %d is full, waiting...", priority)
					// Use a blocking send with timeout
					sendCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
					select {
					case ch <- msg:
						// Message sent after waiting
					case <-sendCtx.Done():
						log.Printf("Error: Timeout sending message to worker channel for priority %d", priority)
						// Commit the message anyway to avoid getting stuck
						if err := c.reader.CommitMessages(ctx, msg); err != nil {
							log.Printf("Error committing skipped message: %v", err)
						}
					}
					cancel()
				}
			}
		}
	}()
}

func (c *Consumer) Close() error {
	// Close reader
	if err := c.reader.Close(); err != nil {
		return err
	}

	// Close all worker channels
	for _, ch := range c.workerPools {
		close(ch)
	}

	// Wait for all workers to finish
	c.wg.Wait()

	return nil
}

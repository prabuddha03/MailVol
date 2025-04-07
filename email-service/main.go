package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/gorilla/mux"
	"github.com/mailvol/email-service/api"
	"github.com/mailvol/email-service/kafka"
	"github.com/mailvol/email-service/mailer"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/viper"
)

func main() {
	// Load configuration
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("./config")
	viper.AutomaticEnv()
	if err := viper.ReadInConfig(); err != nil {
		log.Printf("Warning: Error reading config file: %v, will use environment variables", err)
	}

	// Print AWS region in use
	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = viper.GetString("aws.region")
	}
	log.Printf("Using AWS region: %s", region)

	// Initialize AWS config
	log.Println("Initializing AWS config...")
	_, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(region))
	if err != nil {
		log.Fatalf("Error initializing AWS config: %v", err)
	}

	// Initialize Kafka producer
	log.Println("Initializing Kafka producer...")
	producer := kafka.NewProducer()
	defer producer.Close()

	// Initialize Kafka consumer
	log.Println("Initializing Kafka consumer...")
	consumer := kafka.NewConsumer()
	defer consumer.Close()

	// Initialize mailer
	log.Println("Initializing email sender...")
	emailSender, err := mailer.NewSender()
	if err != nil {
		log.Fatalf("Failed to initialize email sender: %v", err)
	}

	// Setup API context with dependencies
	api.Initialize(producer)

	// Initialize router
	router := mux.NewRouter()

	// Register routes
	router.HandleFunc("/send-email", api.HandleSendEmail).Methods("POST")
	router.Handle("/metrics", promhttp.Handler())

	// Create server
	srv := &http.Server{
		Addr:    ":8080",
		Handler: router,
	}

	// Start server in a goroutine
	go func() {
		log.Println("Starting server on :8080")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	// Start Kafka consumer in a goroutine
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Register message handler for email type
	consumer.RegisterHandler("email", func(msg []byte) error {
		log.Printf("Received message from Kafka: %s", string(msg))

		// Parse message
		var emailJob mailer.EmailJob
		if err := json.Unmarshal(msg, &emailJob); err != nil {
			log.Printf("Error parsing email job: %v", err)
			return err
		}

		// Send email
		if err := emailSender.SendEmail(ctx, emailJob); err != nil {
			log.Printf("Failed to send email: %v", err)
			return err
		}

		return nil
	})

	go func() {
		log.Println("Starting Kafka consumer...")
		consumer.Start(ctx)
	}()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit
	log.Printf("Received signal %s, shutting down...", sig.String())

	// Create shutdown context with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	// Attempt graceful shutdown
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Println("Server exiting")
}

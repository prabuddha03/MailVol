package api

import (
	"context"
	"encoding/json"
	"log"
	"net/http"

	"github.com/go-playground/validator/v10"
	"github.com/mailvol/email-service/kafka"
	"github.com/mailvol/email-service/mailer"
)

var (
	validate *validator.Validate
	producer *kafka.Producer
)

// Initialize configures the API package with required dependencies
func Initialize(kafkaProducer *kafka.Producer) {
	producer = kafkaProducer
	validate = validator.New()
}

type EmailRequest struct {
	From       string `json:"from" validate:"required,email"`
	To         string `json:"to" validate:"required,email"`
	Hash       string `json:"hash" validate:"required"`
	Body       string `json:"body" validate:"required"`
	TemplateID string `json:"template_id" validate:"required"`
	URL        string `json:"url" validate:"required,url"`
	PurposeID  string `json:"purpose_id" validate:"required"`
}

func HandleSendEmail(w http.ResponseWriter, r *http.Request) {
	var req EmailRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("Error decoding request: %v", err)
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Validate request
	if err := validate.Struct(req); err != nil {
		log.Printf("Validation error: %v", err)
		http.Error(w, "Invalid request: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Create email job
	job := mailer.EmailJob{
		From:       req.From,
		To:         req.To,
		Hash:       req.Hash,
		Body:       req.Body,
		TemplateID: req.TemplateID,
		URL:        req.URL,
		PurposeID:  req.PurposeID,
	}

	// Send to Kafka
	if err := producer.SendEmailJob(context.Background(), job); err != nil {
		log.Printf("Error sending to Kafka: %v", err)
		http.Error(w, "Failed to queue email", http.StatusInternalServerError)
		return
	}

	log.Printf("Email job queued successfully: to=%s, template=%s", req.To, req.TemplateID)
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{
		"status":   "Email job queued",
		"to":       req.To,
		"template": req.TemplateID,
	})
}

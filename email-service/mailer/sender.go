package mailer

import (
	"bytes"
	"context"
	"fmt"
	"html/template"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ses"
	"github.com/aws/aws-sdk-go-v2/service/ses/types"
	"github.com/mailvol/email-service/metrics"
)

type Sender struct {
	client      *ses.Client
	templates   map[string]*template.Template
	templateDir string
}

func NewSender() (*Sender, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(os.Getenv("AWS_REGION")),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Set template directory with a fallback
	templateDir := "templates"
	if _, err := os.Stat(templateDir); os.IsNotExist(err) {
		// Try absolute path for Docker environments
		templateDir = "/app/templates"
		if _, err := os.Stat(templateDir); os.IsNotExist(err) {
			log.Printf("Warning: Template directory not found at %s or %s", "templates", templateDir)
		}
	}

	log.Printf("Using template directory: %s", templateDir)

	// Pre-load templates
	templates := make(map[string]*template.Template)
	files, err := os.ReadDir(templateDir)
	if err != nil {
		log.Printf("Warning: Could not read template directory: %v", err)
	} else {
		for _, file := range files {
			if !file.IsDir() && filepath.Ext(file.Name()) == ".html" {
				templateName := file.Name()[:len(file.Name())-len(".html")]
				tmplPath := filepath.Join(templateDir, file.Name())

				tmpl, err := template.ParseFiles(tmplPath)
				if err != nil {
					log.Printf("Warning: Failed to parse template %s: %v", tmplPath, err)
					continue
				}

				templates[templateName] = tmpl
				log.Printf("Loaded template: %s", templateName)
			}
		}
	}

	client := ses.NewFromConfig(cfg)
	return &Sender{
		client:      client,
		templates:   templates,
		templateDir: templateDir,
	}, nil
}

type EmailJob struct {
	From       string
	To         string
	Hash       string
	Body       string
	TemplateID string
	URL        string
	PurposeID  string
}

func (s *Sender) SendEmail(ctx context.Context, job EmailJob) error {
	startTime := time.Now()
	log.Printf("Starting to process email to %s using template %s", job.To, job.TemplateID)

	// Get the pre-loaded template or load it on demand if not found
	tmpl, ok := s.templates[job.TemplateID]
	if !ok {
		var err error
		tmplPath := filepath.Join(s.templateDir, job.TemplateID+".html")
		tmpl, err = template.ParseFiles(tmplPath)
		if err != nil {
			log.Printf("ERROR: Failed to load template %s: %v", job.TemplateID, err)
			metrics.EmailsSentTotal.WithLabelValues("error", job.TemplateID).Inc()
			return fmt.Errorf("template error: %w", err)
		}
	}

	// Render template
	renderStart := time.Now()
	var rendered bytes.Buffer
	if err := tmpl.Execute(&rendered, job); err != nil {
		log.Printf("ERROR: Failed to render template %s: %v", job.TemplateID, err)
		metrics.EmailsSentTotal.WithLabelValues("error", job.TemplateID).Inc()
		return fmt.Errorf("render error: %w", err)
	}
	metrics.EmailRenderDuration.Observe(time.Since(renderStart).Seconds())

	// Safely create string variables for SES
	charset := "UTF-8"
	htmlContent := rendered.String()
	subject := job.PurposeID
	if subject == "" {
		subject = "Email from MailVol"
	}

	// Send email via SES
	input := &ses.SendEmailInput{
		Source: &job.From,
		Destination: &types.Destination{
			ToAddresses: []string{job.To},
		},
		Message: &types.Message{
			Subject: &types.Content{
				Data:    &subject,
				Charset: &charset,
			},
			Body: &types.Body{
				Html: &types.Content{
					Data:    &htmlContent,
					Charset: &charset,
				},
			},
		},
	}

	sendStart := time.Now()
	resp, err := s.client.SendEmail(ctx, input)
	sendDuration := time.Since(sendStart).Seconds()
	metrics.SESSendDuration.Observe(sendDuration)

	if err != nil {
		log.Printf("ERROR: Failed to send email to %s: %v", job.To, err)
		metrics.EmailsSentTotal.WithLabelValues("error", job.TemplateID).Inc()
		return fmt.Errorf("send error: %w", err)
	}

	// Log success with message ID
	msgID := ""
	if resp.MessageId != nil {
		msgID = *resp.MessageId
	}
	log.Printf("SUCCESS: Email sent to %s using template %s, Message ID: %s, Duration: %.2fs",
		job.To, job.TemplateID, msgID, time.Since(startTime).Seconds())
	metrics.EmailsSentTotal.WithLabelValues("success", job.TemplateID).Inc()

	return nil
}

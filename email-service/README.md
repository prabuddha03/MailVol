# Email Service

This service is responsible for sending emails. It receives email requests, processes them, and sends them using AWS SES. It utilizes Kafka for asynchronous message queuing and Prometheus for metrics.

## Components

- **API (`api/`)**: Exposes an HTTP endpoint (`/send-email`) to receive email requests.
  - `handler.go`: Contains the HTTP handler for the `/send-email` route.
- **Kafka (`kafka/`)**: Manages message production and consumption.
  - `producer.go`: Produces messages to a Kafka topic (`email-jobs`) when an email request is received.
  - `consumer.go`: Consumes messages from the `email-jobs` topic and processes them to send emails.
- **Mailer (`mailer/`)**: Handles the actual email sending logic using AWS SES.
  - `sender.go`: Contains the `SendEmail` function that interacts with AWS SES.
- **Templates (`templates/`)**: Stores HTML email templates.
  - `welcome.html`: An example email template.
- **Config (`config/`)**: Manages service configuration.
  - `config.yaml`: Contains configuration parameters like AWS region, Kafka brokers, etc. Environment variables can also be used and will override values in `config.yaml`.
- **Metrics (`metrics/`)**: Exposes application metrics for Prometheus.
  - `prometheus.go`: Initializes and exposes Prometheus metrics.
- **`main.go`**: The entry point of the application. It initializes all components (config, AWS, Kafka producer/consumer, mailer, API router), starts the HTTP server, and the Kafka consumer.
- **`Dockerfile`**: Defines how to build the Docker image for the service. It uses a multi-stage build to create a small, optimized image.
- **`docker-compose.yml`**: Defines the services, networks, and volumes for local development and testing. It includes:
  - `email-service`: The main application.
  - `kafka`: Kafka message broker.
  - `zookeeper`: Zookeeper for Kafka coordination.
  - `kafka-setup`: A utility service to create the `email-jobs` topic in Kafka.
  - `prometheus`: For collecting metrics.
  - `grafana`: For visualizing metrics (can be connected to Prometheus).
- **`prometheus.yml`**: Configuration for Prometheus to scrape metrics from the `email-service`.

## Functionality

1.  An HTTP POST request to `/send-email` (with a JSON payload detailing the recipient, subject, body, etc.) triggers the API handler.
2.  The API handler produces a message to the `email-jobs` Kafka topic.
3.  The Kafka consumer, running in a separate goroutine, consumes messages from this topic.
4.  For each message, the consumer parses the email job and calls the `SendEmail` function in the `mailer` component.
5.  The `mailer` uses AWS SES to send the email.
6.  The service exposes metrics on the `/metrics` endpoint, which can be scraped by Prometheus.

## Setup and Running

### Prerequisites

- Docker and Docker Compose
- AWS credentials configured (either in `config/config.yaml` or as environment variables `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`).
- An AWS SES verified email address for the `AWS_SES_FROM_EMAIL` environment variable or `aws.ses_from_email` in the config.

### Running Locally

1.  Navigate to the `email-service` directory.
2.  Ensure your AWS credentials and SES from-email are set in `docker-compose.yml` or as environment variables.
3.  Run the service using Docker Compose:
    ```bash
    docker-compose up -d
    ```
4.  The email service will be available at `http://localhost:8081`.
    - API endpoint: `POST http://localhost:8081/send-email`
    - Metrics endpoint: `http://localhost:8081/metrics`
5.  Prometheus will be available at `http://localhost:9091`.
6.  Grafana will be available at `http://localhost:3001`.

### Building the Docker Image

To build the Docker image manually:

```bash
docker build -t email-service .
```

## Configuration

Configuration is managed via `config/config.yaml` and environment variables. Environment variables take precedence.

Key configuration options:

- `aws.region`: AWS region for SES.
- `aws.ses_from_email`: The email address to send emails from (must be verified in SES).
- `kafka.brokers`: List of Kafka broker addresses.
- `kafka.topic`: Kafka topic for email jobs (default: `email-jobs`).
- `kafka.groupID`: Kafka consumer group ID.

## API Endpoint

### Send Email

- **URL**: `/send-email`
- **Method**: `POST`
- **Request Body** (JSON):
  ```json
  {
    "to": "recipient@example.com",
    "subject": "Hello from Email Service",
    "html_body": "<h1>Hello!</h1><p>This is a test email.</p>",
    "text_body": "Hello! This is a test email.",
    "template_name": "welcome.html", // Optional: if provided, html_body and text_body are ignored
    "template_data": {
      // Optional: data to pass to the template
      "name": "User"
    }
  }
  ```
- **Success Response**:
  - Code: `202 Accepted`
  - Body: `{"message": "Email request accepted"}`
- **Error Response**:
  - Code: `400 Bad Request` or `500 Internal Server Error`
  - Body: `{"error": "Error message"}`

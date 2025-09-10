package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"text/template"
	"time"

	"dbMonitor/internal/config"
	"dbMonitor/internal/monitor"
	"dbMonitor/internal/notifier"
)

// Definição do template de email para maior flexibilidade
const emailTemplate = `
DATABASE MONITORING ALERT

Database: {{.DatabaseName}}
Alert Type: {{.AlertType}}
Message: {{.Message}}
{{if .Value}}Current Value: {{.Value}}{{end}}
{{if .Threshold}}Configured Threshold: {{.Threshold}}{{end}}
Timestamp: {{.Timestamp}}

This is an automated alert from the database monitoring system.
Please check the database status immediately.

Connection Pool Information:
- Pool connections are managed automatically
- Unhealthy connections are automatically recreated
- Health checks run every {{.HealthCheckInterval}} seconds
`

func main() {
	// Load configuration
	cfg, err := config.Load("config.yaml")
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Initialize notifiers
	var notifiers []notifier.Notifier
	emailNotifier := notifier.NewEmailNotifier(cfg.Email)
	notifiers = append(notifiers, emailNotifier)

	if cfg.Slack.WebhookURL != "" {
		slackNotifier, err := notifier.NewSlackNotifier(cfg.Slack)
		if err == nil {
			notifiers = append(notifiers, slackNotifier)
		} else {
			log.Printf("Warning: Failed to initialize Slack notifier: %v", err)
		}
	}
	multiNotifier := notifier.NewMultiNotifier(notifiers...)

	// Test email connection
	if err := emailNotifier.TestConnection(); err != nil {
		log.Printf("Warning: Email connection test failed: %v", err)
	} else {
		log.Println("Email connection test successful")
	}

	// Initialize database monitor with connection pool
	dbMonitor := monitor.NewDatabaseMonitor(cfg, multiNotifier)
	defer dbMonitor.Close()

	// Setup context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Received interrupt signal, shutting down gracefully...")
		cancel()
	}()

	// Start HTTP server for monitoring endpoints
	go startHTTPServer(dbMonitor, cfg.Application.HTTPServerAddress)

	log.Println("Starting database monitoring with connection pooling...")

	// Initial check
	if err := dbMonitor.CheckAllInstances(ctx); err != nil {
		log.Printf("Initial check completed with errors: %v", err)
	}

	// Setup monitoring ticker
	monitoringTicker := time.NewTicker(time.Duration(cfg.Application.MonitoringInterval) * time.Second)
	defer monitoringTicker.Stop()

	// Setup health check ticker
	healthTicker := time.NewTicker(time.Duration(cfg.Application.HealthCheckInterval) * time.Second)
	defer healthTicker.Stop()

	// Setup alert reset ticker
	alertResetTicker := time.NewTicker(time.Duration(cfg.Application.AlertResetInterval) * time.Second)
	defer alertResetTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Database monitoring stopped")
			return

		case <-monitoringTicker.C:
			if err := dbMonitor.CheckAllInstances(ctx); err != nil {
				log.Printf("Monitoring check completed with errors: %v", err)
			}

		case <-healthTicker.C:
			log.Println("Performing connection pool health check...")
			healthResults := dbMonitor.HealthCheck(ctx)
			healthyCount := 0
			for name, err := range healthResults {
				if err == nil {
					healthyCount++
				} else {
					log.Printf("Database %s is unhealthy: %v", name, err)
				}
			}
			log.Printf("Health check complete: %d/%d databases healthy",
				healthyCount, len(healthResults))

		case <-alertResetTicker.C:
			log.Println("Resetting alert counts...")
			dbMonitor.ResetAlertCounts()
		}
	}
}

func startHTTPServer(dbMonitor *monitor.DatabaseMonitor, address string) {
	mux := http.NewServeMux()

	// Health endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()

		healthResults := dbMonitor.HealthCheck(ctx)

		response := map[string]interface{}{
			"status":    "ok",
			"timestamp": time.Now().Format(time.RFC3339),
			"databases": healthResults,
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	})

	// Stats endpoint
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		stats := dbMonitor.GetLastStats()

		response := map[string]interface{}{
			"timestamp": time.Now().Format(time.RFC3339),
			"stats":     stats,
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	})

	// Pool stats endpoint
	mux.HandleFunc("/pool-stats", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()

		poolStats, err := dbMonitor.GetPoolStats(ctx)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		response := map[string]interface{}{
			"timestamp":  time.Now().Format(time.RFC3339),
			"pool_stats": poolStats,
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	})

	// Alert counts endpoint
	mux.HandleFunc("/alert-counts", func(w http.ResponseWriter, r *http.Request) {
		alertCounts := dbMonitor.GetAlertCounts()

		response := map[string]interface{}{
			"timestamp":    time.Now().Format(time.RFC3339),
			"alert_counts": alertCounts,
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	})

	// Reset alerts endpoint (POST only)
	mux.HandleFunc("/reset-alerts", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		dbMonitor.ResetAlertCounts()

		response := map[string]interface{}{
			"status":    "success",
			"message":   "Alert counts reset",
			"timestamp": time.Now().Format(time.RFC3339),
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	})

	server := &http.Server{
		Addr:         address,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	log.Printf("Starting HTTP server on %s...", address)
	log.Println("Available endpoints:")
	log.Println("  GET  /health      - Database health check")
	log.Println("  GET  /stats       - Last session statistics")
	log.Println("  GET  /pool-stats  - Connection pool statistics")
	log.Println("  GET  /alert-counts - Alert counts")
	log.Println("  POST /reset-alerts - Reset alert counts")

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("HTTP server error: %v", err)
	}
}

// main.go (Updated with enhanced features)
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

	"dbMonitor/internal/config"
	"dbMonitor/internal/monitor"
	"dbMonitor/internal/notifier"
)

func main() {
	// Load configuration
	cfg, err := config.Load("config.yaml")
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Initialize email notifier
	emailNotifier := notifier.NewEmailNotifier(cfg.Email)

	// Test email connection
	if err := emailNotifier.TestConnection(); err != nil {
		log.Printf("Warning: Email connection test failed: %v", err)
	} else {
		log.Println("Email connection test successful")
	}

	// Initialize database monitor with connection pool
	dbMonitor := monitor.NewDatabaseMonitor(cfg, emailNotifier)
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
	go startHTTPServer(dbMonitor)

	log.Println("Starting database monitoring with connection pooling...")

	// Initial check
	if err := dbMonitor.CheckAllInstances(ctx); err != nil {
		log.Printf("Initial check completed with errors: %v", err)
	}

	// Setup monitoring ticker
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	// Setup health check ticker (every 5 minutes)
	healthTicker := time.NewTicker(5 * time.Minute)
	defer healthTicker.Stop()

	// Setup alert reset ticker (every hour)
	alertResetTicker := time.NewTicker(1 * time.Hour)
	defer alertResetTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Database monitoring stopped")
			return

		case <-ticker.C:
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

func startHTTPServer(dbMonitor *monitor.DatabaseMonitor) {
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
		Addr:         ":8080",
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	log.Println("Starting HTTP server on :8080...")
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

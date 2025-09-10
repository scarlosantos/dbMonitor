package monitor

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"dbMonitor/internal/config"
	"dbMonitor/internal/database"
	"dbMonitor/internal/notifier"
	"golang.org/x/sync/errgroup"
)

type DatabaseMonitor struct {
	config      *config.Config
	pool        *database.Pool
	notifier    notifier.Notifier
	mu          sync.RWMutex
	lastStats   map[string]*database.SessionStats
	alertCounts map[string]int
}

type Alert struct {
	DatabaseName string
	AlertType    string
	Message      string
	Value        int
	Threshold    int
	Timestamp    time.Time
}

func NewDatabaseMonitor(cfg *config.Config, notifier notifier.Notifier) *DatabaseMonitor {
	pool := database.NewPool(cfg.Pool)

	monitor := &DatabaseMonitor{
		config:      cfg,
		pool:        pool,
		notifier:    notifier,
		lastStats:   make(map[string]*database.SessionStats),
		alertCounts: make(map[string]int),
	}

	go pool.StartHealthCheckRoutine(context.Background())

	return monitor
}

func (dm *DatabaseMonitor) CheckAllInstances(ctx context.Context) error {
	var g errgroup.Group
	var mu sync.Mutex
	var errors []error

	for _, dbConfig := range dm.config.Databases {
		cfg := dbConfig // Captura a variável de loop para a goroutine
		g.Go(func() error {
			err := dm.checkInstance(ctx, cfg)
			if err != nil {
				mu.Lock()
				errors = append(errors, fmt.Errorf("failed to check %s: %w", cfg.Name, err))
				mu.Unlock()
			}
			return err
		})
	}

	g.Wait()

	if len(errors) > 0 {
		log.Printf("Encountered %d errors during instance checks", len(errors))
		for _, err := range errors {
			log.Printf("Instance check error: %v", err)
		}
		return fmt.Errorf("encountered %d errors during monitoring", len(errors))
	}

	return nil
}

func (dm *DatabaseMonitor) checkInstance(ctx context.Context, cfg config.DatabaseConfig) error {
	conn, err := dm.pool.GetConnection(cfg)
	if err != nil {
		log.Printf("Failed to get connection for %s: %v", cfg.Name, err)
		dm.sendAlert(Alert{
			DatabaseName: cfg.Name,
			AlertType:    "CONNECTION_ERROR",
			Message:      fmt.Sprintf("Failed to establish connection: %v", err),
			Timestamp:    time.Now(),
		})
		return err
	}

	statsCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()

	stats, err := conn.GetSessionStats(statsCtx)
	if err != nil {
		log.Printf("Failed to get statistics for %s: %v", cfg.Name, err)
		dm.sendAlert(Alert{
			DatabaseName: cfg.Name,
			AlertType:    "QUERY_ERROR",
			Message:      fmt.Sprintf("Failed to query statistics: %v", err),
			Timestamp:    time.Now(),
		})
		return err
	}

	dm.mu.Lock()
	dm.lastStats[cfg.Name] = stats
	dm.mu.Unlock()

	log.Printf("DB: %s | Total: %d | Active: %d | Inactive: %d | Idle: %d | Waiting: %d",
		stats.DatabaseName, stats.Total, stats.Active, stats.Inactive, stats.Idle, stats.Waiting)

	dm.checkThresholds(stats)

	return nil
}

func (dm *DatabaseMonitor) checkThresholds(stats *database.SessionStats) {
	thresholds := dm.config.Thresholds
	alertKey := stats.DatabaseName

	if stats.Active > thresholds.ActiveConnections {
		if dm.shouldSendAlert(alertKey, "HIGH_ACTIVE_CONNECTIONS") {
			dm.sendAlert(Alert{
				DatabaseName: stats.DatabaseName,
				AlertType:    "HIGH_ACTIVE_CONNECTIONS",
				Message:      "High number of active connections detected",
				Value:        stats.Active,
				Threshold:    thresholds.ActiveConnections,
				Timestamp:    time.Now(),
			})
		}
	}

	if stats.Inactive > thresholds.InactiveConnections {
		if dm.shouldSendAlert(alertKey, "HIGH_INACTIVE_CONNECTIONS") {
			dm.sendAlert(Alert{
				DatabaseName: stats.DatabaseName,
				AlertType:    "HIGH_INACTIVE_CONNECTIONS",
				Message:      "High number of inactive connections detected",
				Value:        stats.Inactive,
				Threshold:    thresholds.InactiveConnections,
				Timestamp:    time.Now(),
			})
		}
	}

	if stats.Total > thresholds.TotalConnections {
		if dm.shouldSendAlert(alertKey, "HIGH_TOTAL_CONNECTIONS") {
			if dm.shouldSendAlert(alertKey, "HIGH_TOTAL_CONNECTIONS") {
				dm.sendAlert(Alert{
					DatabaseName: stats.DatabaseName,
					AlertType:    "HIGH_TOTAL_CONNECTIONS",
					Message:      "High total number of connections detected",
					Value:        stats.Total,
					Threshold:    thresholds.TotalConnections,
					Timestamp:    time.Now(),
				})
			}
		}
	}
}

func (dm *DatabaseMonitor) shouldSendAlert(databaseName, alertType string) bool {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	key := fmt.Sprintf("%s_%s", databaseName, alertType)
	count := dm.alertCounts[key]
	frequency := dm.config.Application.AlertFrequency

	if count == 0 || (frequency > 0 && count%frequency == 0) {
		dm.alertCounts[key] = count + 1
		return true
	}

	dm.alertCounts[key] = count + 1
	return false
}

func (dm *DatabaseMonitor) sendAlert(alert Alert) {
	subject := fmt.Sprintf("DB Monitor ALERT: %s - %s", alert.DatabaseName, alert.AlertType)

	var body string
	if alert.Value > 0 && alert.Threshold > 0 {
		body = fmt.Sprintf(`
DATABASE MONITORING ALERT

Database: %s
Alert Type: %s
Message: %s
Current Value: %d
Configured Threshold: %d
Timestamp: %s

This is an automated alert from the database monitoring system.
Please check the database status immediately.

Connection Pool Information:
- Pool connections are managed automatically
- Unhealthy connections are automatically recreated
- Health checks run every %d seconds
		`, alert.DatabaseName, alert.AlertType, alert.Message,
			alert.Value, alert.Threshold, alert.Timestamp.Format("2006-01-02 15:04:05"), dm.config.Pool.HealthCheckInterval)
	} else {
		body = fmt.Sprintf(`
DATABASE MONITORING ALERT

Database: %s
Alert Type: %s
Message: %s
Timestamp: %s

This is an automated alert from the database monitoring system.
Please check the database status immediately.

Connection Pool Information:
- Pool connections are managed automatically
- Unhealthy connections are automatically recreated
- Health checks run every %d seconds
		`, alert.DatabaseName, alert.AlertType, alert.Message,
			alert.Timestamp.Format("2006-01-02 15:04:05"), dm.config.Pool.HealthCheckInterval)
	}

	if err := dm.notifier.SendAlert(subject, body); err != nil {
		log.Printf("Failed to send alert for %s: %v", alert.DatabaseName, err)
	} else {
		log.Printf("Alert sent for %s: %s", alert.DatabaseName, alert.AlertType)
	}
}

func (dm *DatabaseMonitor) GetLastStats() map[string]*database.SessionStats {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	stats := make(map[string]*database.SessionStats)
	for k, v := range dm.lastStats {
		statsCopy := *v
		stats[k] = &statsCopy
	}

	return stats
}

func (dm *DatabaseMonitor) GetPoolStats(ctx context.Context) (map[string]*database.PoolStats, error) {
	return dm.pool.GetAllStats(ctx)
}

func (dm *DatabaseMonitor) HealthCheck(ctx context.Context) map[string]error {
	return dm.pool.HealthCheck(ctx)
}

func (dm *DatabaseMonitor) ResetAlertCounts() {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	dm.alertCounts = make(map[string]int)
	log.Println("Alert counts reset")
}

func (dm *DatabaseMonitor) GetAlertCounts() map[string]int {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	counts := make(map[string]int)
	for k, v := range dm.alertCounts {
		counts[k] = v
	}
	return counts
}

func (dm *DatabaseMonitor) Close() error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	if err := dm.pool.Close(); err != nil {
		log.Printf("Error closing connection pool: %v", err)
		return err
	}

	dm.lastStats = make(map[string]*database.SessionStats)
	dm.alertCounts = make(map[string]int)

	log.Println("Database monitor closed successfully")
	return nil
}

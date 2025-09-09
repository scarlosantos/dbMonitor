// internal/database/postgresql.go
package database

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"dbMonitor/internal/config"
	_ "github.com/lib/pq"
)

type PostgreSQLStatsProvider struct{}

func NewPostgreSQLStatsProvider() *PostgreSQLStatsProvider {
	return &PostgreSQLStatsProvider{}
}

func (p *PostgreSQLStatsProvider) GetSessionStats(ctx context.Context, db *sql.DB) (*SessionStats, error) {
	query := `
		SELECT 
			COALESCE(SUM(CASE WHEN state = 'active' THEN 1 ELSE 0 END), 0) as active,
			COALESCE(SUM(CASE WHEN state = 'idle' THEN 1 ELSE 0 END), 0) as idle,
			COALESCE(SUM(CASE WHEN state = 'idle in transaction' THEN 1 ELSE 0 END), 0) as idle_in_txn,
			COALESCE(SUM(CASE WHEN wait_event IS NOT NULL THEN 1 ELSE 0 END), 0) as waiting,
			COALESCE(COUNT(*), 0) as total
		FROM pg_stat_activity 
		WHERE pid != pg_backend_pid()
		AND state IS NOT NULL
	`

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var stats SessionStats
	var active, idle, idleInTxn, waiting, total int

	err := db.QueryRowContext(ctx, query).Scan(&active, &idle, &idleInTxn, &waiting, &total)
	if err != nil {
		return nil, fmt.Errorf("failed to query PostgreSQL statistics: %w", err)
	}

	stats.Active = active
	stats.Idle = idle
	stats.IdleInTxn = idleInTxn
	stats.Waiting = waiting
	stats.Total = total
	stats.Inactive = idle + idleInTxn

	return &stats, nil
}

func connectPostgreSQL(cfg config.DatabaseConfig) (*sql.DB, error) {
	// Build connection string with proper error handling
	connStr := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=%s connect_timeout=30",
		cfg.Host, cfg.Port, cfg.Database, cfg.Username, cfg.Password, cfg.SSLMode)

	// Add SSL certificate files if provided
	if cfg.CertPath != "" {
		if err := validatePostgreSQLCertFiles(cfg.CertPath); err != nil {
			return nil, fmt.Errorf("certificate validation failed: %w", err)
		}

		certFile := filepath.Join(cfg.CertPath, "client-cert.pem")
		keyFile := filepath.Join(cfg.CertPath, "client-key.pem")
		caFile := filepath.Join(cfg.CertPath, "ca-cert.pem")

		connStr += fmt.Sprintf(" sslcert=%s sslkey=%s sslrootcert=%s",
			certFile, keyFile, caFile)
	}

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open PostgreSQL connection: %w", err)
	}

	// Test the connection with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("PostgreSQL connection test failed: %w", err)
	}

	return db, nil
}

func validatePostgreSQLCertFiles(certPath string) error {
	certFile := filepath.Join(certPath, "client-cert.pem")
	keyFile := filepath.Join(certPath, "client-key.pem")
	caFile := filepath.Join(certPath, "ca-cert.pem")

	// Check if certificate files exist and are readable
	files := map[string]string{
		"client certificate": certFile,
		"client key":         keyFile,
		"CA certificate":     caFile,
	}

	for fileType, filePath := range files {
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			return fmt.Errorf("%s file not found: %s", fileType, filePath)
		}

		// Check if file is readable
		file, err := os.Open(filePath)
		if err != nil {
			return fmt.Errorf("cannot read %s file %s: %w", fileType, filePath, err)
		}
		file.Close()
	}

	return nil
}

// GetExtendedStats provides additional PostgreSQL-specific statistics
func (p *PostgreSQLStatsProvider) GetExtendedStats(ctx context.Context, db *sql.DB) (map[string]interface{}, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	stats := make(map[string]interface{})

	// Get database size
	var dbSize int64
	err := db.QueryRowContext(ctx, "SELECT pg_database_size(current_database())").Scan(&dbSize)
	if err != nil {
		return nil, fmt.Errorf("failed to get database size: %w", err)
	}
	stats["database_size_bytes"] = dbSize

	// Get connection limits
	var maxConnections int
	err = db.QueryRowContext(ctx, "SHOW max_connections").Scan(&maxConnections)
	if err != nil {
		return nil, fmt.Errorf("failed to get max connections: %w", err)
	}
	stats["max_connections"] = maxConnections

	// Get current connection count by state
	query := `
		SELECT 
			state,
			COUNT(*) as count
		FROM pg_stat_activity 
		WHERE pid != pg_backend_pid()
		AND state IS NOT NULL
		GROUP BY state
	`

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection states: %w", err)
	}
	defer rows.Close()

	stateStats := make(map[string]int)
	for rows.Next() {
		var state string
		var count int
		if err := rows.Scan(&state, &count); err != nil {
			return nil, fmt.Errorf("failed to scan connection state row: %w", err)
		}
		stateStats[state] = count
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating connection state rows: %w", err)
	}

	stats["connection_states"] = stateStats

	return stats, nil
}

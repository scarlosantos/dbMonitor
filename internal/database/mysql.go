package database

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"dbMonitor/internal/config"
	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
)

type MySQLStatsProvider struct{}

func NewMySQLStatsProvider() *MySQLStatsProvider {
	return &MySQLStatsProvider{}
}

func (m *MySQLStatsProvider) GetSessionStats(ctx context.Context, db *sql.DB) (*SessionStats, error) {
	query := `
		SELECT 
			COALESCE(SUM(CASE WHEN command = 'Sleep' THEN 1 ELSE 0 END), 0) as idle,
			COALESCE(SUM(CASE WHEN command != 'Sleep' AND state != '' THEN 1 ELSE 0 END), 0) as active,
			COALESCE(SUM(CASE WHEN state LIKE '%Waiting%' THEN 1 ELSE 0 END), 0) as waiting,
			COALESCE(COUNT(*), 0) as total
		FROM information_schema.processlist 
		WHERE id != CONNECTION_ID()
	`

	queryCtx, cancel := context.WithTimeout(ctx, time.Duration(cfg.QueryTimeout)*time.Second)
	defer cancel()

	var stats SessionStats
	var idle, active, waiting, total int

	err := db.QueryRowContext(queryCtx, query).Scan(&idle, &active, &waiting, &total)
	if err != nil {
		return nil, fmt.Errorf("failed to query MySQL statistics: %w", err)
	}

	stats.Idle = idle
	stats.Active = active
	stats.Waiting = waiting
	stats.Total = total
	stats.Inactive = idle
	stats.IdleInTxn = 0

	return &stats, nil
}

func connectMySQL(cfg config.DatabaseConfig) (*sql.DB, error) {
	if cfg.CertPath != "" {
		tlsConfig, err := loadTLSConfig(cfg.CertPath)
		if err != nil {
			return nil, fmt.Errorf("failed to configure TLS: %w", err)
		}
		mysql.RegisterTLSConfig("custom", tlsConfig)
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?tls=%s&timeout=%ds&readTimeout=%ds&writeTimeout=%ds&parseTime=true",
		cfg.Username, cfg.Password, cfg.Host, cfg.Port, cfg.Database,
		getMySQLTLSMode(cfg.SSLMode, cfg.CertPath),
		cfg.ConnectTimeout, cfg.QueryTimeout, cfg.QueryTimeout)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open MySQL connection: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.ConnectTimeout)*time.Second)
	defer cancel()

	if err := db.PingContext(pingCtx); err != nil {
		db.Close()
		return nil, fmt.Errorf("MySQL connection test failed: %w", err)
	}

	return db, nil
}

func getMySQLTLSMode(sslMode, certPath string) string {
	if certPath != "" {
		return "custom"
	}

	switch sslMode {
	case "REQUIRED", "require":
		return "true"
	case "DISABLED", "disable":
		return "false"
	case "PREFERRED", "preferred":
		return "preferred"
	default:
		return "preferred"
	}
}

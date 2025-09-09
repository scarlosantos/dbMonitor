package database

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
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

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var stats SessionStats
	var idle, active, waiting, total int

	err := db.QueryRowContext(ctx, query).Scan(&idle, &active, &waiting, &total)
	if err != nil {
		return nil, fmt.Errorf("failed to query MySQL statistics: %w", err)
	}

	stats.Idle = idle
	stats.Active = active
	stats.Waiting = waiting
	stats.Total = total
	stats.Inactive = idle
	stats.IdleInTxn = 0 // MySQL doesn't have this concept like PostgreSQL

	return &stats, nil
}

func connectMySQL(cfg config.DatabaseConfig) (*sql.DB, error) {
	// Configure TLS if certificate path is provided
	if cfg.CertPath != "" {
		tlsConfig, err := loadMySQLTLSConfig(cfg.CertPath)
		if err != nil {
			return nil, fmt.Errorf("failed to configure TLS: %w", err)
		}
		mysql.RegisterTLSConfig("custom", tlsConfig)
	}

	// Build DSN with proper error handling
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?tls=%s&timeout=30s&readTimeout=30s&writeTimeout=30s&parseTime=true",
		cfg.Username, cfg.Password, cfg.Host, cfg.Port, cfg.Database,
		getMySQLTLSMode(cfg.SSLMode, cfg.CertPath))

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open MySQL connection: %w", err)
	}

	// Test the connection with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("MySQL connection test failed: %w", err)
	}

	return db, nil
}

func loadMySQLTLSConfig(certPath string) (*tls.Config, error) {
	certFile := filepath.Join(certPath, "client-cert.pem")
	keyFile := filepath.Join(certPath, "client-key.pem")
	caFile := filepath.Join(certPath, "ca-cert.pem")

	// Verify files exist
	if _, err := os.Stat(certFile); os.IsNotExist(err) {
		return nil, fmt.Errorf("client certificate file not found: %s", certFile)
	}
	if _, err := os.Stat(keyFile); os.IsNotExist(err) {
		return nil, fmt.Errorf("client key file not found: %s", keyFile)
	}
	if _, err := os.Stat(caFile); os.IsNotExist(err) {
		return nil, fmt.Errorf("CA certificate file not found: %s", caFile)
	}

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load client certificate: %w", err)
	}

	caCert, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA certificate: %w", err)
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to parse CA certificate")
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
		ServerName:   "", // Will be set automatically
	}, nil
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

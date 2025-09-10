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

func (m *MySQLStatsProvider) GetSessionStats(ctx context.Context, db *sql.DB, queryTimeout int) (*SessionStats, error) {
	query := `
		SELECT 
			COALESCE(SUM(CASE WHEN command = 'Sleep' THEN 1 ELSE 0 END), 0) as idle,
			COALESCE(SUM(CASE WHEN command != 'Sleep' AND state != '' THEN 1 ELSE 0 END), 0) as active,
			COALESCE(SUM(CASE WHEN state LIKE '%Waiting%' THEN 1 ELSE 0 END), 0) as waiting,
			COALESCE(COUNT(*), 0) as total
		FROM information_schema.processlist 
		WHERE id != CONNECTION_ID()
	`

	queryCtx, cancel := context.WithTimeout(ctx, time.Duration(queryTimeout)*time.Second)
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
	var tlsConfig *mysql.TLSConfig
	var err error

	if cfg.CertPath != "" {
		tlsConfig, err = loadMySQLTLSConfig(cfg.CertPath)
		if err != nil {
			return nil, fmt.Errorf("failed to load MySQL TLS config: %w", err)
		}
	}

	mysqlCfg := mysql.NewConfig()
	mysqlCfg.User = cfg.Username
	mysqlCfg.Passwd = cfg.Password
	mysqlCfg.Net = "tcp"
	mysqlCfg.Addr = fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	mysqlCfg.DBName = cfg.Database
	mysqlCfg.Timeout = time.Duration(cfg.ConnectTimeout) * time.Second
	mysqlCfg.ReadTimeout = time.Duration(cfg.QueryTimeout) * time.Second
	mysqlCfg.WriteTimeout = time.Duration(cfg.QueryTimeout) * time.Second
	mysqlCfg.ParseTime = true

	switch cfg.SSLMode {
	case "REQUIRED", "require":
		mysqlCfg.TLSConfig = "true"
	case "DISABLED", "disable":
		mysqlCfg.TLSConfig = "false"
	case "PREFERRED", "preferred":
		mysqlCfg.TLSConfig = "preferred"
	default:
		mysqlCfg.TLSConfig = "preferred"
	}

	if tlsConfig != nil {
		mysqlCfg.TLSConfig = "custom"
		mysqlCfg.TLS = tlsConfig
	}

	dsn := mysqlCfg.FormatDSN()

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

func loadMySQLTLSConfig(certPath string) (*mysql.TLSConfig, error) {
	if err := validateTLSCertFiles(certPath); err != nil {
		return nil, fmt.Errorf("certificate validation failed: %w", err)
	}

	cert, err := tls.LoadX509KeyPair(
		filepath.Join(certPath, "client-cert.pem"),
		filepath.Join(certPath, "client-key.pem"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load client key pair: %w", err)
	}

	caCert, err := os.ReadFile(filepath.Join(certPath, "ca-cert.pem"))
	if err != nil {
		return nil, fmt.Errorf("failed to read CA cert file: %w", err)
	}

	rootCertPool := x509.NewCertPool()
	if ok := rootCertPool.AppendCertsFromPEM(caCert); !ok {
		return nil, fmt.Errorf("failed to append CA cert")
	}

	return &mysql.TLSConfig{
		Certificates: []tls.Certificate{cert},
		RootCAs:      rootCertPool,
	}, nil
}

// internal/database/connection.go
package database

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	"dbMonitor/internal/config"
	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
)

type Connection struct {
	db     *sql.DB
	config config.DatabaseConfig
}

type SessionStats struct {
	Active       int
	Inactive     int
	Idle         int
	IdleInTxn    int
	Waiting      int
	Total        int
	DatabaseName string
	Timestamp    string
}

func NewConnection(cfg config.DatabaseConfig) (*Connection, error) {
	var db *sql.DB
	var err error

	switch cfg.Type {
	case "mysql":
		db, err = connectMySQL(cfg)
	case "postgresql":
		db, err = connectPostgreSQL(cfg)
	default:
		return nil, fmt.Errorf("tipo de banco não suportado: %s", cfg.Type)
	}

	if err != nil {
		return nil, fmt.Errorf("erro ao conectar com %s: %w", cfg.Name, err)
	}

	return &Connection{
		db:     db,
		config: cfg,
	}, nil
}

func (c *Connection) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

func (c *Connection) GetSessionStats(ctx context.Context) (*SessionStats, error) {
	switch c.config.Type {
	case "mysql":
		return c.getMySQLSessionStats(ctx)
	case "postgresql":
		return c.getPostgreSQLSessionStats(ctx)
	default:
		return nil, fmt.Errorf("tipo de banco não suportado: %s", c.config.Type)
	}
}

func connectMySQL(cfg config.DatabaseConfig) (*sql.DB, error) {
	if cfg.CertPath != "" {
		tlsConfig, err := loadMySQLTLSConfig(cfg.CertPath)
		if err != nil {
			return nil, fmt.Errorf("erro ao configurar TLS: %w", err)
		}
		mysql.RegisterTLSConfig("custom", tlsConfig)
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?tls=%s&timeout=30s",
		cfg.Username, cfg.Password, cfg.Host, cfg.Port, cfg.Database,
		getTLSMode(cfg.SSLMode, cfg.CertPath))

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

func connectPostgreSQL(cfg config.DatabaseConfig) (*sql.DB, error) {
	connStr := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=%s connect_timeout=30",
		cfg.Host, cfg.Port, cfg.Database, cfg.Username, cfg.Password, cfg.SSLMode)

	if cfg.CertPath != "" {
		certFile := filepath.Join(cfg.CertPath, "client-cert.pem")
		keyFile := filepath.Join(cfg.CertPath, "client-key.pem")
		caFile := filepath.Join(cfg.CertPath, "ca-cert.pem")

		connStr += fmt.Sprintf(" sslcert=%s sslkey=%s sslrootcert=%s",
			certFile, keyFile, caFile)
	}

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

func (c *Connection) getMySQLSessionStats(ctx context.Context) (*SessionStats, error) {
	query := `
		SELECT 
			COALESCE(SUM(CASE WHEN command = 'Sleep' THEN 1 ELSE 0 END), 0) as idle,
			COALESCE(SUM(CASE WHEN command != 'Sleep' AND state != '' THEN 1 ELSE 0 END), 0) as active,
			COALESCE(SUM(CASE WHEN state LIKE '%Waiting%' THEN 1 ELSE 0 END), 0) as waiting,
			COALESCE(COUNT(*), 0) as total
		FROM information_schema.processlist 
		WHERE id != CONNECTION_ID()
	`

	var stats SessionStats
	var idle, active, waiting, total int

	err := c.db.QueryRowContext(ctx, query).Scan(&idle, &active, &waiting, &total)
	if err != nil {
		return nil, fmt.Errorf("erro ao consultar estatísticas MySQL: %w", err)
	}

	stats.Idle = idle
	stats.Active = active
	stats.Waiting = waiting
	stats.Total = total
	stats.Inactive = idle
	stats.DatabaseName = c.config.Name

	return &stats, nil
}

func (c *Connection) getPostgreSQLSessionStats(ctx context.Context) (*SessionStats, error) {
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

	var stats SessionStats
	var active, idle, idleInTxn, waiting, total int

	err := c.db.QueryRowContext(ctx, query).Scan(&active, &idle, &idleInTxn, &waiting, &total)
	if err != nil {
		return nil, fmt.Errorf("erro ao consultar estatísticas PostgreSQL: %w", err)
	}

	stats.Active = active
	stats.Idle = idle
	stats.IdleInTxn = idleInTxn
	stats.Waiting = waiting
	stats.Total = total
	stats.Inactive = idle + idleInTxn
	stats.DatabaseName = c.config.Name

	return &stats, nil
}

func loadMySQLTLSConfig(certPath string) (*tls.Config, error) {
	certFile := filepath.Join(certPath, "client-cert.pem")
	keyFile := filepath.Join(certPath, "client-key.pem")
	caFile := filepath.Join(certPath, "ca-cert.pem")

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("erro ao carregar certificado do cliente: %w", err)
	}

	caCert, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("erro ao carregar CA: %w", err)
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
	}, nil
}

func getTLSMode(sslMode, certPath string) string {
	if certPath != "" {
		return "custom"
	}

	switch sslMode {
	case "REQUIRED", "require":
		return "true"
	case "DISABLED", "disable":
		return "false"
	default:
		return "preferred"
	}
}

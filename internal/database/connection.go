package database

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"dbMonitor/internal/config"
)

type Connection struct {
	db     *sql.DB
	config config.DatabaseConfig
	stats  StatsProvider
}

type StatsProvider interface {
	GetSessionStats(ctx context.Context, db *sql.DB) (*SessionStats, error)
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
	var stats StatsProvider
	var err error

	switch cfg.Type {
	case "mysql":
		db, err = connectMySQL(cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to MySQL %s: %w", cfg.Name, err)
		}
		stats = NewMySQLStatsProvider()

	case "postgresql":
		db, err = connectPostgreSQL(cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to PostgreSQL %s: %w", cfg.Name, err)
		}
		stats = NewPostgreSQLStatsProvider()

	default:
		return nil, fmt.Errorf("unsupported database type: %s", cfg.Type)
	}

	if err := configureConnectionPool(db, cfg.Type); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to configure connection pool for %s: %w", cfg.Name, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("connection test failed for %s: %w", cfg.Name, err)
	}

	return &Connection{
		db:     db,
		config: cfg,
		stats:  stats,
	}, nil
}

func (c *Connection) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

func (c *Connection) GetSessionStats(ctx context.Context) (*SessionStats, error) {
	if err := c.db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("database connection unhealthy for %s: %w", c.config.Name, err)
	}

	stats, err := c.stats.GetSessionStats(ctx, c.db)
	if err != nil {
		return nil, fmt.Errorf("failed to get session stats for %s: %w", c.config.Name, err)
	}

	stats.DatabaseName = c.config.Name
	stats.Timestamp = time.Now().Format("2006-01-02 15:04:05")

	return stats, nil
}

func (c *Connection) IsHealthy(ctx context.Context) error {
	if c.db == nil {
		return fmt.Errorf("database connection is nil")
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := c.db.PingContext(pingCtx); err != nil {
		return fmt.Errorf("database ping failed: %w", err)
	}

	return nil
}

func (c *Connection) GetDBStats() sql.DBStats {
	return c.db.Stats()
}

func configureConnectionPool(db *sql.DB, dbType string) error {
	db.SetMaxOpenConns(3)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(time.Hour)
	db.SetConnMaxIdleTime(10 * time.Minute)

	switch dbType {
	case "mysql":
		db.SetMaxOpenConns(3)
		db.SetMaxIdleConns(1)
	case "postgresql":
		db.SetMaxOpenConns(3)
		db.SetMaxIdleConns(1)
	}

	return nil
}

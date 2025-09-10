package database

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	"dbMonitor/internal/config"
)

type Pool struct {
	connections map[string]*Connection
	mu          sync.RWMutex
	poolCfg     config.PoolConfig
}

type PoolStats struct {
	DatabaseName     string                 `json:"database_name"`
	OpenConnections  int                    `json:"open_connections"`
	IdleConnections  int                    `json:"idle_connections"`
	InUseConnections int                    `json:"in_use_connections"`
	MaxConnections   int                    `json:"max_connections"`
	TotalQueries     int64                  `json:"total_queries"`
	ConnectionStats  sql.DBStats            `json:"connection_stats"`
	LastHealthCheck  time.Time              `json:"last_health_check"`
	IsHealthy        bool                   `json:"is_healthy"`
	Extended         map[string]interface{} `json:"extended,omitempty"`
}

func NewPool(poolCfg config.PoolConfig) *Pool {
	return &Pool{
		connections: make(map[string]*Connection),
		poolCfg:     poolCfg,
	}
}

func (p *Pool) GetConnection(cfg config.DatabaseConfig) (*Connection, error) {
	p.mu.RLock()
	if conn, exists := p.connections[cfg.Name]; exists {
		p.mu.RUnlock()

		if err := conn.IsHealthy(context.Background()); err == nil {
			return conn, nil
		}

		log.Printf("Connection to %s is unhealthy, recreating: %v", cfg.Name, err)
		p.removeConnection(cfg.Name)
	} else {
		p.mu.RUnlock()
	}

	return p.createConnection(cfg)
}

func (p *Pool) createConnection(cfg config.DatabaseConfig) (*Connection, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if conn, exists := p.connections[cfg.Name]; exists {
		if err := conn.IsHealthy(context.Background()); err == nil {
			return conn, nil
		}
		conn.Close()
	}

	conn, err := NewConnection(cfg, p.poolCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection for %s: %w", cfg.Name, err)
	}

	p.connections[cfg.Name] = conn
	log.Printf("Created new connection for database: %s", cfg.Name)

	return conn, nil
}

func (p *Pool) removeConnection(name string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if conn, exists := p.connections[name]; exists {
		conn.Close()
		delete(p.connections, name)
		log.Printf("Removed connection for database: %s", name)
	}
}

func (p *Pool) GetAllStats(ctx context.Context) (map[string]*PoolStats, error) {
	p.mu.RLock()
	connections := make(map[string]*Connection)
	for name, conn := range p.connections {
		connections[name] = conn
	}
	p.mu.RUnlock()

	stats := make(map[string]*PoolStats)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errors []error

	for name, conn := range connections {
		wg.Add(1)
		go func(dbName string, connection *Connection) {
			defer wg.Done()

			stat, err := p.getConnectionStats(ctx, dbName, connection)

			mu.Lock()
			defer mu.Unlock()

			if err != nil {
				errors = append(errors, fmt.Errorf("failed to get stats for %s: %w", dbName, err))
				return
			}

			stats[dbName] = stat
		}(name, conn)
	}

	wg.Wait()

	if len(errors) > 0 {
		log.Printf("Encountered %d errors while collecting pool stats", len(errors))
		for _, err := range errors {
			log.Printf("Pool stats error: %v", err)
		}
	}

	return stats, nil
}

func (p *Pool) getConnectionStats(ctx context.Context, name string, conn *Connection) (*PoolStats, error) {
	dbStats := conn.GetDBStats()

	isHealthy := conn.IsHealthy(ctx) == nil

	stats := &PoolStats{
		DatabaseName:     name,
		OpenConnections:  dbStats.OpenConnections,
		IdleConnections:  dbStats.Idle,
		InUseConnections: dbStats.InUse,
		MaxConnections:   dbStats.MaxOpenConnections,
		TotalQueries:     int64(dbStats.OpenConnections), // Approximation
		ConnectionStats:  dbStats,
		LastHealthCheck:  time.Now(),
		IsHealthy:        isHealthy,
	}

	if conn.config.Type == "postgresql" {
		if provider, ok := conn.stats.(*PostgreSQLStatsProvider); ok {
			if extended, err := provider.GetExtendedStats(ctx, conn.db); err == nil {
				stats.Extended = extended
			} else {
				log.Printf("Failed to get extended stats for %s: %v", name, err)
			}
		}
	}

	return stats, nil
}

func (p *Pool) HealthCheck(ctx context.Context) map[string]error {
	p.mu.RLock()
	connections := make(map[string]*Connection)
	for name, conn := range p.connections {
		connections[name] = conn
	}
	p.mu.RUnlock()

	results := make(map[string]error)
	var wg sync.WaitGroup
	var mu sync.Mutex

	for name, conn := range connections {
		wg.Add(1)
		go func(dbName string, connection *Connection) {
			defer wg.Done()

			err := connection.IsHealthy(ctx)

			mu.Lock()
			results[dbName] = err
			mu.Unlock()

			if err != nil {
				log.Printf("Health check failed for %s: %v", dbName, err)
				go p.removeConnection(dbName)
			}
		}(name, conn)
	}

	wg.Wait()
	return results
}

func (p *Pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var lastErr error
	for name, conn := range p.connections {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection to %s: %v", name, err)
			lastErr = err
		}
	}

	p.connections = make(map[string]*Connection)
	return lastErr
}

func (p *Pool) GetConnectionCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.connections)
}

func (p *Pool) RemoveConnection(name string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if conn, exists := p.connections[name]; exists {
		if err := conn.Close(); err != nil {
			return fmt.Errorf("failed to close connection %s: %w", name, err)
		}
		delete(p.connections, name)
		log.Printf("Manually removed connection for database: %s", name)
	}

	return nil
}

func (p *Pool) StartHealthCheckRoutine(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(p.poolCfg.HealthCheckInterval) * time.Second)
	defer ticker.Stop()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				healthResults := p.HealthCheck(context.Background())
				healthyCount := 0
				totalCount := len(healthResults)

				for name, err := range healthResults {
					if err == nil {
						healthyCount++
					} else {
						log.Printf("Connection %s is unhealthy: %v", name, err)
					}
				}

				if totalCount > 0 {
					log.Printf("Connection pool health check: %d/%d healthy connections", healthyCount, totalCount)
				}
			}
		}
	}()
}

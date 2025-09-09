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
)

type DatabaseMonitor struct {
	config      *config.Config
	connections map[string]*database.Connection
	notifier    notifier.Notifier
	mu          sync.RWMutex
	lastStats   map[string]*database.SessionStats
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
	return &DatabaseMonitor{
		config:      cfg,
		connections: make(map[string]*database.Connection),
		notifier:    notifier,
		lastStats:   make(map[string]*database.SessionStats),
	}
}

func (dm *DatabaseMonitor) CheckAllInstances(ctx context.Context) {
	var wg sync.WaitGroup

	for _, dbConfig := range dm.config.Databases {
		wg.Add(1)
		go func(cfg config.DatabaseConfig) {
			defer wg.Done()
			dm.checkInstance(ctx, cfg)
		}(dbConfig)
	}

	wg.Wait()
}

func (dm *DatabaseMonitor) checkInstance(ctx context.Context, cfg config.DatabaseConfig) {
	// Obter ou criar conexão
	conn, err := dm.getConnection(cfg)
	if err != nil {
		log.Printf("Erro ao conectar com %s: %v", cfg.Name, err)
		dm.sendAlert(Alert{
			DatabaseName: cfg.Name,
			AlertType:    "CONNECTION_ERROR",
			Message:      fmt.Sprintf("Falha na conexão: %v", err),
			Timestamp:    time.Now(),
		})
		return
	}

	// Obter estatísticas
	stats, err := conn.GetSessionStats(ctx)
	if err != nil {
		log.Printf("Erro ao obter estatísticas de %s: %v", cfg.Name, err)
		dm.sendAlert(Alert{
			DatabaseName: cfg.Name,
			AlertType:    "QUERY_ERROR",
			Message:      fmt.Sprintf("Erro ao consultar estatísticas: %v", err),
			Timestamp:    time.Now(),
		})
		return
	}

	stats.Timestamp = time.Now().Format("2006-01-02 15:04:05")

	// Armazenar estatísticas atuais
	dm.mu.Lock()
	dm.lastStats[cfg.Name] = stats
	dm.mu.Unlock()

	log.Printf("DB: %s | Total: %d | Ativo: %d | Inativo: %d | Idle: %d | Esperando: %d",
		stats.DatabaseName, stats.Total, stats.Active, stats.Inactive, stats.Idle, stats.Waiting)

	// Verificar thresholds e enviar alertas se necessário
	dm.checkThresholds(stats)
}

func (dm *DatabaseMonitor) getConnection(cfg config.DatabaseConfig) (*database.Connection, error) {
	dm.mu.RLock()
	if conn, exists := dm.connections[cfg.Name]; exists {
		dm.mu.RUnlock()

		// Testar se a conexão ainda está válida
		if err := conn.db.Ping(); err == nil {
			return conn, nil
		}

		// Conexão inválida, remover e recriar
		dm.mu.Lock()
		conn.Close()
		delete(dm.connections, cfg.Name)
		dm.mu.Unlock()
	} else {
		dm.mu.RUnlock()
	}

	// Criar nova conexão
	conn, err := database.NewConnection(cfg)
	if err != nil {
		return nil, err
	}

	dm.mu.Lock()
	dm.connections[cfg.Name] = conn
	dm.mu.Unlock()

	return conn, nil
}

func (dm *DatabaseMonitor) checkThresholds(stats *database.SessionStats) {
	thresholds := dm.config.Thresholds

	// Verificar conexões ativas
	if stats.Active > thresholds.ActiveConnections {
		dm.sendAlert(Alert{
			DatabaseName: stats.DatabaseName,
			AlertType:    "HIGH_ACTIVE_CONNECTIONS",
			Message:      fmt.Sprintf("Muitas conexões ativas detectadas"),
			Value:        stats.Active,
			Threshold:    thresholds.ActiveConnections,
			Timestamp:    time.Now(),
		})
	}

	// Verificar conexões inativas
	if stats.Inactive > thresholds.InactiveConnections {
		dm.sendAlert(Alert{
			DatabaseName: stats.DatabaseName,
			AlertType:    "HIGH_INACTIVE_CONNECTIONS",
			Message:      fmt.Sprintf("Muitas conexões inativas detectadas"),
			Value:        stats.Inactive,
			Threshold:    thresholds.InactiveConnections,
			Timestamp:    time.Now(),
		})
	}

	// Verificar total de conexões
	if stats.Total > thresholds.TotalConnections {
		dm.sendAlert(Alert{
			DatabaseName: stats.DatabaseName,
			AlertType:    "HIGH_TOTAL_CONNECTIONS",
			Message:      fmt.Sprintf("Muitas conexões totais detectadas"),
			Value:        stats.Total,
			Threshold:    thresholds.TotalConnections,
			Timestamp:    time.Now(),
		})
	}
}

func (dm *DatabaseMonitor) sendAlert(alert Alert) {
	subject := fmt.Sprintf("ALERTA DB Monitor: %s - %s", alert.DatabaseName, alert.AlertType)

	var body string
	if alert.Value > 0 && alert.Threshold > 0 {
		body = fmt.Sprintf(`
ALERTA DE MONITORAMENTO DE BANCO DE DADOS

Banco de Dados: %s
Tipo de Alerta: %s
Mensagem: %s
Valor Atual: %d
Limite Configurado: %d
Timestamp: %s

Este é um alerta automatizado do sistema de monitoramento.
Por favor, verifique o estado do banco de dados.
		`, alert.DatabaseName, alert.AlertType, alert.Message,
			alert.Value, alert.Threshold, alert.Timestamp.Format("2006-01-02 15:04:05"))
	} else {
		body = fmt.Sprintf(`
ALERTA DE MONITORAMENTO DE BANCO DE DADOS

Banco de Dados: %s
Tipo de Alerta: %s
Mensagem: %s
Timestamp: %s

Este é um alerta automatizado do sistema de monitoramento.
Por favor, verifique o estado do banco de dados.
		`, alert.DatabaseName, alert.AlertType, alert.Message,
			alert.Timestamp.Format("2006-01-02 15:04:05"))
	}

	if err := dm.notifier.SendAlert(subject, body); err != nil {
		log.Printf("Erro ao enviar alerta para %s: %v", alert.DatabaseName, err)
	} else {
		log.Printf("Alerta enviado para %s: %s", alert.DatabaseName, alert.AlertType)
	}
}

func (dm *DatabaseMonitor) GetLastStats() map[string]*database.SessionStats {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	// Criar cópia das estatísticas
	stats := make(map[string]*database.SessionStats)
	for k, v := range dm.lastStats {
		statsCopy := *v
		stats[k] = &statsCopy
	}

	return stats
}

func (dm *DatabaseMonitor) Close() error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	for name, conn := range dm.connections {
		if err := conn.Close(); err != nil {
			log.Printf("Erro ao fechar conexão com %s: %v", name, err)
		}
	}

	dm.connections = make(map[string]*database.Connection)
	return nil
}

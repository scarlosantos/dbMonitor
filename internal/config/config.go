package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Databases   []DatabaseConfig  `yaml:"databases"`
	Email       EmailConfig       `yaml:"email"`
	Slack       SlackConfig       `yaml:"slack"`
	Thresholds  ThresholdConfig   `yaml:"thresholds"`
	Pool        PoolConfig        `yaml:"pool"`
	Application ApplicationConfig `yaml:"application"`
}

type DatabaseConfig struct {
	Name           string `yaml:"name"`
	Type           string `yaml:"type"`
	Host           string `yaml:"host"`
	Port           int    `yaml:"port"`
	Database       string `yaml:"database"`
	Username       string `yaml:"username"`
	Password       string `yaml:"password"`
	SSLMode        string `yaml:"ssl_mode"`
	CertPath       string `yaml:"cert_path"`
	ConnectTimeout int    `yaml:"connect_timeout"`
	QueryTimeout   int    `yaml:"query_timeout"`
}

type EmailConfig struct {
	SMTPHost  string   `yaml:"smtp_host"`
	SMTPPort  int      `yaml:"smtp_port"`
	Username  string   `yaml:"username"`
	Password  string   `yaml:"password"`
	FromEmail string   `yaml:"from_email"`
	ToEmails  []string `yaml:"to_emails"`
	UseTLS    bool     `yaml:"use_tls"`
}

type SlackConfig struct {
	WebhookURL string `yaml:"webhook_url"`
}

type ThresholdConfig struct {
	ActiveConnections   int `yaml:"active_connections"`
	InactiveConnections int `yaml:"inactive_connections"`
	TotalConnections    int `yaml:"total_connections"`
}

type PoolConfig struct {
	MaxOpenConns        int `yaml:"max_open_conns"`
	MaxIdleConns        int `yaml:"max_idle_conns"`
	ConnMaxLifetime     int `yaml:"conn_max_lifetime"`
	ConnMaxIdleTime     int `yaml:"conn_max_idle_time"`
	HealthCheckInterval int `yaml:"health_check_interval"`
	BackoffInitial      int `yaml:"backoff_initial"`
	BackoffMax          int `yaml:"backoff_max"`
}

type ApplicationConfig struct {
	MonitoringInterval  int    `yaml:"monitoring_interval"`
	HealthCheckInterval int    `yaml:"health_check_interval"`
	AlertResetInterval  int    `yaml:"alert_reset_interval"`
	AlertFrequency      int    `yaml:"alert_frequency"`
	HTTPServerAddress   string `yaml:"http_server_address"`
}

func Load(configPath string) (*Config, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("erro ao ler arquivo de configuração: %w", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("erro ao fazer parse da configuração: %w", err)
	}

	if err := config.validate(); err != nil {
		return nil, fmt.Errorf("configuração inválida: %w", err)
	}

	return &config, nil
}

func (c *Config) validate() error {
	if len(c.Databases) == 0 {
		return fmt.Errorf("nenhuma base de dados configurada")
	}

	for i, db := range c.Databases {
		if db.Name == "" {
			return fmt.Errorf("nome da base de dados %d não pode estar vazio", i)
		}
		if db.Type != "mysql" && db.Type != "postgresql" {
			return fmt.Errorf("tipo de base de dados inválido para %s: %s", db.Name, db.Type)
		}
		if db.Host == "" {
			return fmt.Errorf("host não pode estar vazio para %s", db.Name)
		}
	}

	if c.Email.SMTPHost == "" || c.Email.FromEmail == "" || len(c.Email.ToEmails) == 0 {
		return fmt.Errorf("configuração de email incompleta")
	}

	if c.Application.MonitoringInterval == 0 || c.Application.HealthCheckInterval == 0 || c.Application.AlertResetInterval == 0 || c.Application.AlertFrequency == 0 {
		return fmt.Errorf("configurações de aplicação incompletas")
	}

	return nil
}

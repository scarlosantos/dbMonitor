package notifier

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"dbMonitor/internal/config"
	"gopkg.in/gomail.v2"
)

type Notifier interface {
	SendAlert(subject, body string) error
}

type EmailNotifier struct {
	config config.EmailConfig
	dialer *gomail.Dialer
}

func NewEmailNotifier(cfg config.EmailConfig) *EmailNotifier {
	d := gomail.NewDialer(cfg.SMTPHost, cfg.SMTPPort, cfg.Username, cfg.Password)
	d.TLSConfig = nil

	return &EmailNotifier{
		config: cfg,
		dialer: d,
	}
}

func (e *EmailNotifier) SendAlert(subject, body string) error {
	m := gomail.NewMessage()

	m.SetHeader("From", e.config.FromEmail)
	if len(e.config.ToEmails) == 0 {
		return fmt.Errorf("nenhum destinatário de email configurado")
	}
	m.SetHeader("To", e.config.ToEmails...)
	m.SetHeader("Subject", subject)
	m.SetBody("text/plain", body)

	if err := e.dialer.DialAndSend(m); err != nil {
		return fmt.Errorf("falha ao enviar email: %w", err)
	}

	log.Printf("Email enviado com sucesso: %s", subject)
	return nil
}

func (e *EmailNotifier) TestConnection() error {
	closer, err := e.dialer.Dial()
	if err != nil {
		return fmt.Errorf("erro ao conectar com servidor SMTP: %w", err)
	}
	defer closer.Close()
	return nil
}

// Slack Notifier
type SlackNotifier struct {
	webhookURL string
}

func NewSlackNotifier(cfg config.SlackConfig) (*SlackNotifier, error) {
	if cfg.WebhookURL == "" {
		return nil, fmt.Errorf("URL do webhook do Slack não configurada")
	}
	return &SlackNotifier{
		webhookURL: cfg.WebhookURL,
	}, nil
}

func (s *SlackNotifier) SendAlert(subject, body string) error {
	payload := map[string]string{
		"text": fmt.Sprintf("*%s*\n```%s```", subject, body),
	}
	jsonPayload, _ := json.Marshal(payload)

	req, err := http.NewRequest("POST", s.webhookURL, bytes.NewBuffer(jsonPayload))
	if err != nil {
		return fmt.Errorf("falha ao criar requisição para o Slack: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("falha ao enviar notificação para o Slack: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("erro do servidor Slack: %s", resp.Status)
	}

	log.Printf("Notificação do Slack enviada com sucesso: %s", subject)
	return nil
}

// MultiNotifier
type MultiNotifier struct {
	notifiers []Notifier
}

func NewMultiNotifier(notifiers ...Notifier) *MultiNotifier {
	return &MultiNotifier{
		notifiers: notifiers,
	}
}

func (m *MultiNotifier) SendAlert(subject, body string) error {
	var errors []error
	for _, notifier := range m.notifiers {
		if err := notifier.SendAlert(subject, body); err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("múltiplos erros de notificação: %v", errors)
	}

	return nil
}

// Mock Notifier para testes
type MockNotifier struct {
	SentAlerts []struct {
		Subject string
		Body    string
	}
}

func NewMockNotifier() *MockNotifier {
	return &MockNotifier{
		SentAlerts: make([]struct {
			Subject string
			Body    string
		}, 0),
	}
}

func (m *MockNotifier) SendAlert(subject, body string) error {
	m.SentAlerts = append(m.SentAlerts, struct {
		Subject string
		Body    string
	}{
		Subject: subject,
		Body:    body,
	})
	log.Printf("Mock alert: %s", subject)
	return nil
}

func (m *MockNotifier) GetLastAlert() (string, string) {
	if len(m.SentAlerts) == 0 {
		return "", ""
	}
	last := m.SentAlerts[len(m.SentAlerts)-1]
	return last.Subject, last.Body
}

func (m *MockNotifier) GetAlertCount() int {
	return len(m.SentAlerts)
}

func (m *MockNotifier) Clear() {
	m.SentAlerts = m.SentAlerts[:0]
}

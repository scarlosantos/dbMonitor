// internal/notifier/notifier.go
package notifier

import (
	"fmt"
	"log"

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

	if cfg.UseTLS {
		d.TLSConfig = nil // Use default TLS config
	}

	return &EmailNotifier{
		config: cfg,
		dialer: d,
	}
}

func (e *EmailNotifier) SendAlert(subject, body string) error {
	m := gomail.NewMessage()

	// Configurar remetente
	m.SetHeader("From", e.config.FromEmail)

	// Configurar destinatários
	if len(e.config.ToEmails) == 0 {
		return fmt.Errorf("nenhum destinatário configurado")
	}

	m.SetHeader("To", e.config.ToEmails...)

	// Configurar assunto e corpo
	m.SetHeader("Subject", subject)
	m.SetBody("text/plain", body)

	// Enviar email
	if err := e.dialer.DialAndSend(m); err != nil {
		log.Printf("Erro ao enviar email: %v", err)
		return fmt.Errorf("falha ao enviar email: %w", err)
	}

	log.Printf("Email enviado com sucesso: %s", subject)
	return nil
}

func (e *EmailNotifier) TestConnection() error {
	// Testar conexão SMTP
	closer, err := e.dialer.Dial()
	if err != nil {
		return fmt.Errorf("erro ao conectar com servidor SMTP: %w", err)
	}
	defer closer.Close()

	log.Println("Teste de conexão SMTP realizado com sucesso")
	return nil
}

// Implementação alternativa para outros tipos de notificação (futuro)
type SlackNotifier struct {
	webhookURL string
}

func NewSlackNotifier(webhookURL string) *SlackNotifier {
	return &SlackNotifier{
		webhookURL: webhookURL,
	}
}

func (s *SlackNotifier) SendAlert(subject, body string) error {
	// Implementação futura para Slack
	log.Printf("Slack notification (não implementado): %s", subject)
	return nil
}

// Implementação para múltiplos notificadores
type MultiNotifier struct {
	notifiers []Notifier
}

func NewMultiNotifier(notifiers ...Notifier) *MultiNotifier {
	return &MultiNotifier{
		notifiers: notifiers,
	}
}

func (m *MultiNotifier) SendAlert(subject, body string) error {
	var lastErr error

	for _, notifier := range m.notifiers {
		if err := notifier.SendAlert(subject, body); err != nil {
			log.Printf("Erro em notificador: %v", err)
			lastErr = err
		}
	}

	return lastErr
}

// Implementação de notificador mock para testes
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

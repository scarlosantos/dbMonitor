package database

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"path/filepath"
)

func validateTLSCertFiles(certPath string) error {
	files := map[string]string{
		"client certificate": filepath.Join(certPath, "client-cert.pem"),
		"client key":         filepath.Join(certPath, "client-key.pem"),
		"CA certificate":     filepath.Join(certPath, "ca-cert.pem"),
	}

	for fileType, filePath := range files {
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			return fmt.Errorf("%s file not found: %s", fileType, filePath)
		}

		if _, err := os.Open(filePath); err != nil {
			return fmt.Errorf("cannot read %s file %s: %w", fileType, filePath, err)
		}
	}

	return nil
}

func loadTLSConfig(certPath string, serverName string) (*tls.Config, error) {
	if err := validateTLSCertFiles(certPath); err != nil {
		return nil, fmt.Errorf("certificate validation failed: %w", err)
	}

	certFile := filepath.Join(certPath, "client-cert.pem")
	keyFile := filepath.Join(certPath, "client-key.pem")
	caFile := filepath.Join(certPath, "ca-cert.pem")

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
		ServerName:   serverName,
	}, nil
}

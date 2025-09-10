package database

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"path/filepath"
)

func loadTLSConfig(certPath string) (*tls.Config, error) {
	certFile := filepath.Join(certPath, "client-cert.pem")
	keyFile := filepath.Join(certPath, "client-key.pem")
	caFile := filepath.Join(certPath, "ca-cert.pem")

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
		ServerName:   "",
	}, nil
}

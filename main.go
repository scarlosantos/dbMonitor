package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"dbMonitor/internal/config"
	"dbMonitor/internal/monitor"
	"dbMonitor/internal/notifier"
)

func main() {
	cfg, err := config.Load("config.yaml")
	if err != nil {
		log.Fatalf("Erro ao carregar configuração: %v", err)
	}

	emailNotifier := notifier.NewEmailNotifier(cfg.Email)

	dbMonitor := monitor.NewDatabaseMonitor(cfg, emailNotifier)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Recebido sinal de interrupção, finalizando...")
		cancel()
	}()

	log.Println("Iniciando monitoramento de banco de dados...")

	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	dbMonitor.CheckAllInstances(ctx)

	for {
		select {
		case <-ctx.Done():
			log.Println("Monitoramento finalizado")
			return
		case <-ticker.C:
			dbMonitor.CheckAllInstances(ctx)
		}
	}
}

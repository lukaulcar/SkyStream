package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"skystream/pkg/config"
	"skystream/pkg/db"
	"skystream/pkg/hub"
	"skystream/pkg/poller"
)

func main() {
	envFile := flag.String("env", ".env", "path to .env file")
	portFlag := flag.Int("port", 0, "server port (overrides .env)")
	flag.Parse()

	cfg := config.LoadConfig(*envFile)
	if *portFlag > 0 {
		cfg.Port = *portFlag
	}

	log.Printf("Starting SkyStream service (Port: %d)", cfg.Port)
	log.Printf("Configuration: WebSocket=%v (interval: %v), ClickHouse=%v (interval: %v)",
		cfg.EnableWebSocket, cfg.WsRefreshInterval, cfg.EnableClickHouse, cfg.ClickHouseSaveInterval)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var clickhouseDB *db.DB
	if cfg.EnableClickHouse {
		var err error
		clickhouseDB, err = db.InitDB(ctx, cfg)
		if err != nil {
			log.Printf("Warning: Failed to initialize ClickHouse DB: %v (running with DB saving disabled)", err)
		} else {
			log.Printf("Successfully connected to ClickHouse DB '%s' at %s", cfg.ClickHouseDB, cfg.ClickHouseAddr)
		}
	} else {
		log.Println("ClickHouse database saving is DISABLED in configuration")
	}

	var wsHub *hub.Hub
	if cfg.EnableWebSocket {
		wsHub = hub.NewHub(cfg.WsMaxClients)
		go wsHub.Run()
		log.Printf("WebSocket service is ENABLED (max clients: %d)", cfg.WsMaxClients)
	} else {
		log.Println("WebSocket service is DISABLED in configuration")
	}

	p := poller.NewPoller(cfg, wsHub, clickhouseDB)
	go p.Start(ctx)

	mux := http.NewServeMux()
	if cfg.EnableWebSocket && wsHub != nil {
		mux.HandleFunc("/ws", wsHub.HandleWebSocket)
	} else {
		mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "WebSocket is disabled in configuration", http.StatusServiceUnavailable)
		})
	}

	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Port),
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	serverErrChan := make(chan error, 1)
	go func() {
		log.Printf("HTTP server listening on :%d", cfg.Port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			serverErrChan <- err
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-serverErrChan:
		log.Fatalf("server error: %v", err)
	case <-sigChan:
		log.Println("Received termination signal, shutting down gracefully...")
		cancel()
		p.Wait()
		if clickhouseDB != nil {
			_ = clickhouseDB.Close()
		}
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		_ = server.Shutdown(shutdownCtx)
		log.Println("Server stopped")
	}
}

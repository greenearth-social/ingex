package common

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"
)

// HealthStatus represents the current health state of the service
type HealthStatus struct {
	Healthy   bool      `json:"healthy"`
	Status    string    `json:"status"`
	StartedAt time.Time `json:"started_at"`
	Message   string    `json:"message,omitempty"`
}

// HealthServer manages the HTTP health check endpoint
type HealthServer struct {
	port      int
	server    *http.Server
	mu        sync.RWMutex
	healthy   bool
	startedAt time.Time
	message   string
	logger    *IngestLogger
}

// NewHealthServer creates a new health check server
// It will try the specified port, and if that fails, will try ports up to maxPort
func NewHealthServer(port int, maxPort int, logger *IngestLogger) (*HealthServer, error) {
	hs := &HealthServer{
		port:      port,
		startedAt: time.Now(),
		healthy:   false,
		message:   "Initializing...",
		logger:    logger,
	}

	// Try to find an available port
	actualPort := port
	for actualPort <= maxPort {
		listener, err := net.Listen("tcp", fmt.Sprintf(":%d", actualPort))
		if err == nil {
			listener.Close()
			hs.port = actualPort
			break
		}
		logger.Info("Port %d is in use, trying next port...", actualPort)
		actualPort++
	}

	if actualPort > maxPort {
		return nil, fmt.Errorf("no available ports between %d and %d", port, maxPort)
	}

	if actualPort != port {
		logger.Info("Using port %d for health checks (port %d was in use)", actualPort, port)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", hs.handleHealth)
	mux.HandleFunc("/healthz", hs.handleHealth)
	mux.HandleFunc("/ready", hs.handleReady)
	mux.HandleFunc("/", hs.handleRoot)

	hs.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", hs.port),
		Handler: mux,
	}

	return hs, nil
}

// Start begins serving health check requests
func (hs *HealthServer) Start(ctx context.Context) error {
	hs.logger.Info("Starting health check server on port %d", hs.port)

	go func() {
		if err := hs.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			hs.logger.Error("Health check server error: %v", err)
		}
	}()

	// Wait for shutdown signal
	<-ctx.Done()
	hs.logger.Info("Shutting down health check server...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return hs.server.Shutdown(shutdownCtx)
}

// SetHealthy marks the service as healthy and ready to serve traffic
func (hs *HealthServer) SetHealthy(healthy bool, message string) {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	hs.healthy = healthy
	hs.message = message
	if healthy {
		hs.logger.Info("Service marked as healthy: %s", message)
	} else {
		hs.logger.Info("Service marked as unhealthy: %s", message)
	}
}

// handleHealth handles /health and /healthz endpoints
func (hs *HealthServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	hs.mu.RLock()
	defer hs.mu.RUnlock()

	status := HealthStatus{
		Healthy:   hs.healthy,
		Status:    hs.getStatusString(),
		StartedAt: hs.startedAt,
		Message:   hs.message,
	}

	w.Header().Set("Content-Type", "application/json")
	if !hs.healthy {
		w.WriteHeader(http.StatusServiceUnavailable)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	json.NewEncoder(w).Encode(status)
}

// handleReady handles /ready endpoint (returns 200 only when fully ready)
func (hs *HealthServer) handleReady(w http.ResponseWriter, r *http.Request) {
	hs.mu.RLock()
	defer hs.mu.RUnlock()

	if !hs.healthy {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("Not ready"))
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Ready"))
}

// handleRoot handles the root endpoint
func (hs *HealthServer) handleRoot(w http.ResponseWriter, r *http.Request) {
	hs.mu.RLock()
	defer hs.mu.RUnlock()

	status := HealthStatus{
		Healthy:   hs.healthy,
		Status:    hs.getStatusString(),
		StartedAt: hs.startedAt,
		Message:   hs.message,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(status)
}

// getStatusString returns a human-readable status string
func (hs *HealthServer) getStatusString() string {
	if hs.healthy {
		return "healthy"
	}
	return "unhealthy"
}

// GetPort returns the actual port the health server is using
func (hs *HealthServer) GetPort() int {
	return hs.port
}

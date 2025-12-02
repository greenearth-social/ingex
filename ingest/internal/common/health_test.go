package common

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"
)

func TestHealthServer_NewHealthServer(t *testing.T) {
	logger := NewLogger(false)

	// Test creating a health server
	hs, err := NewHealthServer(8080, 8089, logger)
	if err != nil {
		t.Fatalf("Failed to create health server: %v", err)
	}

	if hs == nil {
		t.Fatal("Expected health server to be created")
	}

	if hs.GetPort() < 8080 || hs.GetPort() > 8089 {
		t.Errorf("Expected port between 8080-8089, got %d", hs.GetPort())
	}
}

func TestHealthServer_SetHealthy(t *testing.T) {
	logger := NewLogger(false)
	hs, err := NewHealthServer(9070, 9079, logger)
	if err != nil {
		t.Fatalf("Failed to create health server: %v", err)
	}

	// Start the server
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go hs.Start(ctx)
	time.Sleep(100 * time.Millisecond)

	port := hs.GetPort()

	// Initially should be unhealthy
	status := getHealthStatus(t, port)
	if status.Healthy {
		t.Error("Expected server to start unhealthy")
	}

	// Test setting healthy
	hs.SetHealthy(true, "Service is running")
	time.Sleep(10 * time.Millisecond) // Allow state to update

	status = getHealthStatus(t, port)
	if !status.Healthy {
		t.Error("Expected server to be healthy after SetHealthy(true)")
	}
	if status.Message != "Service is running" {
		t.Errorf("Expected message 'Service is running', got '%s'", status.Message)
	}
	if status.Status != "healthy" {
		t.Errorf("Expected status 'healthy', got '%s'", status.Status)
	}

	// Test setting unhealthy
	hs.SetHealthy(false, "Service is stopped")
	time.Sleep(10 * time.Millisecond)

	status = getHealthStatus(t, port)
	if status.Healthy {
		t.Error("Expected server to be unhealthy after SetHealthy(false)")
	}
	if status.Message != "Service is stopped" {
		t.Errorf("Expected message 'Service is stopped', got '%s'", status.Message)
	}
	if status.Status != "unhealthy" {
		t.Errorf("Expected status 'unhealthy', got '%s'", status.Status)
	}
}

func TestHealthServer_Endpoints(t *testing.T) {
	logger := NewLogger(false)
	hs, err := NewHealthServer(9080, 9089, logger)
	if err != nil {
		t.Fatalf("Failed to create health server: %v", err)
	}

	// Start the server
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go hs.Start(ctx)
	time.Sleep(100 * time.Millisecond)

	port := hs.GetPort()

	// Test that server starts unhealthy
	resp, err := http.Get(fmt.Sprintf("http://localhost:%d/health", port))
	if err != nil {
		t.Fatalf("Failed to get /health: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("Expected status 503 for unhealthy service, got %d", resp.StatusCode)
	}

	var status HealthStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if status.Healthy {
		t.Error("Expected healthy=false initially")
	}

	// Set service healthy
	hs.SetHealthy(true, "Test service running")
	time.Sleep(10 * time.Millisecond)

	// Test /health endpoint returns 200 when healthy
	resp, err = http.Get(fmt.Sprintf("http://localhost:%d/health", port))
	if err != nil {
		t.Fatalf("Failed to get /health: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200 for healthy service, got %d", resp.StatusCode)
	}

	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if !status.Healthy {
		t.Error("Expected healthy=true after SetHealthy")
	}

	if status.Message != "Test service running" {
		t.Errorf("Expected message 'Test service running', got '%s'", status.Message)
	}

	// Test /healthz endpoint (should be same as /health)
	resp, err = http.Get(fmt.Sprintf("http://localhost:%d/healthz", port))
	if err != nil {
		t.Fatalf("Failed to get /healthz: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200 for /healthz, got %d", resp.StatusCode)
	}

	// Test /ready endpoint
	resp, err = http.Get(fmt.Sprintf("http://localhost:%d/ready", port))
	if err != nil {
		t.Fatalf("Failed to get /ready: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200 for /ready, got %d", resp.StatusCode)
	}

	// Test root endpoint
	resp, err = http.Get(fmt.Sprintf("http://localhost:%d/", port))
	if err != nil {
		t.Fatalf("Failed to get /: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200 for /, got %d", resp.StatusCode)
	}
}

func TestHealthServer_PortRetry(t *testing.T) {
	logger := NewLogger(false)

	// Create first server on port 9090
	hs1, err := NewHealthServer(9090, 9099, logger)
	if err != nil {
		t.Fatalf("Failed to create first health server: %v", err)
	}

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()

	go hs1.Start(ctx1)
	time.Sleep(100 * time.Millisecond)

	// Create second server with same port range - should get different port
	hs2, err := NewHealthServer(9090, 9099, logger)
	if err != nil {
		t.Fatalf("Failed to create second health server: %v", err)
	}

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	go hs2.Start(ctx2)
	time.Sleep(100 * time.Millisecond)

	// Verify both servers are running on different ports
	port1 := hs1.GetPort()
	port2 := hs2.GetPort()

	if port1 == port2 {
		t.Errorf("Expected different ports, both got %d", port1)
	}

	// Verify both servers are accessible
	status1 := getHealthStatus(t, port1)
	status2 := getHealthStatus(t, port2)

	if status1.StartedAt.IsZero() {
		t.Error("Expected server 1 to have valid start time")
	}
	if status2.StartedAt.IsZero() {
		t.Error("Expected server 2 to have valid start time")
	}
}

// Helper function to get health status from a running server
func getHealthStatus(t *testing.T, port int) HealthStatus {
	resp, err := http.Get(fmt.Sprintf("http://localhost:%d/health", port))
	if err != nil {
		t.Fatalf("Failed to get health status: %v", err)
	}
	defer resp.Body.Close()

	var status HealthStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		t.Fatalf("Failed to decode health status: %v", err)
	}

	return status
}

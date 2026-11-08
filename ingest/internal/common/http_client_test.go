package common

import (
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// newConnections runs rounds of `inFlight` concurrent requests through client
// and returns how many TCP connections the server saw opened.
func newConnections(t *testing.T, client *http.Client, inFlight, rounds int) int64 {
	t.Helper()
	var opened atomic.Int64
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Long enough that every request in a round is in flight at once.
		time.Sleep(10 * time.Millisecond)
		_, _ = w.Write([]byte("ok"))
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			opened.Add(1)
		}
	}
	server.Start()
	defer server.Close()

	for range rounds {
		var wg sync.WaitGroup
		for range inFlight {
			wg.Go(func() {
				req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL, nil)
				if err != nil {
					t.Error(err)
					return
				}
				resp, err := client.Do(req)
				if err != nil {
					t.Error(err)
					return
				}
				defer func() { _ = resp.Body.Close() }()
				_, _ = resp.Body.Read(make([]byte, 8))
			})
		}
		wg.Wait()
	}
	return opened.Load()
}

func defaultTransport(t *testing.T) *http.Transport {
	t.Helper()
	transport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		t.Fatal("http.DefaultTransport isn't an *http.Transport")
	}
	return transport
}

func TestNewHTTPClientReusesConnectionsAtItsConcurrency(t *testing.T) {
	const inFlight, rounds = 16, 5

	opened := newConnections(t, NewHTTPClient(5*time.Second, inFlight), inFlight, rounds)
	t.Logf("pooled client: %d connections for %d requests", opened, inFlight*rounds)

	if opened > inFlight {
		t.Errorf("opened %d connections for %d requests %d at a time; want at most %d",
			opened, inFlight*rounds, inFlight, inFlight)
	}
}

func TestDefaultClientChurnsConnectionsBeyondTwo(t *testing.T) {
	// The behaviour NewHTTPClient exists to avoid, pinned so the test above
	// is known to be measuring something.
	const inFlight, rounds = 16, 5
	client := &http.Client{Timeout: 5 * time.Second, Transport: defaultTransport(t).Clone()}

	opened := newConnections(t, client, inFlight, rounds)
	t.Logf("default transport: %d connections for %d requests", opened, inFlight*rounds)

	if opened <= inFlight {
		t.Errorf("default transport opened only %d connections; expected churn beyond %d", opened, inFlight)
	}
}

func TestNewHTTPClientKeepsTheDefaultsForLowConcurrency(t *testing.T) {
	transport, ok := NewHTTPClient(time.Second, 1).Transport.(*http.Transport)
	if !ok {
		t.Fatal("NewHTTPClient's transport isn't an *http.Transport")
	}
	defaults := defaultTransport(t)

	if transport.MaxIdleConnsPerHost != defaults.MaxIdleConnsPerHost ||
		transport.MaxIdleConns != defaults.MaxIdleConns {
		t.Errorf("pool resized for a single connection: %d/%d", transport.MaxIdleConnsPerHost, transport.MaxIdleConns)
	}
	if transport == defaults {
		t.Error("NewHTTPClient must not modify the shared default transport")
	}
}

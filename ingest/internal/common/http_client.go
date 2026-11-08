package common

import (
	"net/http"
	"time"
)

// NewHTTPClient returns an http.Client for a service called with up to
// maxConns requests in flight at once.
//
// Go's default transport keeps only two idle connections per host. A caller
// with more requests in flight than that closes every connection beyond the
// second as its request finishes and dials a new one for the next — under
// steady load, nearly a fresh connection per request, and over TLS a fresh
// handshake each time. Sizing the idle pool to the concurrency lets every
// request reuse a connection instead.
func NewHTTPClient(timeout time.Duration, maxConns int) *http.Client {
	defaults, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		// Something replaced the default transport; leave it as it is.
		return &http.Client{Timeout: timeout}
	}
	transport := defaults.Clone()
	// Zero means http.DefaultMaxIdleConnsPerHost (2), so compare against that.
	perHost := transport.MaxIdleConnsPerHost
	if perHost == 0 {
		perHost = http.DefaultMaxIdleConnsPerHost
	}
	if maxConns > perHost {
		transport.MaxIdleConnsPerHost = maxConns
	}
	if maxConns > transport.MaxIdleConns {
		transport.MaxIdleConns = maxConns
	}
	return &http.Client{Timeout: timeout, Transport: transport}
}

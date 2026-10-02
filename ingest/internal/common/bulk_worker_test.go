package common

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/elastic/go-elasticsearch/v9"
)

func TestBulkIndexWorker_callsFn(t *testing.T) {
	called := false
	fn := func(_ context.Context, _ *elasticsearch.Client, _ string, _ []string, _ bool, _ *IngestLogger) error {
		called = true
		return nil
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go BulkIndexWorker(&wg, context.Background(), nil, "test-index", []string{"a", "b"}, false, NewLogger(false), fn, "index")
	wg.Wait()
	if !called {
		t.Fatal("fn was not called")
	}
}

func TestBulkIndexWorker_errorIsLogged(t *testing.T) {
	fn := func(_ context.Context, _ *elasticsearch.Client, _ string, _ []string, _ bool, _ *IngestLogger) error {
		return fmt.Errorf("intentional error")
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go BulkIndexWorker(&wg, context.Background(), nil, "test-index", []string{"a"}, false, NewLogger(false), fn, "index")
	wg.Wait()
	// passes if no panic — error is logged, not propagated
}

func TestBulkIndexWorker_dryRunPassedToFn(t *testing.T) {
	var gotDryRun bool
	fn := func(_ context.Context, _ *elasticsearch.Client, _ string, _ []string, dryRun bool, _ *IngestLogger) error {
		gotDryRun = dryRun
		return nil
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go BulkIndexWorker(&wg, context.Background(), nil, "test-index", []string{"a"}, true, NewLogger(false), fn, "index")
	wg.Wait()
	if !gotDryRun {
		t.Fatal("dryRun=true was not passed to fn")
	}
}

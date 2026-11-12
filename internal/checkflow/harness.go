package checkflow

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"time"

	"voice-timeline-renderer/internal/httpapi"
	"voice-timeline-renderer/internal/service"
	"voice-timeline-renderer/internal/storage"
)

// Harness drives one workflow run against the real HTTP surface and the real
// local repository. It owns no directory lifecycle: run workspaces are created
// and retained or removed by Run, so a failed run stays reviewable on disk.
type Harness struct {
	baseURL string
	client  *http.Client
	dataDir string
	server  *httptest.Server
	repo    *storage.Repository
	report  *RunReport
}

// newHarness opens the real repository at dataDir (creating its standard
// layout) and starts the production HTTP handler in front of it.
func newHarness(dataDir string, report *RunReport) (*Harness, error) {
	repo, err := storage.NewRepository(dataDir)
	if err != nil {
		return nil, err
	}
	serviceLayer := service.New(repo, service.DefaultConfig())
	httpServer := httpapi.New(serviceLayer, httpapi.Options{
		MaxSourceRequestBytes: 48 << 20,
		MaxJSONBytes:          2 << 20,
	})
	server := httptest.NewServer(httpServer.Handler())
	return &Harness{
		baseURL: server.URL,
		client:  &http.Client{Timeout: 45 * time.Second},
		dataDir: dataDir,
		server:  server,
		repo:    repo,
		report:  report,
	}, nil
}

// Close stops the HTTP server but leaves every byte on disk untouched.
func (harness *Harness) Close() {
	harness.server.Close()
}

// reopen simulates a fresh process opening the same repository: the driving
// server is stopped and a new repository, service, and HTTP server are built
// from the same data directory. Nothing from the previous in-memory state is
// reused; the reader can only see what was durably committed.
func (harness *Harness) reopen() (*Harness, error) {
	harness.Close()
	return newHarness(harness.dataDir, harness.report)
}

// repository exposes the reopened repository so adjudications can read disk
// facts directly, including failed jobs that the HTTP surface never publishes
// as downloadable output.
func (harness *Harness) repository() *storage.Repository {
	return harness.repo
}

func (harness *Harness) requestJSON(
	ctx context.Context,
	method string,
	path string,
	payload any,
) (int, []byte, error) {
	var body io.Reader
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return 0, nil, err
		}
		body = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, harness.baseURL+path, body)
	if err != nil {
		return 0, nil, err
	}
	if payload != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := harness.client.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	result, err := io.ReadAll(response.Body)
	if err != nil {
		return 0, nil, err
	}
	harness.report.exchange(method, path, response.StatusCode, result)
	return response.StatusCode, result, nil
}

func decodeBody(body []byte, target any) error {
	if err := json.Unmarshal(body, target); err != nil {
		return fmt.Errorf("decode response %q: %w", string(body), err)
	}
	return nil
}

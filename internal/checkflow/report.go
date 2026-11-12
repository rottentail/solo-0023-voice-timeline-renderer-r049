package checkflow

import (
	"bytes"
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// maxEvidenceBody bounds how much of a single HTTP response body is copied
// into the run report. Rendered WAV bodies are large; the full bytes stay in
// the data directory, the report only needs enough to identify the response.
const (
	maxEvidenceBody = 4096
	maxBinaryBody   = 128
	looksJSONLimit  = 64
)

// truncateEvidence keeps JSON envelopes readable while preventing binary
// media (the rendered WAV output) from flooding the report; the complete media
// bytes remain in the run data directory.
func truncateEvidence(body []byte) string {
	limit := maxBinaryBody
	head := body
	if len(head) > looksJSONLimit {
		head = head[:looksJSONLimit]
	}
	trimmed := bytes.TrimLeft(head, " \t\r\n")
	if len(trimmed) > 0 && (trimmed[0] == '{' || trimmed[0] == '[') {
		limit = maxEvidenceBody
	}
	text := string(body)
	if len(text) > limit {
		text = text[:limit] + "...[truncated]"
	}
	return text
}

// StepRecord marks one named phase of a workflow run.
type StepRecord struct {
	Name   string `json:"name"`
	OK     bool   `json:"ok"`
	Detail string `json:"detail,omitempty"`
}

// ExchangeRecord stores a real HTTP request/response pair so that a failure
// response can be re-read next to the on-disk facts of the same run.
type ExchangeRecord struct {
	Method string `json:"method"`
	Path   string `json:"path"`
	Status int    `json:"status"`
	Body   string `json:"body,omitempty"`
}

// PromiseRecord is the verdict reached by a reader that reopens the repository
// after the driving server has stopped.
type PromiseRecord struct {
	ID          string `json:"id"`
	Kind        string `json:"kind"`
	Expectation string `json:"expectation"`
	Verdict     string `json:"verdict"`
	Evidence    string `json:"evidence"`
	Detail      string `json:"detail,omitempty"`
}

// DiskRecord describes one path captured in the end-of-run disk snapshot.
type DiskRecord struct {
	Path string `json:"path"`
	Dir  bool   `json:"dir"`
	Mode uint32 `json:"mode"`
	Size int64  `json:"size"`
}

// RunReport is the durable index of one workflow verification run. It lives
// next to the repository data directory so a retained failure can be reviewed
// without re-running anything.
type RunReport struct {
	Workflow   string           `json:"workflow"`
	RunDir     string           `json:"run_dir"`
	DataDir    string           `json:"data_dir"`
	StartedAt  time.Time        `json:"started_at"`
	FinishedAt time.Time        `json:"finished_at,omitempty"`
	Status     string           `json:"status"`
	Error      string           `json:"error,omitempty"`
	Steps      []StepRecord     `json:"steps"`
	Exchanges  []ExchangeRecord `json:"exchanges"`
	Promises   []PromiseRecord  `json:"promises"`
	Disk       []DiskRecord     `json:"disk_snapshot"`
}

func newRunReport(workflow, runDir, dataDir string) *RunReport {
	return &RunReport{
		Workflow:  workflow,
		RunDir:    runDir,
		DataDir:   dataDir,
		StartedAt: time.Now().UTC(),
		Status:    "running",
	}
}

// Step records the outcome of a named workflow phase and returns the error so
// call sites keep their linear return style.
func (report *RunReport) Step(name string, err error) error {
	record := StepRecord{Name: name, OK: err == nil}
	if err != nil {
		record.Detail = err.Error()
	}
	report.Steps = append(report.Steps, record)
	return err
}

func (report *RunReport) exchange(method, path string, status int, body []byte) {
	report.Exchanges = append(report.Exchanges, ExchangeRecord{
		Method: method,
		Path:   path,
		Status: status,
		Body:   truncateEvidence(body),
	})
}

// promise appends an adjudicated promise. A non-nil err marks the promise
// broken and is returned so the workflow fails while evidence is retained.
func (report *RunReport) promise(record PromiseRecord, err error) error {
	if err != nil {
		record.Verdict = "broken"
		if record.Detail == "" {
			record.Detail = err.Error()
		}
	} else if record.Verdict == "" {
		record.Verdict = "fulfilled"
	}
	report.Promises = append(report.Promises, record)
	return err
}

// snapshot walks the repository data directory and records every surviving
// path, including unpublished staging content under tmp/.
func (report *RunReport) snapshot() {
	entries := make([]DiskRecord, 0)
	_ = filepath.WalkDir(report.DataDir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return nil
		}
		relative, err := filepath.Rel(report.DataDir, path)
		if err != nil {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return nil
		}
		entries = append(entries, DiskRecord{
			Path: filepath.ToSlash(relative),
			Dir:  entry.IsDir(),
			Mode: uint32(info.Mode().Perm()),
			Size: info.Size(),
		})
		return nil
	})
	sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })
	report.Disk = entries
}

func (report *RunReport) write() error {
	payload, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	payload = append(payload, '\n')
	return os.WriteFile(filepath.Join(report.RunDir, "report.json"), payload, 0o640)
}

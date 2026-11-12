package checkflow

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// RunOptions controls where workflow run workspaces live and whether a
// successful run is kept for inspection.
type RunOptions struct {
	WorkspaceRoot string
	KeepOnSuccess bool
}

// DefaultWorkspaceRoot is the stable, reviewable home of run workspaces. It
// lives inside the repository working directory and is git-ignored.
const DefaultWorkspaceRoot = ".check-runs"

// Run executes one workflow inside a uniquely named, durable workspace.
//
// The workspace is never shared between runs, so a later successful run cannot
// inherit pollution from an earlier one. Failed runs are always retained with
// their report and full repository state; successful runs remove their
// workspace unless KeepOnSuccess is requested.
func Run(ctx context.Context, workflow string, options RunOptions) error {
	switch workflow {
	case "ingest-audio-source", "compose-timeline", "render-mixdown":
	default:
		return fmt.Errorf("unknown workflow %q", workflow)
	}
	root := options.WorkspaceRoot
	if strings.TrimSpace(root) == "" {
		root = DefaultWorkspaceRoot
	}
	runDir, dataDir, err := createRunWorkspace(root, workflow)
	if err != nil {
		return err
	}
	report := newRunReport(workflow, runDir, dataDir)

	runErr := executeWorkflow(ctx, workflow, dataDir, report)
	report.FinishedAt = time.Now().UTC()
	if runErr != nil {
		report.Status = "failed"
		report.Error = runErr.Error()
	} else {
		report.Status = "passed"
	}
	// A fault or a panic could leave an index read-only. Normalize modes so a
	// retained failure scene is always readable by the reviewer.
	ensureReviewable(dataDir)
	report.snapshot()
	if reportErr := report.write(); reportErr != nil {
		// A report write failure must surface even when the workflow passed.
		if runErr == nil {
			runErr = fmt.Errorf("write run report: %w", reportErr)
		}
	}

	if runErr != nil || options.KeepOnSuccess {
		if runErr != nil {
			return fmt.Errorf("%w (run workspace retained at %s)", runErr, runDir)
		}
		fmt.Printf("workflow %s retained its run workspace at %s\n", workflow, runDir)
		return nil
	}
	if removeErr := os.RemoveAll(runDir); removeErr != nil {
		return fmt.Errorf("remove successful run workspace %s: %w", runDir, removeErr)
	}
	return nil
}

func executeWorkflow(ctx context.Context, workflow, dataDir string, report *RunReport) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("workflow panic: %v", recovered)
		}
	}()
	harness, err := newHarness(dataDir, report)
	if err != nil {
		return report.Step("start harness", err)
	}
	defer harness.Close()

	switch workflow {
	case "ingest-audio-source":
		err = runIngest(ctx, harness, report)
	case "compose-timeline":
		err = runCompose(ctx, harness, report)
	case "render-mixdown":
		err = runRender(ctx, harness, report)
	}
	if err != nil {
		return err
	}

	// The driving server stops here; the reader opens the same directory as a
	// fresh process and adjudicates every success/failure promise from disk.
	reader, err := harness.reopen()
	if err != nil {
		return report.Step("reopen repository for adjudication", err)
	}
	defer reader.Close()
	return report.Step("adjudicate promises after reopen", adjudicate(workflow, ctx, reader, report))
}

func adjudicate(workflow string, ctx context.Context, reader *Harness, report *RunReport) error {
	switch workflow {
	case "ingest-audio-source":
		return adjudicateIngest(ctx, reader, report,
			[]string{"source_ingest_01"},
			[]string{"source_ingest_bad", "source_ingest_commit_fault"},
		)
	case "compose-timeline":
		return adjudicateCompose(ctx, reader, report,
			[]string{"timeline_compose_01"},
			[]string{"timeline_compose_bad"},
		)
	case "render-mixdown":
		return adjudicateRender(ctx, reader, report,
			[]string{"render_mixdown_01"},
			[]string{"render_publish_fault_01"},
		)
	default:
		return fmt.Errorf("unknown workflow %q", workflow)
	}
}

func createRunWorkspace(root, workflow string) (runDir, dataDir string, err error) {
	suffix, err := randomSuffix()
	if err != nil {
		return "", "", err
	}
	name := fmt.Sprintf("%s-%s-%s", workflow, time.Now().UTC().Format("20060102T150405Z"), suffix)
	runDir = filepath.Join(root, name)
	dataDir = filepath.Join(runDir, "data")
	// Fail loudly if a generated name ever collides with an existing run.
	if _, err := os.Stat(runDir); err == nil {
		return "", "", fmt.Errorf("run workspace %s already exists", runDir)
	} else if !os.IsNotExist(err) {
		return "", "", fmt.Errorf("inspect run workspace %s: %w", runDir, err)
	}
	if err := os.MkdirAll(dataDir, 0o750); err != nil {
		return "", "", fmt.Errorf("create run workspace %s: %w", runDir, err)
	}
	return runDir, dataDir, nil
}

func randomSuffix() (string, error) {
	buf := make([]byte, 4)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

// ensureReviewable restores standard writable modes across the run data
// directory so retained evidence can be inspected regardless of how the run
// ended. It never changes production code, only check-owned workspace files.
func ensureReviewable(dataDir string) {
	_ = filepath.Walk(dataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.IsDir() {
			_ = os.Chmod(path, 0o750)
		} else {
			_ = os.Chmod(path, 0o640)
		}
		return nil
	})
}

// leftoverStaging returns any source/render staging directories that survived
// a failed publication. Production storage stages under tmp/source-* and
// tmp/render-* and removes them on every failure path.
func leftoverStaging(dataDir string) ([]string, error) {
	tmpDir := filepath.Join(dataDir, "tmp")
	entries, err := os.ReadDir(tmpDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var matches []string
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() && (strings.HasPrefix(name, "source-") || strings.HasPrefix(name, "render-")) {
			matches = append(matches, filepath.ToSlash(filepath.Join("tmp", name)))
		}
	}
	return matches, nil
}

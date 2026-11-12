package checkflow

import (
	"context"
	"fmt"

	"voice-timeline-renderer/internal/domain"
)

// The reader functions reopen the same data directory after the driving server
// has stopped. Every verdict is reached through a fresh repository instance, so
// only durably committed facts count as fulfilled promises.

func adjudicateIngest(ctx context.Context, reader *Harness, report *RunReport, present, absent []string) error {
	var firstErr error
	for _, id := range present {
		record := PromiseRecord{
			ID:          id,
			Kind:        "source",
			Expectation: "registered source metadata and media survive a repository reopen",
			Evidence:    "storage.GetSource on a fresh repository over the same data directory",
		}
		_, payload, err := reader.repository().GetSource(id)
		if err != nil {
			err = fmt.Errorf("committed source %q is not readable after reopen: %w", id, err)
		} else if len(payload) == 0 {
			err = fmt.Errorf("committed source %q has empty media after reopen", id)
		} else if status, readErr := reader.getSourceStatus(ctx, id); readErr != nil || status != 200 {
			err = fmt.Errorf("GET source %q after reopen returned status %d: %w", id, status, readErr)
		}
		if report.promise(record, err) != nil && firstErr == nil {
			firstErr = err
		}
	}
	for _, id := range absent {
		record := PromiseRecord{
			ID:          id,
			Kind:        "source",
			Expectation: "rejected registration leaves no visible source after reopen",
			Evidence:    "storage.GetSourceMeta on a fresh repository must report not_found",
		}
		var err error
		if _, getErr := reader.repository().GetSourceMeta(id); getErr == nil {
			err = fmt.Errorf("rejected source %q became visible on disk", id)
		} else if appErr := domain.AsAppError(getErr); appErr.Kind != domain.KindNotFound {
			err = fmt.Errorf("rejected source %q produced unexpected storage error: %w", id, getErr)
		} else if status, readErr := reader.getSourceStatus(ctx, id); readErr != nil || status != 404 {
			err = fmt.Errorf("GET rejected source %q after reopen returned status %d, want 404", id, status)
		}
		if report.promise(record, err) != nil && firstErr == nil {
			firstErr = err
		}
	}
	if err := assertNoStagingLeftovers(reader, report); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

func adjudicateCompose(ctx context.Context, reader *Harness, report *RunReport, present, absent []string) error {
	var firstErr error
	for _, id := range present {
		record := PromiseRecord{
			ID:          id,
			Kind:        "timeline",
			Expectation: "persisted timeline is readable and ready after reopen",
			Evidence:    "storage.GetTimeline on a fresh repository over the same data directory",
		}
		plan, err := reader.repository().GetTimeline(id)
		if err == nil && plan.State != "ready" {
			err = fmt.Errorf("timeline %q reopened in state %q, want ready", id, plan.State)
		}
		if err == nil {
			if status, readErr := reader.getTimelineStatus(ctx, id); readErr != nil || status != 200 {
				err = fmt.Errorf("GET timeline %q after reopen returned status %d: %w", id, status, readErr)
			}
		}
		if report.promise(record, err) != nil && firstErr == nil {
			firstErr = err
		}
	}
	for _, id := range absent {
		record := PromiseRecord{
			ID:          id,
			Kind:        "timeline",
			Expectation: "rejected timeline validation persists no record after reopen",
			Evidence:    "storage.GetTimeline on a fresh repository must report not_found",
		}
		var err error
		if _, getErr := reader.repository().GetTimeline(id); getErr == nil {
			err = fmt.Errorf("rejected timeline %q became visible on disk", id)
		} else if appErr := domain.AsAppError(getErr); appErr.Kind != domain.KindNotFound {
			err = fmt.Errorf("rejected timeline %q produced unexpected storage error: %w", id, getErr)
		} else if status, readErr := reader.getTimelineStatus(ctx, id); readErr != nil || status != 404 {
			err = fmt.Errorf("GET rejected timeline %q after reopen returned status %d, want 404", id, status)
		}
		if report.promise(record, err) != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func adjudicateRender(ctx context.Context, reader *Harness, report *RunReport, completedIDs, failedIDs []string) error {
	var firstErr error
	for _, id := range completedIDs {
		record := PromiseRecord{
			ID:          id,
			Kind:        "render",
			Expectation: "completed job, receipt, and published output survive a repository reopen",
			Evidence:    "storage.GetJob and storage.GetRenderOutput on a fresh repository",
		}
		err := checkCompletedAfterReopen(ctx, reader, id)
		if report.promise(record, err) != nil && firstErr == nil {
			firstErr = err
		}
	}
	for _, id := range failedIDs {
		record := PromiseRecord{
			ID:          id,
			Kind:        "render",
			Expectation: "failed job is durably terminal and no partial output is published",
			Evidence:    "storage.GetJob must read failed; storage.GetRenderOutput must report not completed",
		}
		err := checkFailedAfterReopen(ctx, reader, id)
		if report.promise(record, err) != nil && firstErr == nil {
			firstErr = err
		}
	}
	if err := assertNoStagingLeftovers(reader, report); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

func checkCompletedAfterReopen(ctx context.Context, reader *Harness, id string) error {
	job, err := reader.repository().GetJob(id)
	if err != nil {
		return fmt.Errorf("completed job %q unreadable after reopen: %w", id, err)
	}
	if job.State != domain.RenderCompleted {
		return fmt.Errorf("job %q reopened in state %q, want completed", id, job.State)
	}
	if job.Output == nil || job.ReceiptDigest == "" {
		return fmt.Errorf("completed job %q lost output metadata after reopen", id)
	}
	payload, receipt, err := reader.repository().GetRenderOutput(id)
	if err != nil {
		return fmt.Errorf("published output for %q unreadable after reopen: %w", id, err)
	}
	if len(payload) == 0 {
		return fmt.Errorf("published output for %q is empty after reopen", id)
	}
	if receipt.Output.Digest != job.ReceiptDigest {
		return fmt.Errorf("receipt digest for %q disagrees with the job after reopen", id)
	}
	if status, readErr := reader.getRenderStatus(ctx, id); readErr != nil || status != 200 {
		return fmt.Errorf("GET render %q after reopen returned status %d: %w", id, status, readErr)
	}
	return nil
}

func checkFailedAfterReopen(ctx context.Context, reader *Harness, id string) error {
	job, err := reader.repository().GetJob(id)
	if err != nil {
		return fmt.Errorf("failed job %q is not durable after reopen: %w", id, err)
	}
	if job.State != domain.RenderFailed {
		return fmt.Errorf("job %q reopened in state %q, want failed", id, job.State)
	}
	if job.Error == nil || job.Error.Code == "" {
		return fmt.Errorf("failed job %q lost its stable error code after reopen", id)
	}
	if job.Output != nil || job.ReceiptDigest != "" {
		return fmt.Errorf("failed job %q carries output metadata it never published", id)
	}
	if _, _, outputErr := reader.repository().GetRenderOutput(id); outputErr == nil {
		return fmt.Errorf("failed job %q exposes a published output folder", id)
	} else if appErr := domain.AsAppError(outputErr); appErr.Kind != domain.KindUnready {
		return fmt.Errorf("failed job %q output lookup returned unexpected error: %w", id, outputErr)
	}
	if status, readErr := reader.getRenderStatus(ctx, id); readErr != nil || status != 200 {
		return fmt.Errorf("GET failed render %q after reopen returned status %d: %w", id, status, readErr)
	}
	return nil
}

func assertNoStagingLeftovers(reader *Harness, report *RunReport) error {
	record := PromiseRecord{
		ID:          "tmp-staging",
		Kind:        "storage",
		Expectation: "failed publication removes its unpublished staging directory",
		Evidence:    "tmp/ contains no source-* or render-* staging directories after the run",
	}
	matches, err := leftoverStaging(reader.dataDir)
	if err == nil && len(matches) > 0 {
		err = fmt.Errorf("unpublished staging directories survived: %v", matches)
	}
	return report.promise(record, err)
}

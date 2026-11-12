package checkflow

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math"

	"voice-timeline-renderer/internal/audiocodec"
	"voice-timeline-renderer/internal/domain"
	"voice-timeline-renderer/internal/service"
)

func runIngest(ctx context.Context, harness *Harness, report *RunReport) error {
	wav, err := sineWAV(48000, 1, 250, 440, 0.4)
	if err != nil {
		return err
	}
	asset, status, err := harness.createSource(ctx, "source_ingest_01", "narration take", wav)
	if err != nil {
		return err
	}
	if status != 201 {
		return fmt.Errorf("source write returned status %d, want 201", status)
	}
	if asset.FrameCount != 12000 || asset.BitsPerSample != 16 {
		return fmt.Errorf("source metadata is inconsistent: %+v", asset)
	}
	readBack, status, err := harness.getSource(ctx, asset.ID)
	if err != nil {
		return err
	}
	if status != 200 || readBack.Digest != asset.Digest {
		return fmt.Errorf("source read-back mismatch: status=%d digest=%s", status, readBack.Digest)
	}
	badStatus, body, err := harness.requestJSON(ctx, "PUT", "/v1/sources/source_ingest_bad", service.SourceWriteRequest{
		Name:      "broken take",
		WAVBase64: base64.StdEncoding.EncodeToString([]byte("not a wave")),
	})
	if err != nil {
		return err
	}
	if badStatus != 422 || failureCode(body) != "invalid_wav" {
		return fmt.Errorf("malformed media returned status=%d code=%s", badStatus, failureCode(body))
	}
	missing, status, err := harness.getSource(ctx, "source_ingest_bad")
	if err != nil {
		return err
	}
	if status != 404 || missing.ID != "" {
		return fmt.Errorf("failed source became visible: status=%d id=%s", status, missing.ID)
	}

	// Real publication-boundary fault: decoding, measurement, and staging all
	// succeed; only the atomic commit of the source directory is sabotaged.
	fault, err := armCommitFault(harness.dataDir, "sources")
	if err != nil {
		return report.Step("arm source commit fault", err)
	}
	faultAsset, faultStatus, faultBody, faultErr := harness.createSourceWithBody(ctx, "source_ingest_commit_fault", "commit fault take", wav)
	restoreErr := fault.Restore()
	if faultErr != nil {
		return report.Step("source commit fault request", faultErr)
	}
	if stepErr := report.Step("source commit fault request", nil); stepErr != nil {
		return stepErr
	}
	if restoreErr != nil {
		return report.Step("restore source index after fault", restoreErr)
	}
	if faultStatus != 500 || failureCode(faultBody) != "storage_error" {
		return fmt.Errorf("commit fault returned status=%d code=%s, want 500 storage_error",
			faultStatus, failureCode(faultBody))
	}
	if faultAsset.ID != "" {
		return fmt.Errorf("commit-faulted source returned metadata id=%q, want empty", faultAsset.ID)
	}
	faulted, status, err := harness.getSource(ctx, "source_ingest_commit_fault")
	if err != nil {
		return err
	}
	if status != 404 || faulted.ID != "" {
		return fmt.Errorf("commit-faulted source became visible: status=%d id=%s", status, faulted.ID)
	}
	return nil
}

func runCompose(ctx context.Context, harness *Harness, report *RunReport) error {
	first, err := sineWAV(48000, 1, 300, 220, 0.35)
	if err != nil {
		return err
	}
	second, err := sineWAV(44100, 2, 250, 330, 0.25)
	if err != nil {
		return err
	}
	if _, status, err := harness.createSource(ctx, "source_compose_a", "first span", first); err != nil || status != 201 {
		return fmt.Errorf("first source setup failed: status=%d err=%v", status, err)
	}
	if _, status, err := harness.createSource(ctx, "source_compose_b", "second span", second); err != nil || status != 201 {
		return fmt.Errorf("second source setup failed: status=%d err=%v", status, err)
	}
	validSpans := []domain.TimelineSpan{
		{SourceID: "source_compose_a", SourceStartFrame: 0, SourceEndFrame: 14400, GainDB: -3, CrossfadeFrames: 0},
		{SourceID: "source_compose_b", SourceStartFrame: 0, SourceEndFrame: 11025, GainDB: 1, CrossfadeFrames: 2400},
	}
	plan, status, err := harness.createTimeline(ctx, "timeline_compose_01", "two span assembly", 48000, 2, -1, validSpans)
	if err != nil {
		return err
	}
	if status != 201 || plan.State != "ready" {
		return fmt.Errorf("timeline write returned status=%d state=%s", status, plan.State)
	}
	readBack, status, err := harness.getTimeline(ctx, plan.ID)
	if err != nil {
		return err
	}
	if status != 200 || readBack.OutputFrames != plan.OutputFrames || len(readBack.Spans) != 2 {
		return fmt.Errorf("timeline read-back mismatch: status=%d frames=%d", status, readBack.OutputFrames)
	}
	invalidSpans := append([]domain.TimelineSpan(nil), validSpans...)
	invalidSpans[1].CrossfadeFrames = 999999
	_, status, err = harness.createTimeline(ctx, "timeline_compose_bad", "invalid crossfade", 48000, 2, -1, invalidSpans)
	if err != nil {
		return err
	}
	if status != 422 {
		return fmt.Errorf("invalid crossfade returned status %d, want 422", status)
	}
	return nil
}

func runRender(ctx context.Context, harness *Harness, report *RunReport) error {
	first, err := sineWAV(48000, 1, 400, 220, 0.25)
	if err != nil {
		return err
	}
	second, err := sineWAV(44100, 2, 350, 330, 0.2)
	if err != nil {
		return err
	}
	if _, status, err := harness.createSource(ctx, "source_render_a", "opening take", first); err != nil || status != 201 {
		return fmt.Errorf("opening source setup failed: status=%d err=%v", status, err)
	}
	if _, status, err := harness.createSource(ctx, "source_render_b", "closing take", second); err != nil || status != 201 {
		return fmt.Errorf("closing source setup failed: status=%d err=%v", status, err)
	}
	spans := []domain.TimelineSpan{
		{SourceID: "source_render_a", SourceStartFrame: 0, SourceEndFrame: 19200, GainDB: -2, CrossfadeFrames: 0},
		{SourceID: "source_render_b", SourceStartFrame: 0, SourceEndFrame: 15435, GainDB: 0, CrossfadeFrames: 4800},
	}
	plan, status, err := harness.createTimeline(ctx, "timeline_render_01", "crossfaded narration", 48000, 2, -1, spans)
	if err != nil {
		return err
	}
	if status != 201 {
		return fmt.Errorf("timeline setup returned status=%d", status)
	}
	job, status, err := harness.createRender(ctx, "render_mixdown_01", plan.ID)
	if err != nil {
		return err
	}
	if status != 201 || job.State != domain.RenderCompleted || job.Output == nil {
		return fmt.Errorf("render returned status=%d state=%s output=%v", status, job.State, job.Output)
	}
	fetched, status, err := harness.getRender(ctx, job.ID)
	if err != nil {
		return err
	}
	if status != 200 || fetched.State != domain.RenderCompleted {
		return fmt.Errorf("render read-back returned status=%d state=%s", status, fetched.State)
	}
	receipt, status, err := harness.getReceipt(ctx, job.ID)
	if err != nil {
		return err
	}
	if status != 200 || receipt.Output.FrameCount != plan.OutputFrames {
		return fmt.Errorf("receipt frame mismatch: status=%d receipt=%d plan=%d", status, receipt.Output.FrameCount, plan.OutputFrames)
	}
	audioBytes, status, err := harness.getOutput(ctx, job.ID)
	if err != nil {
		return err
	}
	if status != 200 {
		return fmt.Errorf("output returned status %d", status)
	}
	digest := sha256.Sum256(audioBytes)
	if hex.EncodeToString(digest[:]) != receipt.Output.Digest {
		return fmt.Errorf("output digest does not match receipt")
	}
	clip, info, err := audiocodec.DecodeWAV(audioBytes)
	if err != nil {
		return fmt.Errorf("decode rendered output: %w", err)
	}
	measured := audiocodec.Measure(clip.Samples)
	if info.FrameCount != receipt.Output.FrameCount {
		return fmt.Errorf("decoded frame count %d != receipt %d", info.FrameCount, receipt.Output.FrameCount)
	}
	if math.Abs(measured.PeakDBFS-(-1)) > 0.25 {
		return fmt.Errorf("output peak %.3f dBFS does not meet -1 dBFS target", measured.PeakDBFS)
	}

	// Real publication-boundary fault: the job reaches finalizing and the
	// output WAV plus receipt are fully staged, but the atomic publication
	// rename is sabotaged. The job must land in a durable failed terminal
	// state with no downloadable output.
	fault, err := armCommitFault(harness.dataDir, "renders")
	if err != nil {
		return report.Step("arm render publish fault", err)
	}
	faultJob, faultStatus, faultBody, faultErr := harness.createRenderWithBody(ctx, "render_publish_fault_01", plan.ID)
	restoreErr := fault.Restore()
	if faultErr != nil {
		return report.Step("render publish fault request", faultErr)
	}
	if stepErr := report.Step("render publish fault request", nil); stepErr != nil {
		return stepErr
	}
	if restoreErr != nil {
		return report.Step("restore render index after fault", restoreErr)
	}
	if faultStatus != 500 || failureCode(faultBody) != "storage_error" {
		return fmt.Errorf("publish fault returned status=%d code=%s, want 500 storage_error",
			faultStatus, failureCode(faultBody))
	}
	if faultJob.ID != "render_publish_fault_01" || faultJob.State != domain.RenderFailed {
		return fmt.Errorf("publish fault job state=%q id=%q, want failed terminal job", faultJob.State, faultJob.ID)
	}
	if faultJob.Error == nil || faultJob.Error.Code != "storage_error" {
		return fmt.Errorf("publish fault job error=%+v, want storage_error code", faultJob.Error)
	}
	if faultJob.Output != nil || faultJob.ReceiptDigest != "" {
		return fmt.Errorf("failed publish job carries output metadata")
	}
	if outputStatus, _, err := harness.requestJSON(ctx, "GET", "/v1/renders/render_publish_fault_01/output", nil); err != nil {
		return err
	} else if outputStatus != 409 {
		return fmt.Errorf("failed render output returned status=%d, want 409", outputStatus)
	}
	return nil
}

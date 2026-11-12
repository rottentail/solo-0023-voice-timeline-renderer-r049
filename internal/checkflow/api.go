package checkflow

import (
	"context"
	"encoding/base64"
	"fmt"

	"voice-timeline-renderer/internal/domain"
	"voice-timeline-renderer/internal/service"
)

type failureEnvelope struct {
	Error domain.ErrorDetail `json:"error"`
	Job   *domain.RenderJob  `json:"job,omitempty"`
}

// createSourceWithBody returns the decoded asset (zero on failure), the HTTP
// status, and the raw response body so failure-path checks can inspect the
// stable error envelope.
func (harness *Harness) createSourceWithBody(
	ctx context.Context,
	id string,
	name string,
	wav []byte,
) (domain.SourceAsset, int, []byte, error) {
	status, body, err := harness.requestJSON(ctx, "PUT", "/v1/sources/"+id, service.SourceWriteRequest{
		Name:      name,
		WAVBase64: base64.StdEncoding.EncodeToString(wav),
	})
	if err != nil {
		return domain.SourceAsset{}, status, body, err
	}
	var asset domain.SourceAsset
	if status == 200 || status == 201 {
		if err := decodeBody(body, &asset); err != nil {
			return domain.SourceAsset{}, status, body, err
		}
	}
	return asset, status, body, nil
}

func (harness *Harness) createSource(
	ctx context.Context,
	id string,
	name string,
	wav []byte,
) (domain.SourceAsset, int, error) {
	asset, status, _, err := harness.createSourceWithBody(ctx, id, name, wav)
	return asset, status, err
}

func (harness *Harness) getSource(
	ctx context.Context,
	id string,
) (domain.SourceAsset, int, error) {
	status, body, err := harness.requestJSON(ctx, "GET", "/v1/sources/"+id, nil)
	if err != nil {
		return domain.SourceAsset{}, status, err
	}
	var asset domain.SourceAsset
	if status == 200 {
		if err := decodeBody(body, &asset); err != nil {
			return domain.SourceAsset{}, status, err
		}
	}
	return asset, status, nil
}

// getSourceStatus performs a read without requiring a success body, which the
// reopen reader needs for 404 adjudications.
func (harness *Harness) getSourceStatus(ctx context.Context, id string) (int, error) {
	status, _, err := harness.requestJSON(ctx, "GET", "/v1/sources/"+id, nil)
	return status, err
}

func (harness *Harness) createTimeline(
	ctx context.Context,
	id string,
	name string,
	sampleRate int,
	channels int,
	targetPeak float64,
	spans []domain.TimelineSpan,
) (domain.Timeline, int, error) {
	status, body, err := harness.requestJSON(ctx, "PUT", "/v1/timelines/"+id, service.TimelineWriteRequest{
		Name:           name,
		SampleRate:     sampleRate,
		OutputChannels: channels,
		TargetPeakDBFS: targetPeak,
		Spans:          spans,
	})
	if err != nil {
		return domain.Timeline{}, status, err
	}
	var plan domain.Timeline
	if status == 200 || status == 201 {
		if err := decodeBody(body, &plan); err != nil {
			return domain.Timeline{}, status, err
		}
	}
	return plan, status, nil
}

func (harness *Harness) getTimeline(
	ctx context.Context,
	id string,
) (domain.Timeline, int, error) {
	status, body, err := harness.requestJSON(ctx, "GET", "/v1/timelines/"+id, nil)
	if err != nil {
		return domain.Timeline{}, status, err
	}
	var plan domain.Timeline
	if status == 200 {
		if err := decodeBody(body, &plan); err != nil {
			return domain.Timeline{}, status, err
		}
	}
	return plan, status, nil
}

func (harness *Harness) getTimelineStatus(ctx context.Context, id string) (int, error) {
	status, _, err := harness.requestJSON(ctx, "GET", "/v1/timelines/"+id, nil)
	return status, err
}

func (harness *Harness) createRender(
	ctx context.Context,
	id string,
	timelineID string,
) (domain.RenderJob, int, error) {
	job, status, _, err := harness.createRenderWithBody(ctx, id, timelineID)
	return job, status, err
}

// createRenderWithBody also returns the raw response body so failure-path
// checks can inspect the stable error envelope alongside the terminal job.
func (harness *Harness) createRenderWithBody(
	ctx context.Context,
	id string,
	timelineID string,
) (domain.RenderJob, int, []byte, error) {
	status, body, err := harness.requestJSON(ctx, "PUT", "/v1/renders/"+id, service.RenderWriteRequest{
		TimelineID: timelineID,
	})
	if err != nil {
		return domain.RenderJob{}, status, body, err
	}
	var envelope failureEnvelope
	if status >= 400 {
		if decodeErr := decodeBody(body, &envelope); decodeErr != nil {
			return domain.RenderJob{}, status, body, decodeErr
		}
		if envelope.Job != nil {
			return *envelope.Job, status, body, nil
		}
		return domain.RenderJob{}, status, body, nil
	}
	var job domain.RenderJob
	if err := decodeBody(body, &job); err != nil {
		return domain.RenderJob{}, status, body, err
	}
	return job, status, body, nil
}

func (harness *Harness) getRender(
	ctx context.Context,
	id string,
) (domain.RenderJob, int, error) {
	status, body, err := harness.requestJSON(ctx, "GET", "/v1/renders/"+id, nil)
	if err != nil {
		return domain.RenderJob{}, status, err
	}
	var job domain.RenderJob
	if status == 200 {
		if err := decodeBody(body, &job); err != nil {
			return domain.RenderJob{}, status, err
		}
	}
	return job, status, nil
}

func (harness *Harness) getRenderStatus(ctx context.Context, id string) (int, error) {
	status, _, err := harness.requestJSON(ctx, "GET", "/v1/renders/"+id, nil)
	return status, err
}

func (harness *Harness) getReceipt(
	ctx context.Context,
	id string,
) (domain.RenderReceipt, int, error) {
	status, body, err := harness.requestJSON(ctx, "GET", "/v1/renders/"+id+"/receipt", nil)
	if err != nil {
		return domain.RenderReceipt{}, status, err
	}
	var receipt domain.RenderReceipt
	if status == 200 {
		if err := decodeBody(body, &receipt); err != nil {
			return domain.RenderReceipt{}, status, err
		}
	}
	return receipt, status, nil
}

func (harness *Harness) getOutput(
	ctx context.Context,
	id string,
) ([]byte, int, error) {
	status, body, err := harness.requestJSON(ctx, "GET", "/v1/renders/"+id+"/output", nil)
	if err != nil {
		return nil, status, err
	}
	if status != 200 {
		var envelope failureEnvelope
		_ = decodeBody(body, &envelope)
		return nil, status, fmt.Errorf("unexpected output status %d with error %s", status, envelope.Error.Code)
	}
	return body, status, nil
}

func failureCode(body []byte) string {
	var envelope failureEnvelope
	if err := decodeBody(body, &envelope); err != nil {
		return ""
	}
	return envelope.Error.Code
}

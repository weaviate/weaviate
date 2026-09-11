//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package batch

import (
	"context"
	"math"
	"time"

	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

const (
	holdRecheckInterval = 250 * time.Millisecond
	ackDelayExponent    = 2
)

type admissionChecker interface {
	Refresh(updateMappings bool)
	CheckAlloc(sizeInBytes int64) error
	Ratio() float64
}

var _ admissionChecker = (*memwatch.Monitor)(nil)

// delayAck is the soft backpressure. It calculates and reports the
// live heap ratio then sleeps for a duration determined by the ratio.
func (h *StreamHandler) delayAck(ctx context.Context, heapRatio float64) {
	if h.metrics != nil {
		h.metrics.OnLiveHeapRatio(heapRatio)
	}

	if delay := ackDelay(h.config, heapRatio); delay > 0 {
		h.wait(ctx, delay)
	}
}

// ackDelay is how long to wait before acknowledging a message at the given live
// heap ratio. It uses exponential linear normalization between the engage and gate ratios,
// with ackDelayExponent defined as above (quadratic as it stands).
func ackDelay(cfg config.BatchStream, ratio float64) time.Duration {
	if cfg.GateRatio <= cfg.EngageRatio {
		return 0
	}
	switch {
	case math.IsNaN(ratio), ratio < 0:
		ratio = 0
	case ratio > 1:
		ratio = 1
	}
	if ratio <= cfg.EngageRatio {
		return 0
	}
	if ratio >= cfg.GateRatio {
		return cfg.MaxAckDelay
	}
	scaled := (ratio - cfg.EngageRatio) / (cfg.GateRatio - cfg.EngageRatio)
	return time.Duration(float64(cfg.MaxAckDelay) * math.Pow(scaled, ackDelayExponent))
}

// wait waits for duration d or until the context or shutting down context is done, whichever comes first.
//
// It returns true if the wait ran to completion, and false otherwise.
func (h *StreamHandler) wait(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-h.shuttingDownCtx.Done():
		return h.shuttingDownCtx.Err()
	}
}

// tryAdmit reports whether a message of size bytes fits on top of the memory
// already in flight, and reserves those bytes when it does.
// The function locks admitMu to ensure that the check and potential reservation of memory are atomic.
func (h *StreamHandler) tryAdmit(size int64) (float64, error) {
	h.admitMu.Lock()
	defer h.admitMu.Unlock()

	h.admissionChecker.Refresh(false)
	err := h.admissionChecker.CheckAlloc(size + h.memInFlight.Load())
	if err == nil {
		h.memInFlight.Add(size)
	}
	return h.admissionChecker.Ratio(), err
}

// holdForMemory is the memory gate. It repeatedly attempts to admit the specified memory size,
// waiting for a short interval between attempts, until it either succeeds or the hold period expires.
func (h *StreamHandler) holdForMemory(ctx context.Context, size int64) (float64, error) {
	heapRatio, err := h.tryAdmit(size)
	if err == nil {
		return heapRatio, nil
	}

	deadline := time.Now().Add(time.Duration(h.config.HoldSeconds) * time.Second)
	for {
		wait := min(holdRecheckInterval, time.Until(deadline))
		if wait <= 0 {
			return 0, err
		}
		if wErr := h.wait(ctx, wait); wErr != nil {
			return 0, wErr
		}
		if heapRatio, err = h.tryAdmit(size); err == nil {
			return heapRatio, nil
		}
	}
}

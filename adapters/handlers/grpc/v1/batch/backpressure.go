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
	// holdRecheckInterval is how long a held message waits between memory
	// re-checks. A wait still ends at once when the stream ends or the server
	// shuts down, because sleepSlice selects on both of those signals.
	holdRecheckInterval = 250 * time.Millisecond

	// ackDelayExponent is the simplest exponent that curves the delay upward.
	// Halfway between the engage and gate ratios the delay is a quarter of the
	// maximum.
	ackDelayExponent = 2
)

// admissionChecker decides whether a batch message may be admitted. It also
// reports live heap as a fraction of the memory limit. The receiver calls
// nothing else on a checker.
type admissionChecker interface {
	Refresh(updateMappings bool)
	CheckAlloc(sizeInBytes int64) error
	Ratio() float64
}

// The default checker Start builds is a *memwatch.Monitor, so it has to satisfy
// the interface.
var _ admissionChecker = (*memwatch.Monitor)(nil)

// delayAck is the soft backpressure. The message is already with the workers;
// only its Ack is held back, for longer the closer live heap is to the gate. A
// client that waits for acks before sending more slows down before it reaches
// the gate. The wait never runs in a worker: in-flight memory is freed only as
// batches complete, so the workers must keep draining.
func (h *StreamHandler) delayAck(ctx context.Context) {
	ratio := h.admissionChecker.Ratio()
	if h.metrics != nil {
		h.metrics.OnLiveHeapRatio(ratio)
	}

	if delay := ackDelay(h.config, ratio); delay > 0 {
		h.sleepSlice(ctx, delay)
	}
}

// ackDelay is how long to wait before acknowledging a message at the given live
// heap ratio. It is zero at or below the engage ratio, and cfg.MaxAckDelay at
// or above the gate ratio. In between it rises along a curve that stays below
// the straight line joining those two points. If the gate ratio is at or below
// the engage ratio the curve has no room to rise, so the delay is zero for every
// ratio. A ratio that is NaN, negative, or above one is clamped into [0, 1].
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

// sleepSlice waits d and reports whether the wait ran to completion. It returns
// false as soon as the stream ends or the server begins shutting down. The
// receiver only starts its grace period once shuttingDownCtx is done, so that
// signal always fires first and no other deadline applies here.
func (h *StreamHandler) sleepSlice(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	case <-h.shuttingDownCtx.Done():
		return false
	}
}

// tryAdmit reports whether a message of size bytes fits on top of the memory
// already in flight, and reserves those bytes when it does. The check and the
// reservation happen under one lock. A second stream's check therefore counts
// the first stream's reservation, and two streams cannot both pass against the
// same free memory. Nothing that blocks runs under admitMu.
func (h *StreamHandler) tryAdmit(size int64) error {
	h.admitMu.Lock()
	defer h.admitMu.Unlock()

	h.admissionChecker.Refresh(false)
	err := h.admissionChecker.CheckAlloc(size + h.memInFlight.Load())
	if err == nil {
		h.memInFlight.Add(size)
	}
	return err
}

// holdForMemory is the memory gate. A failed check is retried every
// holdRecheckInterval until it passes or the hold expires; only then does the
// caller take the OutOfMemory path. A held stream is stalled, not slowed:
// slowing a client before the gate is delayAck's job. A passing check reserves
// the message's size inside tryAdmit; a failing one reserves nothing.
func (h *StreamHandler) holdForMemory(ctx context.Context, size int64) error {
	err := h.tryAdmit(size)
	if err == nil {
		return nil
	}

	deadline := time.Now().Add(time.Duration(h.config.HoldSeconds) * time.Second)
	for {
		wait := min(holdRecheckInterval, time.Until(deadline))
		if wait <= 0 || !h.sleepSlice(ctx, wait) {
			return err
		}
		if err = h.tryAdmit(size); err == nil {
			return nil
		}
	}
}

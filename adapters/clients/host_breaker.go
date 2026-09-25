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

package clients

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/failsafe-go/failsafe-go/circuitbreaker"

	"github.com/weaviate/weaviate/usecases/replica"
)

const (
	// hostBreakerThreshold is small because the failures counted are node-wide
	hostBreakerThreshold = 3

	// hostBreakerCoolOff is short on purpose: a pod back in under a second must be picked up at once
	hostBreakerCoolOff = 500 * time.Millisecond

	// hostBreakerMaxCoolOff caps the escalation, so only a node that stays down is left alone for long
	hostBreakerMaxCoolOff = 5 * time.Second

	// hostBreakerCoolOffFactor is how much harder each consecutive open backs off
	hostBreakerCoolOffFactor = 2

	// hostBreakerIdleTTL bounds the registry: host:port rotates on every restart
	hostBreakerIdleTTL = time.Minute
)

// ErrHostCircuitOpen is returned instead of contacting a host whose breaker is open
var ErrHostCircuitOpen = replica.ErrHostCircuitOpen

// hostBreaker is one host's breaker plus when it was last used, for eviction
type hostBreaker struct {
	// mu guards breaker and coolOff: escalating the delay means building a replacement
	mu       sync.Mutex
	breaker  circuitbreaker.CircuitBreaker[any]
	coolOff  time.Duration
	failures int // reported in the refusal; the replacement breaker starts counting from zero
	lastSeen atomic.Int64
}

func (hb *hostBreaker) touch() {
	hb.lastSeen.Store(time.Now().UnixNano())
}

// refusal reports how long the host stays refused and the streak behind it; zero lets a probe through
func (hb *hostBreaker) refusal() (time.Duration, int) {
	hb.mu.Lock()
	defer hb.mu.Unlock()
	if hb.breaker.State() != circuitbreaker.OpenState {
		return 0, hb.failures
	}
	if remaining := hb.breaker.RemainingDelay(); remaining > 0 {
		return remaining, hb.failures
	}
	hb.breaker.HalfOpen()
	return 0, hb.failures
}

// probe half-opens the breaker for a caller that has no other replica left, so the outcome still counts
func (hb *hostBreaker) probe() {
	hb.mu.Lock()
	defer hb.mu.Unlock()
	if hb.breaker.State() == circuitbreaker.OpenState {
		hb.breaker.HalfOpen()
	}
}

// recordFailure re-opens the breaker with a doubled cool-off when a probe fails again, capped at maxCoolOff
func (hb *hostBreaker) recordFailure(newBreaker func(time.Duration) circuitbreaker.CircuitBreaker[any], maxCoolOff time.Duration) {
	hb.mu.Lock()
	defer hb.mu.Unlock()
	hb.failures++
	probing := hb.breaker.IsHalfOpen()
	hb.breaker.RecordFailure()
	if !probing || !hb.breaker.IsOpen() {
		return
	}
	hb.coolOff = min(hb.coolOff*hostBreakerCoolOffFactor, maxCoolOff)
	hb.breaker = newBreaker(hb.coolOff)
	hb.breaker.Open()
}

// recordSuccess reports whether the breaker closed, which resets the escalated cool-off
func (hb *hostBreaker) recordSuccess(coolOff time.Duration) bool {
	hb.mu.Lock()
	defer hb.mu.Unlock()
	hb.breaker.RecordSuccess()
	if !hb.breaker.IsClosed() {
		return false
	}
	hb.failures = 0
	hb.coolOff = coolOff
	return true
}

// hostBreakers tracks replica health per host:port; only failing hosts get an entry
type hostBreakers struct {
	mu    sync.RWMutex
	hosts map[string]*hostBreaker

	// fields, not constants, so tests need not sleep for the production values
	threshold  uint
	coolOff    time.Duration
	maxCoolOff time.Duration
}

func newHostBreakers() *hostBreakers {
	return &hostBreakers{
		hosts:      make(map[string]*hostBreaker),
		threshold:  hostBreakerThreshold,
		coolOff:    hostBreakerCoolOff,
		maxCoolOff: hostBreakerMaxCoolOff,
	}
}

// newBreaker opens after threshold consecutive failures and half-opens after the cool-off
func (b *hostBreakers) newBreaker(coolOff time.Duration) circuitbreaker.CircuitBreaker[any] {
	return circuitbreaker.NewBuilder[any]().
		WithFailureThreshold(b.threshold).
		WithSuccessThreshold(1).
		WithDelay(coolOff).
		Build()
}

// allow bypasses failsafe's permit API: a cancelled probe would leak the permit and strand the host
func (b *hostBreakers) allow(ctx context.Context, host string) error {
	b.mu.RLock()
	hb := b.hosts[host]
	b.mu.RUnlock()
	if hb == nil {
		return nil
	}
	hb.touch()

	remaining, failures := hb.refusal()
	if remaining <= 0 {
		return nil
	}
	if replica.HostBreakerBypassed(ctx) {
		hb.probe() // the caller has no other replica: let it through and let its answer decide
		return nil
	}
	return fmt.Errorf("%w: %s (%d consecutive failures, retrying in %s)",
		ErrHostCircuitOpen, host, failures, remaining.Round(time.Millisecond))
}

// observe records a finished call's outcome; errors from the caller giving up are ignored
func (b *hostBreakers) observe(ctx context.Context, host string, err error) {
	switch {
	case err == nil:
		b.succeeded(host)
	case ctx.Err() != nil:
	case hostLevelFailure(err):
		b.failed(host)
	default:
		b.succeeded(host)
	}
}

// succeeded records a healthy answer and drops the host once its breaker closes
func (b *hostBreakers) succeeded(host string) {
	b.mu.RLock()
	tracked := b.hosts[host] != nil
	b.mu.RUnlock()
	if !tracked {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	hb := b.hosts[host]
	if hb == nil {
		return
	}
	if hb.recordSuccess(b.coolOff) {
		delete(b.hosts, host)
	}
}

func (b *hostBreakers) failed(host string) {
	b.mu.Lock()
	hb := b.hosts[host]
	if hb == nil {
		b.evictIdle()
		hb = &hostBreaker{breaker: b.newBreaker(b.coolOff), coolOff: b.coolOff}
		b.hosts[host] = hb
	}
	b.mu.Unlock()

	hb.touch()
	hb.recordFailure(b.newBreaker, b.maxCoolOff)
}

// evictIdle drops hosts nothing has asked about within hostBreakerIdleTTL
func (b *hostBreakers) evictIdle() {
	cutoff := time.Now().Add(-hostBreakerIdleTTL).UnixNano()
	for host, hb := range b.hosts {
		if hb.lastSeen.Load() < cutoff {
			delete(b.hosts, host)
		}
	}
}

// hostLevelFailure reports whether err means the node itself could not serve
func hostLevelFailure(err error) bool {
	if httpErr, ok := AsHTTPError(err); ok {
		switch httpErr.Code {
		case http.StatusServiceUnavailable, http.StatusBadGateway, http.StatusGatewayTimeout, http.StatusTooManyRequests:
			return true
		default:
			return false
		}
	}
	return true
}

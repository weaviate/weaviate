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
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/failsafe-go/failsafe-go/circuitbreaker"
)

const (
	// hostBreakerThreshold is small because the failures counted are node-wide
	hostBreakerThreshold = 3

	// hostBreakerCoolOff is short on purpose: a pod back in under a second must be picked up at once,
	// and a refusal then costs a read one attempt rather than the read itself
	hostBreakerCoolOff = 500 * time.Millisecond

	// hostBreakerIdleTTL bounds the registry: host:port rotates on every restart
	hostBreakerIdleTTL = time.Minute
)

// ErrHostCircuitOpen is returned instead of contacting a host whose breaker is open. It costs the
// caller one replica, never the request: the next attempt after the cool-off is let through as a probe.
var ErrHostCircuitOpen = errors.New("replica host marked unhealthy by circuit breaker")

// hostBreaker is one host's breaker plus when it was last used, for eviction
type hostBreaker struct {
	breaker  circuitbreaker.CircuitBreaker[any]
	lastSeen atomic.Int64
}

func (hb *hostBreaker) touch() {
	hb.lastSeen.Store(time.Now().UnixNano())
}

// hostBreakers tracks replica health per host:port; only failing hosts get an entry
type hostBreakers struct {
	mu    sync.RWMutex
	hosts map[string]*hostBreaker

	// fields, not constants, so tests need not sleep for the production values
	threshold uint
	coolOff   time.Duration
}

func newHostBreakers() *hostBreakers {
	return &hostBreakers{
		hosts:     make(map[string]*hostBreaker),
		threshold: hostBreakerThreshold,
		coolOff:   hostBreakerCoolOff,
	}
}

// newBreaker opens after threshold consecutive failures and half-opens after the cool-off
func (b *hostBreakers) newBreaker() circuitbreaker.CircuitBreaker[any] {
	return circuitbreaker.NewBuilder[any]().
		WithFailureThreshold(b.threshold).
		WithSuccessThreshold(1).
		WithDelay(b.coolOff).
		Build()
}

// allow bypasses failsafe's permit API: a cancelled probe would leak the permit and strand the host
func (b *hostBreakers) allow(host string) error {
	b.mu.RLock()
	hb := b.hosts[host]
	b.mu.RUnlock()
	if hb == nil {
		return nil
	}
	hb.touch()

	if hb.breaker.State() != circuitbreaker.OpenState {
		return nil
	}
	remaining := hb.breaker.RemainingDelay()
	if remaining <= 0 {
		hb.breaker.HalfOpen() // the cool-off elapsed, so this call is the probe that decides
		return nil
	}
	return fmt.Errorf("%w: %s (retrying in %s)", ErrHostCircuitOpen, host, remaining.Round(time.Millisecond))
}

// unhealthy reports whether this host is currently refused; unlike allow it never half-opens
func (b *hostBreakers) unhealthy(host string) bool {
	b.mu.RLock()
	hb := b.hosts[host]
	b.mu.RUnlock()
	if hb == nil {
		return false
	}
	return hb.breaker.State() == circuitbreaker.OpenState && hb.breaker.RemainingDelay() > 0
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
	hb.breaker.RecordSuccess()
	if hb.breaker.IsClosed() {
		delete(b.hosts, host)
	}
}

func (b *hostBreakers) failed(host string) {
	b.mu.Lock()
	hb := b.hosts[host]
	if hb == nil {
		b.evictIdle()
		hb = &hostBreaker{breaker: b.newBreaker()}
		b.hosts[host] = hb
	}
	b.mu.Unlock()

	hb.touch()
	hb.breaker.RecordFailure()
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

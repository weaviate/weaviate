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

// Package usagelimits enforces server-side usage limits configured via env
// vars and runtime overrides.
//
// Object enforcement lives here: Manager.CheckObjects sums per-shard async
// counts via an injected ObjectCounter and returns a *LimitExceededError if
// the next write would push the live total past MAXIMUM_ALLOWED_OBJECTS_COUNT.
// The check is invoked from the storage chokepoint (Shard.PutObject{,Batch}
// in adapters/repos/db/) so it covers both local and forwarded writes for
// RF=1.
//
// Schema-side limits (collections, tenants, shards) do their own counting
// inline in usecases/schema/ and only reach into this package for the typed
// error and the operator-overridable message template via NewLimitExceededError.
//
// See docs/usage_limits.md for the full design.
package usagelimits

import (
	"context"
	"fmt"

	"github.com/weaviate/weaviate/usecases/config/runtime"
)

// ObjectCounter sums object counts across all locally-owned shards. The
// implementation must use the async (CountAsync) path because synchronous
// counting on every write is unacceptable on hot paths. Brief overshoot
// during fast bulk imports is documented and accepted; it self-corrects on
// the next memtable flush.
type ObjectCounter interface {
	LocalObjectCount(ctx context.Context) (int64, error)
}

// Config is the read-only view of usage-limit configuration the Manager
// needs. Values are runtime-overrideable; the Manager re-reads them via
// DynamicValue.Get() on every check, so SIGHUP / file-watcher updates take
// effect without restart. A nil DynamicValue is treated as "unlimited".
type Config struct {
	// ErrorMessage is the operator-overridable template used to render the
	// user-facing error message (see RenderTemplate).
	ErrorMessage *runtime.DynamicValue[string]
	// MaxObjectsCount caps per-instance live object count. <0 means unlimited.
	MaxObjectsCount *runtime.DynamicValue[int]
}

// Manager is the policy gate for the object-count limit. Constructed once
// at startup; the wire-protocol mapping (HTTP 429 / gRPC RESOURCE_EXHAUSTED)
// lives in adapters/handlers/*.
type Manager struct {
	cfg     Config
	counter ObjectCounter
}

// NewManager constructs a Manager. counter may be nil for tests; in that
// case CheckObjects is a no-op when MaxObjectsCount is unset, and returns
// an error if a cap is configured (loud rather than silent misconfiguration).
func NewManager(cfg Config, counter ObjectCounter) *Manager {
	return &Manager{cfg: cfg, counter: counter}
}

// CheckObjects rejects when (currentObjects + n) would exceed the cap. n is
// the number of objects this request would add (1 for single writes,
// len(batch) for batches). Whole-batch-rejection is the caller's
// responsibility — the caller passes len(batch) and rejects the entire
// request on a non-nil return.
func (m *Manager) CheckObjects(ctx context.Context, n int64) error {
	if m == nil {
		return nil
	}
	cap := readLimit(m.cfg.MaxObjectsCount)
	if cap < 0 {
		return nil
	}
	if m.counter == nil {
		return fmt.Errorf("usagelimits: object limit configured but no counter wired")
	}
	current, err := m.counter.LocalObjectCount(ctx)
	if err != nil {
		return fmt.Errorf("usagelimits: counting objects: %w", err)
	}
	if current+n > cap {
		return NewLimitExceededError(m.template(), LimitObjects, cap)
	}
	return nil
}

// HasObjectCap reports whether MaxObjectsCount is configured (>= 0) at
// the moment of the call.
func (m *Manager) HasObjectCap() bool {
	if m == nil {
		return false
	}
	return readLimit(m.cfg.MaxObjectsCount) >= 0
}

func (m *Manager) template() string {
	if m == nil || m.cfg.ErrorMessage == nil {
		return ""
	}
	return m.cfg.ErrorMessage.Get()
}

// readLimit reads a *DynamicValue[int] safely, returning -1 (unlimited) for
// nil. DynamicValue.Get() returns the int zero value (0) for nil — which
// would translate to "zero allowed" — so the nil check has to live here.
func readLimit(dv *runtime.DynamicValue[int]) int64 {
	if dv == nil {
		return -1
	}
	return int64(dv.Get())
}

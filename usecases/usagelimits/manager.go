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

package usagelimits

import (
	"context"
	"fmt"

	"github.com/weaviate/weaviate/usecases/config/runtime"
)

// ObjectCounter sums object counts across all locally-owned shards. The
// Manager calls this on the runtime path of every CheckObjects(); the
// implementation must use the async (CountAsync) path because synchronous
// counting on every write is unacceptable on hot paths. Brief overshoot
// during fast bulk imports is documented and accepted; it self-corrects
// on the next memtable flush.
type ObjectCounter interface {
	LocalObjectCount(ctx context.Context) (int64, error)
}

// CollectionCounter returns the current collection count. Counts the same
// set of classes the existing MaximumAllowedCollectionsCount check counts —
// implemented over the schema reader / RAFT-backed schema state.
type CollectionCounter interface {
	LocalCollectionCount(ctx context.Context) (int64, error)
}

// TenantCounter returns the current tenant count for the named class.
// Implementations read from the schema state; tenants are checked at create
// time only, not on subsequent multi-tenancy config changes (this is a
// guardrail, not a security boundary).
type TenantCounter interface {
	LocalTenantCount(ctx context.Context, class string) (int64, error)
}

// Config is the read-only view of usage-limit configuration the Manager
// needs. All values are runtime-overrideable; the Manager re-reads them
// (via DynamicValue.Get()) on every check, so SIGHUP / file-watcher
// updates take effect without restart.
//
// A nil DynamicValue is treated as "unlimited" so the Manager remains
// usable in tests and during early bootstrap before configuration is
// fully wired.
type Config struct {
	// Scope: declared unit of accounting. Today only "node" is implemented;
	// "cluster" / "namespace" are rejected at startup.
	Scope *runtime.DynamicValue[string]
	// ErrorMessage is the operator-overridable template for the user-facing
	// error message rendered into LimitExceededError.RenderedMessage.
	ErrorMessage *runtime.DynamicValue[string]

	// MaxObjectsCount caps per-instance live object count. <0 (incl. the
	// default -1) means unlimited.
	MaxObjectsCount *runtime.DynamicValue[int]
	// MaxCollectionsCount caps the number of collections (classes). Mirrors
	// the existing MaximumAllowedCollectionsCount semantics.
	MaxCollectionsCount *runtime.DynamicValue[int]
	// MaxTenantsPerCollection caps tenants per multi-tenant class.
	MaxTenantsPerCollection *runtime.DynamicValue[int]
	// MaxShardsPerCollection caps shards in a class create request.
	MaxShardsPerCollection *runtime.DynamicValue[int]
}

// Manager is the cross-cutting policy gate for usage limits. Consumed from
// usecases/objects and usecases/schema after authorization and before
// replication, on the coordinator only. Returns *LimitExceededError on a
// miss, or nil otherwise. Wire-protocol mapping (to HTTP 429 / gRPC
// RESOURCE_EXHAUSTED) lives in adapters/handlers/*.
type Manager struct {
	cfg               Config
	objectCounter     ObjectCounter
	collectionCounter CollectionCounter
	tenantCounter     TenantCounter
}

// NewManager constructs a Manager. Counters may be nil for paths the
// caller does not exercise (e.g. tests that only check shard limits) —
// but a nil counter combined with a configured limit on its corresponding
// Check* call returns an error rather than silently passing through, so
// misconfiguration is loud rather than silent.
func NewManager(
	cfg Config,
	objectCounter ObjectCounter,
	collectionCounter CollectionCounter,
	tenantCounter TenantCounter,
) *Manager {
	return &Manager{
		cfg:               cfg,
		objectCounter:     objectCounter,
		collectionCounter: collectionCounter,
		tenantCounter:     tenantCounter,
	}
}

// CheckObjects rejects when (currentObjects + n) would exceed
// MaxObjectsCount. n is the number of objects this request would add (1
// for single writes, len(batch) for batches). The whole-batch-rejection
// rule lives at the call site, not here — the caller passes len(batch)
// and rejects the entire request on a non-nil return.
func (m *Manager) CheckObjects(ctx context.Context, n int64) error {
	if m == nil {
		return nil
	}
	cap := readLimit(m.cfg.MaxObjectsCount)
	if cap < 0 {
		return nil
	}
	if m.objectCounter == nil {
		return fmt.Errorf("usagelimits: object limit configured but no counter wired")
	}
	current, err := m.objectCounter.LocalObjectCount(ctx)
	if err != nil {
		return fmt.Errorf("usagelimits: counting objects: %w", err)
	}
	if current+n > cap {
		return m.exceeded(LimitObjects, cap)
	}
	return nil
}

// CheckCollections rejects when (currentCollections + n) would exceed
// MaxCollectionsCount.
func (m *Manager) CheckCollections(ctx context.Context, n int64) error {
	if m == nil {
		return nil
	}
	cap := readLimit(m.cfg.MaxCollectionsCount)
	if cap < 0 {
		return nil
	}
	if m.collectionCounter == nil {
		return fmt.Errorf("usagelimits: collection limit configured but no counter wired")
	}
	current, err := m.collectionCounter.LocalCollectionCount(ctx)
	if err != nil {
		return fmt.Errorf("usagelimits: counting collections: %w", err)
	}
	if current+n > cap {
		return m.exceeded(LimitCollections, cap)
	}
	return nil
}

// CheckTenants rejects when (currentTenants + n) for the named class
// would exceed MaxTenantsPerCollection.
func (m *Manager) CheckTenants(ctx context.Context, class string, n int64) error {
	if m == nil {
		return nil
	}
	cap := readLimit(m.cfg.MaxTenantsPerCollection)
	if cap < 0 {
		return nil
	}
	if m.tenantCounter == nil {
		return fmt.Errorf("usagelimits: tenant limit configured but no counter wired")
	}
	current, err := m.tenantCounter.LocalTenantCount(ctx, class)
	if err != nil {
		return fmt.Errorf("usagelimits: counting tenants for %q: %w", class, err)
	}
	if current+n > cap {
		return m.exceeded(LimitTenants, cap)
	}
	return nil
}

// CheckShards rejects a class-create request whose sharding config asks
// for more shards than MaxShardsPerCollection. Config-time only; no live
// count needed.
func (m *Manager) CheckShards(requestedShards int) error {
	if m == nil {
		return nil
	}
	cap := readLimit(m.cfg.MaxShardsPerCollection)
	if cap < 0 {
		return nil
	}
	if int64(requestedShards) > cap {
		return m.exceeded(LimitShards, cap)
	}
	return nil
}

// CurrentScope returns the configured scope (defaulting to ScopeNode for
// unset / empty). Invariant: by the time a Manager exists, scope has
// already been validated at startup, so this never returns an unsupported
// value.
func (m *Manager) CurrentScope() Scope {
	if m == nil {
		return ScopeNode
	}
	s := m.cfg.Scope.Get()
	if s == "" {
		return ScopeNode
	}
	return Scope(s)
}

func (m *Manager) exceeded(limit LimitName, value int64) *LimitExceededError {
	return &LimitExceededError{
		Limit:           limit,
		Value:           value,
		RenderedMessage: RenderTemplate(m.cfg.ErrorMessage.Get(), limit, value),
	}
}

// readLimit reads a *DynamicValue[int] safely, returning -1 (unlimited) for
// nil. The DynamicValue.Get() method itself handles nil receivers, but it
// returns the int zero value (0) for nil — which would translate to "zero
// allowed". Treating nil as unlimited keeps the Manager usable when only a
// subset of counters/limits is wired (tests, partial config).
func readLimit(dv *runtime.DynamicValue[int]) int64 {
	if dv == nil {
		return -1
	}
	return int64(dv.Get())
}

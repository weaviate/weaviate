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

// Package lazyshardunloading is the licensed lazy shard unloading policy. It
// implements [db.ShardUnloader]: it caps the loaded shards of a node by count
// (max_loaded_shards) and heap (memory watermarks), and unloads idle shards
// (idle_ttl), without ever changing a tenant's status.
//
// A load at the limit hands the least recently used sampled shard to the
// unload workers and proceeds at once ("make room"); with no victim it goes
// over the limit ("overcommit"). A background loop drains toward the limits
// and sweeps idle shards at a bounded rate. Every unload goes through
// [db.LazyLoadShard.TryUnload], which only try-locks, so nothing here waits on
// a request, a load or a RAFT apply.
package lazyshardunloading

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db"
)

// Config wires an [Unloader].
type Config struct {
	Logger logrus.FieldLogger
	// Registerer receives the metrics; nil keeps them unregistered.
	Registerer prometheus.Registerer
	Knobs      Knobs
	// MemoryRatio is the heap ratio against GOMEMLIMIT (memwatch); nil reads 0.
	MemoryRatio func() float64
	// NamespacesEnabled adds the namespace label and enables namespace_max_share.
	NamespacesEnabled bool
	// NamespaceActive reports whether a namespace is active; only active
	// namespaces give up shards. nil treats every namespace as active.
	NamespaceActive func(namespace string) bool
}

// Unloader is the licensed [db.ShardUnloader]. Its goroutines start in New and
// stop in Close, which DB.Shutdown calls.
type Unloader struct {
	registry *db.LoadedShardRegistry
	p        *policy[*db.LazyLoadShard]
}

func New(cfg Config) *Unloader {
	logger := cfg.Logger
	if logger == nil {
		logger = logrus.New()
	}
	logger = logger.WithField("component", "lazy_shard_unloading")
	registry := db.NewLoadedShardRegistry()
	return &Unloader{
		registry: registry,
		p: newPolicy[*db.LazyLoadShard](registry, cfg.Knobs, policyDeps{
			logger:     logger,
			metrics:    newMetrics(cfg.Registerer, cfg.NamespacesEnabled, logger),
			clock:      db.AccessClockNow,
			memRatio:   cfg.MemoryRatio,
			nsActive:   cfg.NamespaceActive,
			namespaces: cfg.NamespacesEnabled,
		}),
	}
}

func (u *Unloader) Registry() *db.LoadedShardRegistry { return u.registry }

func (u *Unloader) BeforeLoad(shard *db.LazyLoadShard) { u.p.beforeLoad(shard) }

func (u *Unloader) AllowWarmupLoad() bool { return u.p.allowWarmupLoad() }

func (u *Unloader) Close(ctx context.Context) error { return u.p.close(ctx) }

func (u *Unloader) SetReindexActivitySource(src func(ctx context.Context) (db.ReindexActivity, error)) {
	u.p.setReindexSource(src)
}

// RefreshConfig applies changed knobs now instead of at the next tick; the runtime-config hook calls it.
func (u *Unloader) RefreshConfig() { u.p.refreshConfig() }

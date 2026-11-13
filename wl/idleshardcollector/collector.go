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

// Package idleshardcollector is the licensed idle shard collector: like a
// garbage collector for shards, it reclaims loaded shards nobody requested for
// idle_ttl, and the next request reloads them; tenant status never changes.
//
// It implements [db.ShardUnloader]. Loads are never refused or delayed, and
// nothing caps how many shards are loaded. Only data reads and writes count
// as access; comparisons, status reads and TTL finds do not. A background loop
// sweeps the loaded shards every check_interval and hands the idle ones to the
// unload workers, at most unload_rate per check. Every unload goes through
// [db.LazyLoadShard.TryUnload], which only try-locks, so nothing here waits on
// a request, a load or a RAFT apply.
package idleshardcollector

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db"
)

// Config wires a [Collector].
type Config struct {
	Logger logrus.FieldLogger
	// Registerer receives the metrics; nil keeps them unregistered.
	Registerer prometheus.Registerer
	Knobs      Knobs
	// NamespacesEnabled adds the namespace label to the loaded gauge.
	NamespacesEnabled bool
	// NamespaceActive reports whether a namespace is active; only active
	// namespaces give up shards. nil treats every namespace as active.
	NamespaceActive func(namespace string) bool
}

// Collector is the licensed [db.ShardUnloader]. Its goroutines start in New and
// stop in Close, which DB.Shutdown calls.
type Collector struct {
	registry *db.LoadedShardRegistry
	p        *policy[*db.LazyLoadShard]
}

func New(cfg Config) *Collector {
	logger := cfg.Logger
	if logger == nil {
		logger = logrus.New()
	}
	logger = logger.WithField("component", "idle_shard_collector")
	registry := db.NewLoadedShardRegistry()
	return &Collector{
		registry: registry,
		p: newPolicy[*db.LazyLoadShard](registry, cfg.Knobs, policyDeps{
			logger:     logger,
			metrics:    newMetrics(cfg.Registerer, cfg.NamespacesEnabled, logger),
			clock:      db.AccessClockNow,
			nsActive:   cfg.NamespaceActive,
			namespaces: cfg.NamespacesEnabled,
		}),
	}
}

func (u *Collector) Registry() *db.LoadedShardRegistry { return u.registry }

func (u *Collector) BeforeLoad(shard *db.LazyLoadShard) { u.p.beforeLoad(shard) }

func (u *Collector) Collecting() bool { return u.p.collecting() }

func (u *Collector) Close(ctx context.Context) error { return u.p.close(ctx) }

// RefreshConfig applies changed knobs now instead of at the next tick; the runtime-config hook calls it.
func (u *Collector) RefreshConfig() { u.p.refreshConfig() }

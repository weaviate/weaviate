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

package rest

import (
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/license"
	"github.com/weaviate/weaviate/usecases/namespaces"
)

// lazyShardUnloadingHookKey prefixes every LazyShardUnloading* runtime field.
const lazyShardUnloadingHookKey = "LazyShardUnloading"

// lazyShardUnloadingModeFor is the only code that pairs lazy loading with
// Config.WeaviateLicense. Unloading has no flag of its own: it is on wherever
// lazy loading is, which is unless it is explicitly disabled.
func lazyShardUnloadingModeFor(cfg config.Config) license.Mode {
	lazyLoading := cfg.EnableLazyLoadShards == nil || *cfg.EnableLazyLoadShards
	return license.ModeFor(lazyLoading, cfg.WeaviateLicense)
}

// lazyShardUnloaderFor returns the unloader the DB gets; build, the only wl/
// code of unloading, runs in FeatureLicensed alone. Every other mode returns
// nil, which keeps today's lazy loading, with no warning and no refusal.
func lazyShardUnloaderFor(mode license.Mode, build func() db.ShardUnloader) db.ShardUnloader {
	switch mode {
	case license.FeatureLicensed:
		return build()
	case license.FeatureOff, license.FeatureUnlicensed:
	}
	return nil
}

// shardUnloaderRefresher is the licensed unloader's hook entry, named here so this file stays clear of wl/.
type shardUnloaderRefresher interface{ RefreshConfig() }

// lazyShardUnloadingHooks returns the runtime-config hook that applies changed
// knobs at once; none without an unloader, so an unlicensed node logs nothing.
func lazyShardUnloadingHooks(u db.ShardUnloader) map[string]func() error {
	r, ok := u.(shardUnloaderRefresher)
	if !ok {
		return nil
	}
	return map[string]func() error{lazyShardUnloadingHookKey: func() error {
		r.RefreshConfig()
		return nil
	}}
}

// namespaceActiveFor reports whether a namespace is active; nil when namespaces are off.
func namespaceActiveFor(enabled bool, e namespaces.Exister) func(string) bool {
	if !enabled || e == nil {
		return nil
	}
	return func(name string) bool {
		ns, ok := e.GetNamespace(name)
		return ok && ns.State == api.NamespaceStateActive
	}
}

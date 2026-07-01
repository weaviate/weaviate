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

package db

import "context"

// DropVectorLiveOpsLookup returns the set of drop-vector edit-op IDs that still
// have a live (non-terminal) distributed task. Wired from the cluster layer, which
// can list the DTM tasks; the objects bucket uses it on shard load to sweep an
// orphaned op instead of re-arming it.
type DropVectorLiveOpsLookup func(ctx context.Context) (map[string]struct{}, error)

// SetDropVectorLiveOpsLookup installs the live-op lookup. Called once at startup
// from the wiring layer, after the DTM client exists.
func (db *DB) SetDropVectorLiveOpsLookup(fn DropVectorLiveOpsLookup) {
	db.dropVectorLiveOpsMu.Lock()
	db.dropVectorLiveOpsLookup = fn
	db.dropVectorLiveOpsMu.Unlock()
}

// dropVectorLiveOps resolves the live-op set via the installed lookup, or (nil,nil)
// when none is wired yet — the objects bucket then skips the orphan sweep and keeps
// the pre-existing re-arm behavior.
func (db *DB) dropVectorLiveOps(ctx context.Context) (map[string]struct{}, error) {
	db.dropVectorLiveOpsMu.RLock()
	fn := db.dropVectorLiveOpsLookup
	db.dropVectorLiveOpsMu.RUnlock()
	if fn == nil {
		return nil, nil
	}
	return fn(ctx)
}

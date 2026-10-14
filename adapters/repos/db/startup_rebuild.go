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

import (
	"slices"
	"sync"
)

// startupRebuild makes a rebuild enabled at startup, such as
// REINDEX_VECTOR_DIMENSIONS_AT_STARTUP, run on a shard once per process, not on
// every lazy reload.
type startupRebuild struct {
	enabled bool

	mu sync.Mutex
	// whether the rebuild succeeded
	outcomes map[rebuiltShard]bool
	objects  int64
}

// rebuiltShard is not keyed by shardId, which joins names that may contain its "_"
type rebuiltShard struct {
	index, shard string
}

func (r *startupRebuild) pending(index, shard string) bool {
	if r == nil || !r.enabled {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return !r.outcomes[rebuiltShard{index, shard}]
}

func (r *startupRebuild) record(index, shard string, objects int, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.outcomes == nil {
		r.outcomes = map[rebuiltShard]bool{}
	}
	r.outcomes[rebuiltShard{index, shard}] = ok
	if ok {
		r.objects += int64(objects)
	}
}

func (r *startupRebuild) done(index, shard string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.outcomes[rebuiltShard{index, shard}]
}

// forget makes dropped shards pending again, as a restore or a replica copy can bring
// them back with other data. No shards forgets the whole index.
func (r *startupRebuild) forget(index string, shards ...string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for key := range r.outcomes {
		if key.index == index && (len(shards) == 0 || slices.Contains(shards, key.shard)) {
			delete(r.outcomes, key)
		}
	}
}

func (r *startupRebuild) summary() (rebuilt, failed int, objects int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, ok := range r.outcomes {
		if ok {
			rebuilt++
		} else {
			failed++
		}
	}
	return rebuilt, failed, r.objects
}

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
	"fmt"
	"sync"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

// Async replication never loads a shard that holds data (#12195). A hosted shard
// that is unloaded because it has never held an object is the one exception a
// replica needs to be repairable at all: a node that was down for a tenant's
// first writes rejoins with an empty, unloaded shard and would otherwise answer
// not-ready forever (#12526). Such a shard is answered as an empty shard —
// empty hashtree, no digests — without loading it. Only a repair write loads it,
// the same way a user write to the tenant would.

// hostsUnloadedEmptyShard reports whether shardName is held here as an unloaded
// shard that has never held an object. Only HOT local shards are in the map, so
// COLD, offloaded, and dropped shards never qualify.
func (i *Index) hostsUnloadedEmptyShard(shardName string) bool {
	lazy, ok := i.shards.Load(shardName).(*LazyLoadShard)
	return ok && !lazy.isLoaded() && lazy.neverWritten()
}

// unloadedEmptyShardHashTree returns the hashtree an unloaded never-written shard
// would hold once loaded: the effective height, no leaves. Async replication off
// for the shard answers errAsyncReplicationNotActive, as a loaded shard without a
// hashtree does.
func (i *Index) unloadedEmptyShardHashTree(shardName string) (hashtree.AggregatedHashTree, error) {
	enabled, config := i.asyncReplicationStateForShard(shardName)
	if !enabled {
		return nil, fmt.Errorf("%w: async replication disabled for unloaded shard %q", errAsyncReplicationNotActive, shardName)
	}
	if i.globalreplicationConfig != nil {
		config = config.Effective(*i.globalreplicationConfig)
	}
	return emptyHashTree(config.hashtreeHeight)
}

// emptyHashTrees caches one tree per height that nothing aggregates into; Root and
// Level sync its inner nodes under the tree's own mutex, so sharing it across
// requests is safe.
var emptyHashTrees sync.Map // height (int) -> *hashtree.HashTree

func emptyHashTree(height int) (*hashtree.HashTree, error) {
	if ht, ok := emptyHashTrees.Load(height); ok {
		return ht.(*hashtree.HashTree), nil
	}
	ht, err := hashtree.NewHashTree(height)
	if err != nil {
		return nil, err
	}
	actual, _ := emptyHashTrees.LoadOrStore(height, ht)
	return actual.(*hashtree.HashTree), nil
}

// allMissingDigests is CompareDigests' answer for a shard with no objects: every
// source object is absent here.
func allMissingDigests(sourceDigests []types.RepairResponse) []types.RepairResponse {
	result := make([]types.RepairResponse, len(sourceDigests))
	for i, d := range sourceDigests {
		result[i] = types.RepairResponse{ID: d.ID}
	}
	return result
}

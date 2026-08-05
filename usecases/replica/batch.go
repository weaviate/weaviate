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

package replica

import (
	"sort"

	"github.com/go-openapi/strfmt"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/storobj"
)

// IndexedBatch holds an indexed list of objects
type IndexedBatch struct {
	Data []*storobj.Object
	// Index is z-index used to maintain object's order
	Index []int
}

// createBatch creates IndexedBatch from xs
func createBatch(xs []*storobj.Object) IndexedBatch {
	var bi IndexedBatch
	bi.Data = xs
	bi.Index = make([]int, len(xs))
	for i := 0; i < len(xs); i++ {
		bi.Index[i] = i
	}
	return bi
}

// cluster data object by shard
func clusterObjectByShard(bi IndexedBatch) []ShardPart {
	index := bi.Index
	data := bi.Data
	sort.Slice(index, func(i, j int) bool {
		return data[index[i]].BelongsToShard < data[index[j]].BelongsToShard
	})
	clusters := make([]ShardPart, 0, 16)
	// partition
	cur := data[index[0]]
	j := 0
	for i := 1; i < len(index); i++ {
		if data[index[i]].BelongsToShard == cur.BelongsToShard {
			continue
		}
		clusters = append(clusters, ShardPart{
			Shard: cur.BelongsToShard,
			Node:  cur.BelongsToNode, Data: data,
			Index: index[j:i],
		})
		j = i
		cur = data[index[j]]

	}
	clusters = append(clusters, ShardPart{
		Shard: cur.BelongsToShard,
		Node:  cur.BelongsToNode, Data: data,
		Index: index[j:],
	})
	return clusters
}

// ShardPart represents a data partition belonging to a physical shard
type ShardPart struct {
	Shard string // one-to-one mapping between Shard and Node
	Node  string

	Data  []*storobj.Object
	Index []int // index for data
}

func (b *ShardPart) ObjectIDs() []strfmt.UUID {
	xs := make([]strfmt.UUID, len(b.Index))
	for i, idx := range b.Index {
		xs[i] = b.Data[idx].ID()
	}
	return xs
}

// Digests returns update times and ids for the caller's own copies, in Index
// order. These are search results that may lack projected-away properties or
// a requested vector, so they can never serve as repair content.
func (b *ShardPart) Digests() ([]types.RepairResponse, []strfmt.UUID) {
	xs := make([]types.RepairResponse, len(b.Index))
	ys := make([]strfmt.UUID, len(b.Index))

	for i, idx := range b.Index {
		p := b.Data[idx]
		ys[i] = p.ID()
		xs[i] = types.RepairResponse{ID: ys[i].String(), UpdateTime: p.LastUpdateTimeUnix()}
	}
	return xs, ys
}

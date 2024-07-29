//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hnsw

import (
	"encoding/binary"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/visited"
	"github.com/weaviate/weaviate/entities/storobj"
)

func (h *hnsw) RepairTombstones() error {
	h.pools.visitedListsLock.RLock()
	visited := h.pools.visitedLists.Borrow()
	h.pools.visitedListsLock.RUnlock()
	cursor := h.store.Bucket(helpers.ObjectsBucketLSM).Cursor()
	i := 0
	for k, val := cursor.First(); k != nil; k, val = cursor.Next() {
		obj, err := storobj.FromBinary(val)
		if err != nil {
			return errors.Wrapf(err, "unmarhsal item %d", i)
		}

		if len(obj.Vector) == int(h.dims) {
			id := binary.LittleEndian.Uint64(k)
			visited.Visit(id)
		}
		i++

	}
	cursor.Close()

	h.RLock()
	lenOfNodes := uint64(len(h.nodes))
	h.RUnlock()
	var connections [][]uint64
	for id := uint64(0); id < lenOfNodes; id++ {
		h.shardedNodeLocks.Lock(id)
		node := h.nodes[id]
		if node != nil {
			if len(node.connections) > len(connections) {
				connections = make([][]uint64, len(node.connections))
			} else {
				connections = connections[:len(node.connections)]
			}
			for i, neighbourAtLevel := range node.connections {
				if len(neighbourAtLevel) > len(connections[i]) {
					connections[i] = make([]uint64, len(neighbourAtLevel))
				} else {
					connections[i] = connections[i][:len(neighbourAtLevel)]
				}
				copy(neighbourAtLevel, connections[i])
			}
			h.shardedNodeLocks.Unlock(id)
			h.checkTombstoneFor(uint64(id), visited)
			for _, neighbourAtLevel := range connections {
				for _, neighbour := range neighbourAtLevel {
					h.checkTombstoneFor(neighbour, visited)
				}
			}

		} else {
			h.shardedNodeLocks.Unlock(id)
		}
	}

	h.pools.visitedListsLock.RLock()
	h.pools.visitedLists.Return(visited)
	h.pools.visitedListsLock.RUnlock()
	return nil
}

func (h *hnsw) checkTombstoneFor(id uint64, visited visited.ListSet) {
	if h.hasTombstone(id) || visited.Visited(id) {
		// nothing to do, this node already has a tombstone, it will be cleaned up
		// in the next deletion cycle
		return
	}

	h.addTombstone(id)
	h.logger.WithField("action", "attach_tombstone_to_deleted_node").
		WithField("node_id", id).
		Infof("found a deleted node (%d) without a tombstone, "+
			"tombstone was added", id)

	visited.Visit(id)
}

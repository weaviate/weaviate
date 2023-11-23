//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hnsw

import (
	"context"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

func (h *hnsw) ValidateBeforeInsert(vector []float32) error {
	if h.isEmpty() {
		return nil
	}
	// check if vector length is the same as existing nodes
	existingNodeVector, err := h.cache.Get(context.Background(), h.entryPointID)
	if err != nil {
		return err
	}

	if len(existingNodeVector) != len(vector) {
		return fmt.Errorf("new node has a vector with length %v. "+
			"Existing nodes have vectors with length %v", len(vector), len(existingNodeVector))
	}

	return nil
}

func (h *hnsw) addBatch(ctx context.Context, ids []uint64, vectors [][]float32, compressedVectors [][]byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	compressed := len(compressedVectors) != 0
	if (!compressed && len(ids) != len(vectors)) || (compressed && len(ids) != len(compressedVectors)) {
		return errors.Errorf("ids and vectors sizes does not match")
	}
	if len(ids) == 0 {
		return errors.Errorf("insertBatch called with empty lists")
	}
	h.trackDimensionsOnce.Do(func() {
		if compressed {
			atomic.StoreInt32(&h.dims, int32(len(compressedVectors[0])))
		} else {
			atomic.StoreInt32(&h.dims, int32(len(vectors[0])))
		}
	})
	levels := make([]int, len(ids))
	maxId := uint64(0)
	for i, id := range ids {
		if maxId < id {
			maxId = id
		}
		levels[i] = int(math.Floor(-math.Log(h.randFunc()) * h.levelNormalizer))
	}
	h.RLock()
	previousSize := uint64(len(h.nodes))
	if maxId >= previousSize {
		h.RUnlock()
		h.Lock()
		if maxId >= previousSize {
			err := h.growIndexToAccomodateNode(maxId, h.logger)
			if err != nil {
				h.Unlock()
				return errors.Wrapf(err, "grow HNSW index to accommodate node %d", maxId)
			}
		}
		h.Unlock()
	} else {
		h.RUnlock()
	}

	for i := range ids {
		if err := ctx.Err(); err != nil {
			return err
		}

		node := &vertex{
			id:    ids[i],
			level: levels[i],
		}
		globalBefore := time.Now()
		var vector []float32
		var compressedVector []byte
		if compressed {
			compressedVector = compressedVectors[i]
		} else {
			vector = vectors[i]
		}
		if (compressed && len(compressedVector) == 0) || (!compressed && len(vector) == 0) {
			return errors.Errorf("insert called with nil-vector")
		}

		h.metrics.InsertVector()

		if !compressed && h.distancerProvider.Type() == "cosine-dot" {
			// cosine-dot requires normalized vectors, as the dot product and cosine
			// similarity are only identical if the vector is normalized
			vector = distancer.Normalize(vector)
		}

		err := h.addOne(vector, compressedVector, node)
		if err != nil {
			return err
		}

		h.insertMetrics.total(globalBefore)
	}
	return nil
}

func (h *hnsw) AddBatch(ctx context.Context, ids []uint64, vectors [][]float32) error {
	return h.addBatch(ctx, ids, vectors, nil)
}

func (h *hnsw) addOne(vector []float32, compressedVector []byte, node *vertex) error {
	h.compressActionLock.RLock()
	h.deleteVsInsertLock.RLock()

	before := time.Now()

	defer func() {
		h.deleteVsInsertLock.RUnlock()
		h.compressActionLock.RUnlock()
		h.insertMetrics.updateGlobalEntrypoint(before)
	}()

	wasFirst := false
	var firstInsertError error
	h.initialInsertOnce.Do(func() {
		if h.isEmpty() {
			wasFirst = true
			firstInsertError = h.insertInitialElement(node, vector)
		}
	})
	if wasFirst {
		if firstInsertError != nil {
			return firstInsertError
		}
		return nil
	}

	node.markAsMaintenance()

	h.RLock()
	// initially use the "global" entrypoint which is guaranteed to be on the
	// currently highest layer
	entryPointID := h.entryPointID
	// initially use the level of the entrypoint which is the highest level of
	// the h-graph in the first iteration
	currentMaximumLayer := h.currentMaximumLayer
	h.RUnlock()

	targetLevel := node.level
	node.connections = make([][]uint64, targetLevel+1)

	for i := targetLevel; i >= 0; i-- {
		capacity := h.maximumConnections
		if i == 0 {
			capacity = h.maximumConnectionsLayerZero
		}

		node.connections[i] = make([]uint64, 0, capacity)
	}

	if err := h.commitLog.AddNode(node); err != nil {
		return err
	}

	nodeId := node.id

	h.shardedNodeLocks.Lock(nodeId)
	h.nodes[nodeId] = node
	h.shardedNodeLocks.Unlock(nodeId)

	if h.compressed.Load() {
		if compressedVector == nil {
			compressedVector = h.pq.Encode(vector)
		}
		h.storeCompressedVector(node.id, compressedVector)
		h.compressedVectorsCache.Preload(node.id, compressedVector)
	} else {
		h.cache.Preload(node.id, vector)
	}

	h.insertMetrics.prepareAndInsertNode(before)
	before = time.Now()

	var err error
	entryPointID, err = h.findBestEntrypointForNode(currentMaximumLayer, targetLevel,
		entryPointID, vector, compressedVector)
	if err != nil {
		return errors.Wrap(err, "find best entrypoint")
	}

	h.insertMetrics.findEntrypoint(before)
	before = time.Now()

	// TODO: check findAndConnectNeighbors...
	if err := h.findAndConnectNeighbors(node, entryPointID, vector, compressedVector,
		targetLevel, currentMaximumLayer, helpers.NewAllowList()); err != nil {
		return errors.Wrap(err, "find and connect neighbors")
	}

	h.insertMetrics.findAndConnectTotal(before)
	before = time.Now()

	node.unmarkAsMaintenance()

	h.RLock()
	if targetLevel > h.currentMaximumLayer {
		h.RUnlock()
		h.Lock()
		// check again to avoid changes from RUnlock to Lock again
		if targetLevel > h.currentMaximumLayer {
			if err := h.commitLog.SetEntryPointWithMaxLayer(nodeId, targetLevel); err != nil {
				h.Unlock()
				return err
			}

			h.entryPointID = nodeId
			h.currentMaximumLayer = targetLevel
		}
		h.Unlock()
	} else {
		h.RUnlock()
	}

	return nil
}

func (h *hnsw) Add(id uint64, vector []float32) error {
	return h.AddBatch(context.TODO(), []uint64{id}, [][]float32{vector})
}

func (h *hnsw) insertInitialElement(node *vertex, nodeVec []float32) error {
	h.Lock()
	defer h.Unlock()

	if err := h.commitLog.SetEntryPointWithMaxLayer(node.id, 0); err != nil {
		return err
	}

	h.entryPointID = node.id
	h.currentMaximumLayer = 0
	node.connections = [][]uint64{
		make([]uint64, 0, h.maximumConnectionsLayerZero),
	}
	node.level = 0
	if err := h.commitLog.AddNode(node); err != nil {
		return err
	}

	err := h.growIndexToAccomodateNode(node.id, h.logger)
	if err != nil {
		return errors.Wrapf(err, "grow HNSW index to accommodate node %d", node.id)
	}

	h.shardedNodeLocks.Lock(node.id)
	h.nodes[node.id] = node
	h.shardedNodeLocks.Unlock(node.id)

	if h.compressed.Load() {
		compressed := h.pq.Encode(nodeVec)
		h.storeCompressedVector(node.id, compressed)
		h.compressedVectorsCache.Preload(node.id, compressed)
	} else {
		h.cache.Preload(node.id, nodeVec)
	}

	// go h.insertHook(node.id, 0, node.connections)
	return nil
}

func (h *hnsw) CompressVector(vector []float32) []byte {
	if h.compressed.Load() {
		return h.pq.Encode(vector)
	}
	return nil
}

func (h *hnsw) AddCompressed(id uint64, vector []byte) error {
	return h.AddCompressedBatch(context.TODO(), []uint64{id}, [][]byte{vector})
}

func (h *hnsw) AddCompressedBatch(ctx context.Context, ids []uint64, vectors [][]byte) error {
	return h.addBatch(ctx, ids, nil, vectors)
}

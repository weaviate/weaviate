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
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/multivector"
)

func (h *hnsw) ValidateBeforeInsert(vector []float32) error {
	dims := int(atomic.LoadInt32(&h.dims))

	// no vectors exist
	if dims == 0 {
		return nil
	}

	// check if vector length is the same as existing nodes
	if dims != len(vector) {
		return fmt.Errorf("new node has a vector with length %v. "+
			"Existing nodes have vectors with length %v", len(vector), dims)
	}

	return nil
}

func (h *hnsw) ValidateMultiBeforeInsert(vector [][]float32) error {
	dims := int(atomic.LoadInt32(&h.dims))

	if h.muvera.Load() {
		return nil
	}

	// no vectors exist
	if dims == 0 {
		vecDimensions := make(map[int]struct{})
		for i := range vector {
			vecDimensions[len(vector[i])] = struct{}{}
		}
		if len(vecDimensions) > 1 {
			return fmt.Errorf("multi vector array consists of vectors with varying dimensions")
		}
		return nil
	}

	// check if vector length is the same as existing nodes
	for i := range vector {
		if dims != len(vector[i]) {
			return fmt.Errorf("new node has a multi vector with length %v at position %v. "+
				"Existing nodes have vectors with length %v", len(vector[i]), i, dims)
		}
	}

	return nil
}

func (h *hnsw) AddBatch(ctx context.Context, ids []uint64, vectors [][]float32) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if h.multivector.Load() && !h.muvera.Load() {
		return errors.Errorf("AddBatch called on multivector index")
	}
	if len(ids) != len(vectors) {
		return errors.Errorf("ids and vectors sizes does not match")
	}
	if len(ids) == 0 {
		return errors.Errorf("insertBatch called with empty lists")
	}

	var err error
	h.trackDimensionsOnce.Do(func() {
		dims := len(vectors[0])
		for _, vec := range vectors {
			if len(vec) != dims {
				err = errors.Errorf("addBatch called with vectors of different lengths")
				return
			}
		}
		if err == nil {
			atomic.StoreInt32(&h.dims, int32(len(vectors[0])))
		}
	})

	if err != nil {
		return err
	}

	levels := make([]int, len(ids))
	maxId := uint64(0)
	for i, id := range ids {
		if maxId < id {
			maxId = id
		}
		levels[i] = int(math.Floor(-math.Log(h.randFunc()) * h.levelNormalizer))
	}
	h.RLock()
	if maxId >= uint64(len(h.nodes)) {
		h.RUnlock()
		h.Lock()
		if maxId >= uint64(len(h.nodes)) {
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

		vector := vectors[i]
		node := &vertex{
			id:    ids[i],
			level: levels[i],
		}
		globalBefore := time.Now()
		if len(vector) == 0 {
			return errors.Errorf("insert called with nil-vector")
		}

		h.metrics.InsertVector()

		vector = h.normalizeVec(vector)
		err := h.addOne(ctx, vector, node)
		if err != nil {
			return err
		}

		h.insertMetrics.total(globalBefore)
	}
	return nil
}

func (h *hnsw) AddMultiBatch(ctx context.Context, docIDs []uint64, vectors [][][]float32) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !h.multivector.Load() {
		return errors.Errorf("addMultiBatch called on non-multivector index")
	}
	if len(docIDs) != len(vectors) {
		return errors.Errorf("ids and vectors sizes does not match")
	}
	if len(docIDs) == 0 {
		return errors.Errorf("addMultiBatch called with empty lists")
	}

	if h.muvera.Load() {
		h.trackMuveraOnce.Do(func() {
			h.muveraEncoder.InitEncoder(len(vectors[0][0]))
			h.Lock()
			if err := h.muveraEncoder.PersistMuvera(h.commitLog); err != nil {
				h.Unlock()
				h.logger.WithField("action", "persist muvera").Error(err)
				return
			}
			h.Unlock()
		})
		// Process all vectors
		processedVectors := make([][]float32, len(vectors))
		for i, v := range vectors {
			processedVectors[i] = h.muveraEncoder.EncodeDoc(v)
			docIDBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(docIDBytes, docIDs[i])
			muveraBytes := multivector.MuveraBytesFromFloat32(processedVectors[i])
			if err := h.store.Bucket(h.id+"_muvera_vectors").Put(docIDBytes, muveraBytes); err != nil {
				return errors.Wrap(err, fmt.Sprintf("failed to put %s_muvera_vectors into the bucket", h.id))
			}
		}
		// Replace original vectors with processed ones
		return h.AddBatch(ctx, docIDs, processedVectors)
	}

	var err error
	h.trackDimensionsOnce.Do(func() {
		dim := len(vectors[0][0])
		for _, doc := range vectors {
			for _, vec := range doc {
				if len(vec) != dim {
					err = errors.Errorf("addMultiBatch called with vectors of different lengths")
					return
				}
			}
		}
		if err == nil {
			atomic.StoreInt32(&h.dims, int32(len(vectors[0][0])))
		}
	})

	if err != nil {
		return err
	}

	for i, docID := range docIDs {
		numVectors := len(vectors[i])
		levels := make([]int, numVectors)
		for j := range numVectors {
			levels[j] = int(math.Floor(-math.Log(h.randFunc()) * h.levelNormalizer))
		}

		h.Lock()
		counter := h.vecIDcounter
		h.vecIDcounter += uint64(numVectors)
		h.Unlock()

		maxId := counter + uint64(numVectors)

		h.RLock()
		if maxId >= uint64(len(h.nodes)) {
			h.RUnlock()
			h.Lock()
			if maxId >= uint64(len(h.nodes)) {
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

		ids := make([]uint64, numVectors)
		for id := range ids {
			ids[id] = counter + uint64(id)
		}
		if h.compressed.Load() {
			h.compressor.PreloadMulti(docID, ids, vectors[i])
		} else {
			h.cache.PreloadMulti(docID, ids, vectors[i])
		}
		for j := range numVectors {
			if err := ctx.Err(); err != nil {
				return err
			}

			vector := vectors[i][j]

			globalBefore := time.Now()
			if len(vector) == 0 {
				return errors.Errorf("insert called with nil-vector")
			}

			h.metrics.InsertVector()

			vector = h.normalizeVec(vector)

			nodeId := counter
			counter++

			node := &vertex{
				id:    uint64(nodeId),
				level: levels[j],
			}

			h.Lock()
			h.docIDVectors[docID] = append(h.docIDVectors[docIDs[i]], nodeId)
			h.Unlock()

			nodeIDBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(nodeIDBytes, nodeId)
			docIDBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(docIDBytes, docID)
			err := h.store.Bucket(h.id+"_mv_mappings").Put(nodeIDBytes, docIDBytes)
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("failed to put %s_mv_mappings into the bucket", h.id))
			}

			err = h.addOne(ctx, vector, node)
			if err != nil {
				return err
			}

			h.insertMetrics.total(globalBefore)
		}

	}

	return nil
}

func (h *hnsw) addOne(ctx context.Context, vector []float32, node *vertex) error {
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

	singleVector := !h.multivector.Load() || h.muvera.Load()
	if singleVector {
		if h.compressed.Load() {
			h.compressor.Preload(nodeId, vector)
		} else {
			h.cache.Preload(nodeId, vector)
		}
	}

	h.insertMetrics.prepareAndInsertNode(before)
	before = time.Now()

	var err error
	var distancer compressionhelpers.CompressorDistancer
	var returnFn compressionhelpers.ReturnDistancerFn
	if h.compressed.Load() {
		distancer, returnFn = h.compressor.NewDistancer(vector)
		defer returnFn()
	}
	entryPointID, err = h.findBestEntrypointForNode(ctx, currentMaximumLayer, targetLevel,
		entryPointID, vector, distancer)
	if err != nil {
		return errors.Wrap(err, "find best entrypoint")
	}

	h.insertMetrics.findEntrypoint(before)
	before = time.Now()

	// TODO: check findAndConnectNeighbors...
	if err := h.findAndConnectNeighbors(ctx, node, entryPointID, vector, distancer,
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

func (h *hnsw) Add(ctx context.Context, id uint64, vector []float32) error {
	return h.AddBatch(ctx, []uint64{id}, [][]float32{vector})
}

func (h *hnsw) AddMulti(ctx context.Context, id uint64, vector [][]float32) error {
	return h.AddMultiBatch(ctx, []uint64{id}, [][][]float32{vector})
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

	singleVector := !h.multivector.Load() || h.muvera.Load()
	if singleVector {
		if h.compressed.Load() {
			h.compressor.Preload(node.id, nodeVec)
		} else {
			h.cache.Preload(node.id, nodeVec)
		}
	}

	// go h.insertHook(node.id, 0, node.connections)
	return nil
}

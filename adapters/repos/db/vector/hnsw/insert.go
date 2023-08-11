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
	"math/rand"
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
	existingNodeVector, err := h.cache.get(context.Background(), h.entryPointID)
	if err != nil {
		return err
	}

	if len(existingNodeVector) != len(vector) {
		return fmt.Errorf("new node has a vector with length %v. "+
			"Existing nodes have vectors with length %v", len(vector), len(existingNodeVector))
	}

	return nil
}

func (h *hnsw) Add(id uint64, vector []float32) error {
	before := time.Now()
	if len(vector) == 0 {
		return errors.Errorf("insert called with nil-vector")
	}

	h.metrics.InsertVector()
	defer h.insertMetrics.total(before)

	node := &vertex{
		id: id,
	}

	if h.distancerProvider.Type() == "cosine-dot" {
		// cosine-dot requires normalized vectors, as the dot product and cosine
		// similarity are only identical if the vector is normalized
		vector = distancer.Normalize(vector)
	}

	h.compressActionLock.RLock()
	defer h.compressActionLock.RUnlock()
	return h.insert(node, vector)
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

	h.nodes[node.id] = node
	if h.compressed.Load() {
		compressed := h.pq.Encode(nodeVec)
		h.storeCompressedVector(node.id, compressed)
		h.compressedVectorsCache.preload(node.id, compressed)
	} else {
		h.cache.preload(node.id, nodeVec)
	}

	// go h.insertHook(node.id, 0, node.connections)
	return nil
}

func (h *hnsw) insert(node *vertex, nodeVec []float32) error {
	h.trackDimensionsOnce.Do(func() {
		atomic.StoreInt32(&h.dims, int32(len(nodeVec)))
	})
	h.deleteVsInsertLock.RLock()
	defer h.deleteVsInsertLock.RUnlock()

	before := time.Now()

	wasFirst := false
	var firstInsertError error
	h.initialInsertOnce.Do(func() {
		if h.isEmpty() {
			wasFirst = true
			firstInsertError = h.insertInitialElement(node, nodeVec)
		}
	})
	if wasFirst {
		return firstInsertError
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

	targetLevel := int(math.Floor(-math.Log(h.randFunc()) * h.levelNormalizer))

	// before = time.Now()
	// m.addBuildingItemLocking(before)
	node.level = targetLevel
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

	// before = time.Now()
	h.Lock()
	// m.addBuildingLocking(before)
	err := h.growIndexToAccomodateNode(node.id, h.logger)
	if err != nil {
		h.Unlock()
		return errors.Wrapf(err, "grow HNSW index to accommodate node %d", node.id)
	}
	h.Unlock()

	// // make sure this new vec is immediately present in the cache, so we don't
	// // have to read it from disk again
	if h.compressed.Load() {
		compressed := h.pq.Encode(nodeVec)
		h.storeCompressedVector(node.id, compressed)
		h.compressedVectorsCache.preload(node.id, compressed)
	} else {
		h.cache.preload(node.id, nodeVec)
	}

	h.Lock()
	h.nodes[nodeId] = node
	h.Unlock()

	h.insertMetrics.prepareAndInsertNode(before)
	before = time.Now()

	entryPointID, err = h.findBestEntrypointForNode(currentMaximumLayer, targetLevel,
		entryPointID, nodeVec)
	if err != nil {
		return errors.Wrap(err, "find best entrypoint")
	}

	h.insertMetrics.findEntrypoint(before)
	before = time.Now()

	if err := h.findAndConnectNeighbors(node, entryPointID, nodeVec,
		targetLevel, currentMaximumLayer, helpers.NewAllowList()); err != nil {
		return errors.Wrap(err, "find and connect neighbors")
	}

	h.insertMetrics.findAndConnectTotal(before)
	before = time.Now()
	defer h.insertMetrics.updateGlobalEntrypoint(before)

	// go h.insertHook(nodeId, targetLevel, neighborsAtLevel)
	node.unmarkAsMaintenance()

	h.Lock()
	if targetLevel > h.currentMaximumLayer {
		// before = time.Now()
		// m.addBuildingLocking(before)
		if err := h.commitLog.SetEntryPointWithMaxLayer(nodeId, targetLevel); err != nil {
			h.Unlock()
			return err
		}

		h.entryPointID = nodeId
		h.currentMaximumLayer = targetLevel
	}
	h.Unlock()

	return nil
}

func (h *hnsw) HybridAdd(id uint64, vector []float32, filters map[int]int, lambda float32) error {
	before := time.Now()
	if len(vector) == 0 {
		return errors.Errorf("insert called with nil-vector")
	}

	h.metrics.InsertVector()
	defer h.insertMetrics.total(before)

	node := &vertex{
		id:      id,
		filters: filters,
	}

	if h.distancerProvider.Type() == "cosine-dot" {
		// cosine-dot requires normalized vectors, as the dot product and cosine
		// similarity are only identical if the vector is normalized
		vector = distancer.Normalize(vector)
	}

	h.compressActionLock.RLock()
	defer h.compressActionLock.RUnlock()
	return h.hybridInsert(node, vector, lambda)
}

func (h *hnsw) insertInitialElementPerFilterPerValue(node *vertex, nodeVec []float32) error {
	h.Lock()
	defer h.Unlock()

	if err := h.commitLog.SetEntryPointWithMaxLayer(node.id, 0); err != nil {
		return err
	}

	// loop through filters
	for filter, filterValue := range node.filters {
		if _, ok := h.entryPointIDperFilterPerValue[filter]; !ok {
			// then the filter map hasn't even been initialized, let alone the value
			h.entryPointIDperFilterPerValue[filter] = make(map[int]uint64)
			h.currentMaximumLayerPerFilterPerValue[filter] = make(map[int]int)
		}
		if _, ok := h.entryPointIDperFilterPerValue[filter][filterValue]; !ok {
			h.entryPointIDperFilterPerValue[filter][filterValue] = node.id
			h.currentMaximumLayerPerFilterPerValue[filter][filterValue] = 0
		}
	}

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

	h.nodes[node.id] = node
	if h.compressed.Load() {
		compressed := h.pq.Encode(nodeVec)
		h.storeCompressedVector(node.id, compressed)
		h.compressedVectorsCache.preload(node.id, compressed)
	} else {
		h.cache.preload(node.id, nodeVec)
	}

	// go h.insertHook(node.id, 0, node.connections)
	return nil
}

func (h *hnsw) hybridInsert(node *vertex, nodeVec []float32, lambda float32) error {
	h.trackDimensionsOnce.Do(func() {
		atomic.StoreInt32(&h.dims, int32(len(nodeVec)))
	})
	h.deleteVsInsertLock.RLock()
	defer h.deleteVsInsertLock.RUnlock()

	before := time.Now()

	var firstInsertError error
	emptyEPfound := false
	h.Lock()
	for filter := range node.filters {
		if _, ok := h.entryPointIDperFilterPerValue[filter]; !ok {
			emptyEPfound = true
			break
		} else {
			if _, ok := h.entryPointIDperFilterPerValue[filter][node.filters[filter]]; !ok {
				emptyEPfound = true
				break
			}
		}
	}
	h.Unlock()
	//h.RUnlock()
	/*
		For example, if we have 3 filters, one of them may not have an entrypoint in the graph yet.
	*/
	if emptyEPfound {
		//h.Lock()
		firstInsertError = h.insertInitialElementPerFilterPerValue(node, nodeVec)
		// Locking is already nested within `insertInitialElementPerFilter`
		//h.Unlock()
		return firstInsertError
	}

	node.markAsMaintenance()

	h.RLock()
	// select filter for insert entrypoint
	// loop through each filter and set the maxLayerPerFilterValue to
	randomIndex := rand.Intn(len(node.filters))
	epFilter := node.filters[randomIndex]
	entryPointID := h.entryPointIDperFilterPerValue[epFilter][node.filters[epFilter]]
	currentMaximumLayer := h.currentMaximumLayerPerFilterPerValue[epFilter][node.filters[epFilter]]
	h.RUnlock()

	targetLevel := int(math.Floor(-math.Log(h.randFunc()) * h.levelNormalizer))
	// If this targetLevel is higher than

	// before = time.Now()
	// m.addBuildingItemLocking(before)
	node.level = targetLevel
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

	// before = time.Now()
	h.Lock()
	// m.addBuildingLocking(before)
	err := h.growIndexToAccomodateNode(node.id, h.logger)
	if err != nil {
		h.Unlock()
		return errors.Wrapf(err, "grow HNSW index to accommodate node %d", node.id)
	}
	h.Unlock()

	// // make sure this new vec is immediately present in the cache, so we don't
	// // have to read it from disk again
	if h.compressed.Load() {
		compressed := h.pq.Encode(nodeVec)
		h.storeCompressedVector(node.id, compressed)
		h.compressedVectorsCache.preload(node.id, compressed)
	} else {
		h.cache.preload(node.id, nodeVec)
	}

	h.Lock()
	h.nodes[nodeId] = node
	h.Unlock()

	h.insertMetrics.prepareAndInsertNode(before)
	before = time.Now()

	epFilterMap := make(map[int]int)
	epFilterMap[epFilter] = node.filters[epFilter]
	entryPointID, err = h.findBestEntrypointForNodeWithFilter(currentMaximumLayer, targetLevel,
		entryPointID, nodeVec, epFilterMap)
	if err != nil {
		return errors.Wrap(err, "find best entrypoint")
	}

	h.insertMetrics.findEntrypoint(before)
	before = time.Now()

	// Add Lambda Here
	if err := h.findAndConnectNeighborsHybrid(node, entryPointID, nodeVec,
		targetLevel, currentMaximumLayer, node.filters, lambda, helpers.NewAllowList()); err != nil {
		return errors.Wrap(err, "find and connect neighbors")
	}

	h.insertMetrics.findAndConnectTotal(before)
	before = time.Now()
	defer h.insertMetrics.updateGlobalEntrypoint(before)

	// go h.insertHook(nodeId, targetLevel, neighborsAtLevel)
	node.unmarkAsMaintenance()

	// This might be wonky
	h.Lock()
	for filter, filterValue := range node.filters {
		if targetLevel > h.currentMaximumLayerPerFilterPerValue[filter][filterValue] {
			/* h.commitLog.SetEntryPointWithMaxLayer(nodeId, targetLevel).. */
			h.entryPointIDperFilterPerValue[filter][filterValue] = nodeId
			h.currentMaximumLayerPerFilterPerValue[filter][filterValue] = targetLevel
		}
	}
	h.Unlock()

	return nil
}

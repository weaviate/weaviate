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
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storobj"
)

type breakCleanUpTombstonedNodesFunc func() bool

// Delete attaches a tombstone to an item so it can be periodically cleaned up
// later and the edges reassigned
func (h *hnsw) Delete(id uint64) error {
	h.deleteVsInsertLock.Lock()
	defer h.deleteVsInsertLock.Unlock()

	h.deleteLock.Lock()
	defer h.deleteLock.Unlock()

	before := time.Now()
	defer h.metrics.TrackDelete(before, "total")

	h.metrics.DeleteVector()
	if err := h.addTombstone(id); err != nil {
		return err
	}

	h.cache.delete(context.TODO(), id)

	// Adding a tombstone might not be enough in some cases, if the tombstoned
	// entry was the entrypoint this might lead to issues for following inserts:
	// On a nearly empty graph the entrypoint might be the only viable element to
	// connect to, however, because the entrypoint itself is tombstones
	// connections to it are impossible. So, unless we find a new entrypoint,
	// subsequent inserts might end up isolated (without edges) in the graph.
	// This is especially true if the tombstoned entrypoint is the only node in
	// the graph. In this case we must reset the graph, so it acts like an empty
	// one. Otherwise we'd insert the next id and have only one possible node to
	// connect it to (the entrypoint). With that one being tombstoned, the new
	// node would be guaranteed to have zero edges

	node := h.nodeByID(id)
	if node == nil {
		// node was already deleted/cleaned up
		return nil
	}

	if h.getEntrypoint() == id {
		beforeDeleteEP := time.Now()
		defer h.metrics.TrackDelete(beforeDeleteEP, "delete_entrypoint")

		denyList := h.tombstonesAsDenyList()
		if onlyNode, err := h.resetIfOnlyNode(node, denyList); err != nil {
			return errors.Wrap(err, "reset index")
		} else if !onlyNode {
			if err := h.deleteEntrypoint(node, denyList); err != nil {
				return errors.Wrap(err, "delete entrypoint")
			}
		}
	}
	return nil
}

func (h *hnsw) resetIfEmpty() (empty bool, err error) {
	h.resetLock.Lock()
	h.Lock()
	defer h.Unlock()
	defer h.resetLock.Unlock()

	if h.isEmptyUnsecured() {
		return true, h.resetUnsecured()
	}
	return false, nil
}

func (h *hnsw) resetIfOnlyNode(needle *vertex, denyList helpers.AllowList) (onlyNode bool, err error) {
	h.resetLock.Lock()
	h.Lock()
	defer h.Unlock()
	defer h.resetLock.Unlock()

	if h.isOnlyNodeUnsecured(needle, denyList) {
		return true, h.resetUnsecured()
	}
	return false, nil
}

func (h *hnsw) resetUnsecured() error {
	h.resetCtxCancel()
	resetCtx, resetCtxCancel := context.WithCancel(context.Background())
	h.resetCtx = resetCtx
	h.resetCtxCancel = resetCtxCancel

	h.entryPointID = 0
	h.currentMaximumLayer = 0
	h.initialInsertOnce = &sync.Once{}
	h.nodes = make([]*vertex, initialSize)

	return h.commitLog.Reset()
}

func (h *hnsw) tombstonesAsDenyList() helpers.AllowList {
	deleteList := helpers.NewAllowList()
	h.tombstoneLock.Lock()
	defer h.tombstoneLock.Unlock()

	tombstones := h.tombstones
	for id := range tombstones {
		deleteList.Insert(id)
	}

	return deleteList
}

func (h *hnsw) getEntrypoint() uint64 {
	h.RLock()
	defer h.RUnlock()

	return h.entryPointID
}

func (h *hnsw) copyTombstonesToAllowList(breakCleanUpTombstonedNodes breakCleanUpTombstonedNodesFunc) (ok bool, deleteList helpers.AllowList) {
	h.resetLock.Lock()
	defer h.resetLock.Unlock()

	if breakCleanUpTombstonedNodes() {
		return false, nil
	}

	h.RLock()
	lenOfNodes := uint64(len(h.nodes))
	h.RUnlock()

	h.tombstoneLock.Lock()
	defer h.tombstoneLock.Unlock()

	deleteList = helpers.NewAllowList()
	for id := range h.tombstones {
		if lenOfNodes <= id {
			// we're trying to delete an id outside the possible range, nothing to do
			continue
		}

		deleteList.Insert(id)
	}

	if deleteList.IsEmpty() {
		return false, nil
	}

	return true, deleteList
}

// CleanUpTombstonedNodes removes nodes with a tombstone and reassigns
// edges that were previously pointing to the tombstoned nodes
func (h *hnsw) CleanUpTombstonedNodes(stopFunc cyclemanager.StopFunc) error {
	h.metrics.StartCleanup(1)
	defer h.metrics.EndCleanup(1)

	h.resetLock.Lock()
	resetCtx := h.resetCtx
	h.resetLock.Unlock()

	breakCleanUpTombstonedNodes := func() bool {
		return resetCtx.Err() != nil || stopFunc()
	}

	ok, deleteList := h.copyTombstonesToAllowList(breakCleanUpTombstonedNodes)
	if !ok {
		return nil
	}

	if ok, err := h.reassignNeighborsOf(deleteList, breakCleanUpTombstonedNodes); err != nil {
		return err
	} else if !ok {
		return nil
	}

	if ok, err := h.replaceDeletedEntrypoint(deleteList, breakCleanUpTombstonedNodes); err != nil {
		return err
	} else if !ok {
		return nil
	}

	if ok, err := h.removeTombstonesAndNodes(deleteList, breakCleanUpTombstonedNodes); err != nil {
		return err
	} else if !ok {
		return nil
	}

	if _, err := h.resetIfEmpty(); err != nil {
		return err
	}

	return nil
}

func (h *hnsw) replaceDeletedEntrypoint(deleteList helpers.AllowList, breakCleanUpTombstonedNodes breakCleanUpTombstonedNodesFunc) (ok bool, err error) {
	h.resetLock.Lock()
	defer h.resetLock.Unlock()

	if breakCleanUpTombstonedNodes() {
		return false, nil
	}

	it := deleteList.Iterator()
	for id, ok := it.Next(); ok; id, ok = it.Next() {
		if h.getEntrypoint() == id {
			// this a special case because:
			//
			// 1. we need to find a new entrypoint, if this is the last point on this
			// level, we need to find an entrypoint on a lower level
			// 2. there is a risk that this is the only node in the entire graph. In
			// this case we must reset the graph
			h.RLock()
			node := h.nodes[id]
			h.RUnlock()
			if err := h.deleteEntrypoint(node, deleteList); err != nil {
				return false, errors.Wrap(err, "delete entrypoint")
			}
		}
	}

	return true, nil
}

func (h *hnsw) reassignNeighborsOf(deleteList helpers.AllowList, breakCleanUpTombstonedNodes breakCleanUpTombstonedNodesFunc) (ok bool, err error) {
	h.RLock()
	size := len(h.nodes)
	h.RUnlock()

	for n := 0; n < size; n++ {
		if ok, err := h.reassignNeighbor(uint64(n), deleteList, breakCleanUpTombstonedNodes); err != nil {
			return false, errors.Wrap(err, "reassign neighbor edges")
		} else if !ok {
			return false, nil
		}
	}

	return true, nil
}

func (h *hnsw) reassignNeighbor(neighbor uint64, deleteList helpers.AllowList, breakCleanUpTombstonedNodes breakCleanUpTombstonedNodesFunc) (ok bool, err error) {
	h.resetLock.Lock()
	defer h.resetLock.Unlock()

	if breakCleanUpTombstonedNodes() {
		return false, nil
	}

	h.RLock()
	neighborNode := h.nodes[neighbor]
	currentEntrypoint := h.entryPointID
	currentMaximumLayer := h.currentMaximumLayer
	h.RUnlock()

	if neighborNode == nil || deleteList.Contains(neighborNode.id) {
		return true, nil
	}

	neighborVec, err := h.vectorForID(context.Background(), neighbor)
	if err != nil {
		var e storobj.ErrNotFound
		if errors.As(err, &e) {
			h.handleDeletedNode(e.DocID)
			return true, nil
		} else {
			// not a typed error, we can recover from, return with err
			return false, errors.Wrap(err, "get neighbor vec")
		}
	}
	neighborNode.Lock()
	neighborLevel := neighborNode.level
	if !connectionsPointTo(neighborNode.connections, deleteList) {
		// nothing needs to be changed, skip
		neighborNode.Unlock()
		return true, nil
	}
	neighborNode.Unlock()

	entryPointID, err := h.findBestEntrypointForNode(currentMaximumLayer,
		neighborLevel, currentEntrypoint, neighborVec)
	if err != nil {
		return false, errors.Wrap(err, "find best entrypoint")
	}

	if entryPointID == neighbor {
		// if we use ourselves as entrypoint and delete all connections in the
		// next step, we won't find any neighbors, so we need to use an
		// alternative entryPoint in this round

		if h.isOnlyNode(&vertex{id: neighbor}, deleteList) {
			neighborNode.Lock()
			// delete all existing connections before re-assigning
			neighborLevel = neighborNode.level
			neighborNode.connections = make([][]uint64, neighborLevel+1)
			neighborNode.Unlock()

			if err := h.commitLog.ClearLinks(neighbor); err != nil {
				return false, err
			}
			return true, nil
		}

		tmpDenyList := deleteList.DeepCopy()
		tmpDenyList.Insert(entryPointID)

		alternative, level := h.findNewLocalEntrypoint(tmpDenyList, currentMaximumLayer,
			entryPointID)
		if level > neighborLevel {
			neighborNode.Lock()
			// reset connections according to level
			neighborNode.connections = make([][]uint64, level+1)
			neighborNode.Unlock()
		}
		neighborLevel = level
		entryPointID = alternative
	}

	neighborNode.markAsMaintenance()
	neighborNode.Lock()
	// delete all existing connections before re-assigning
	for level := range neighborNode.connections {
		neighborNode.connections[level] = neighborNode.connections[level][:0]
	}
	neighborNode.Unlock()
	if err := h.commitLog.ClearLinks(neighbor); err != nil {
		return false, err
	}

	if err := h.findAndConnectNeighbors(neighborNode, entryPointID, neighborVec,
		neighborLevel, currentMaximumLayer, deleteList); err != nil {
		return false, errors.Wrap(err, "find and connect neighbors")
	}
	neighborNode.unmarkAsMaintenance()

	h.metrics.CleanedUp()
	return true, nil
}

func connectionsPointTo(connections [][]uint64, needles helpers.AllowList) bool {
	for _, atLevel := range connections {
		for _, pointer := range atLevel {
			if needles.Contains(pointer) {
				return true
			}
		}
	}

	return false
}

// deleteEntrypoint deletes the current entrypoint and replaces it with a new
// one. It respects the attached denyList, so that it doesn't assign another
// node which also has a tombstone and is also in the process of being cleaned
// up
func (h *hnsw) deleteEntrypoint(node *vertex, denyList helpers.AllowList) error {
	if h.isOnlyNode(node, denyList) {
		// no point in finding another entrypoint if this is the only node
		return nil
	}

	node.Lock()
	level := node.level
	id := node.id
	node.Unlock()

	newEntrypoint, level, ok := h.findNewGlobalEntrypoint(denyList, level, id)
	if !ok {
		return nil
	}

	h.Lock()
	h.entryPointID = newEntrypoint
	h.currentMaximumLayer = level
	h.Unlock()
	if err := h.commitLog.SetEntryPointWithMaxLayer(newEntrypoint, level); err != nil {
		return err
	}

	return nil
}

// returns entryPointID, level and whether a change occurred
func (h *hnsw) findNewGlobalEntrypoint(denyList helpers.AllowList, targetLevel int,
	oldEntrypoint uint64,
) (uint64, int, bool) {
	if h.getEntrypoint() != oldEntrypoint {
		// entrypoint has already been changed (this could be due to a new import
		// for example, nothing to do for us
		return 0, 0, false
	}

	for l := targetLevel; l >= 0; l-- {
		// ideally we can find a new entrypoint at the same level of the
		// to-be-deleted node. However, there is a chance it was the only node on
		// that level, in that case we need to look at the next lower level for a
		// better candidate

		h.RLock()
		maxNodes := len(h.nodes)
		h.RUnlock()

		for i := 0; i < maxNodes; i++ {
			if h.getEntrypoint() != oldEntrypoint {
				// entrypoint has already been changed (this could be due to a new import
				// for example, nothing to do for us
				return 0, 0, false
			}

			if denyList.Contains(uint64(i)) {
				continue
			}
			h.RLock()
			candidate := h.nodes[i]
			h.RUnlock()

			if candidate == nil {
				continue
			}

			candidate.Lock()
			candidateLevel := candidate.level
			candidate.Unlock()

			if candidateLevel != l {
				// not reaching up to the current level, skip in hope of finding another candidate
				continue
			}

			// we have a node that matches
			return uint64(i), l, true
		}
	}

	// we made it thorugh the entire graph and didn't find a new entrypoint all
	// the way down to level 0. This can only mean the graph is empty, which is
	// unexpected. This situation should have been prevented by the deleteLock.
	panic("findNewEntrypoint called on an empty hnsw graph")
}

// returns entryPointID, level and whether a change occurred
func (h *hnsw) findNewLocalEntrypoint(denyList helpers.AllowList, targetLevel int,
	oldEntrypoint uint64,
) (uint64, int) {
	if h.getEntrypoint() != oldEntrypoint {
		// the current global entrypoint is different from our local entrypoint, so
		// we can just use the global one, as the global one is guaranteed to be
		// present on every level, i.e. it is always chosen from the highest
		// currently available level
		return h.getEntrypoint(), h.currentMaximumLayer
	}

	h.RLock()
	maxNodes := len(h.nodes)
	h.RUnlock()

	for l := targetLevel; l >= 0; l-- {
		// ideally we can find a new entrypoint at the same level of the
		// to-be-deleted node. However, there is a chance it was the only node on
		// that level, in that case we need to look at the next lower level for a
		// better candidate
		for i := 0; i < maxNodes; i++ {
			if denyList.Contains(uint64(i)) {
				continue
			}
			h.RLock()
			candidate := h.nodes[i]
			h.RUnlock()

			if candidate == nil {
				continue
			}

			candidate.Lock()
			candidateLevel := candidate.level
			candidate.Unlock()

			if candidateLevel != l {
				// not reaching up to the current level, skip in hope of finding another candidate
				continue
			}

			// we have a node that matches
			return uint64(i), l
		}
	}

	panic("findNewLocalEntrypoint called on an empty hnsw graph")
}

func (h *hnsw) isOnlyNode(needle *vertex, denyList helpers.AllowList) bool {
	h.RLock()
	defer h.RUnlock()

	return h.isOnlyNodeUnsecured(needle, denyList)
}

func (h *hnsw) isOnlyNodeUnsecured(needle *vertex, denyList helpers.AllowList) bool {
	for _, node := range h.nodes {
		if node == nil || node.id == needle.id || denyList.Contains(node.id) {
			continue
		}
		return false
	}
	return true
}

func (h *hnsw) hasTombstone(id uint64) bool {
	h.tombstoneLock.RLock()
	defer h.tombstoneLock.RUnlock()
	_, ok := h.tombstones[id]
	return ok
}

func (h *hnsw) addTombstone(id uint64) error {
	h.metrics.AddTombstone()
	h.tombstoneLock.Lock()
	h.tombstones[id] = struct{}{}
	h.tombstoneLock.Unlock()
	return h.commitLog.AddTombstone(id)
}

func (h *hnsw) removeTombstonesAndNodes(deleteList helpers.AllowList, breakCleanUpTombstonedNodes breakCleanUpTombstonedNodesFunc) (ok bool, err error) {
	it := deleteList.Iterator()
	for id, ok := it.Next(); ok; id, ok = it.Next() {
		h.metrics.RemoveTombstone()
		h.tombstoneLock.Lock()
		delete(h.tombstones, id)
		h.tombstoneLock.Unlock()

		h.resetLock.Lock()
		if !breakCleanUpTombstonedNodes() {
			h.nodes[id] = nil
			if err := h.commitLog.DeleteNode(id); err != nil {
				h.resetLock.Unlock()
				return false, err
			}
		}
		h.resetLock.Unlock()

		if err := h.commitLog.RemoveTombstone(id); err != nil {
			return false, err
		}
	}

	return true, nil
}

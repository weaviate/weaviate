//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package hnsw

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/entities/storobj"
)

// Delete attaches a tombstone to an item so it can be periodically cleaned up
// later and the edges reassigned
func (h *hnsw) Delete(id uint64) error {
	h.deleteLock.Lock()
	defer h.deleteLock.Unlock()

	h.metrics.DeleteVector()
	if err := h.addTombstone(id); err != nil {
		return err
	}

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
		denyList := h.tombstonesAsDenyList()
		if h.isOnlyNode(node, denyList) {
			if err := h.reset(); err != nil {
				return errors.Wrap(err, "reset index")
			}
		} else {
			if err := h.deleteEntrypoint(node, denyList); err != nil {
				return errors.Wrap(err, "delete entrypoint")
			}
		}
	}
	return nil
}

func (h *hnsw) reset() error {
	h.Lock()
	defer h.Unlock()
	h.entryPointID = 0
	h.currentMaximumLayer = 0
	h.initialInsertOnce = &sync.Once{}
	h.nodes = make([]*vertex, initialSize)

	return h.commitLog.Reset()
}

func (h *hnsw) tombstonesAsDenyList() helpers.AllowList {
	deleteList := helpers.AllowList{}
	h.tombstoneLock.Lock()
	defer h.tombstoneLock.Unlock()

	tombstones := h.tombstones
	for id := range tombstones {
		deleteList.Insert(id)
	}

	return deleteList
}

func (h *hnsw) getEntrypoint() uint64 {
	h.Lock()
	defer h.Unlock()

	return h.entryPointID
}

func (h *hnsw) copyTombstonesToAllowList() helpers.AllowList {
	h.tombstoneLock.Lock()
	defer h.tombstoneLock.Unlock()

	deleteList := helpers.AllowList{}
	lenOfNodes := uint64(len(h.nodes))

	for id := range h.tombstones {
		if lenOfNodes <= id {
			// we're trying to delete an id outside the possible range, nothing to do
			continue
		}

		deleteList.Insert(id)
	}

	return deleteList
}

// CleanUpTombstonedNodes removes nodes with a tombstone and reassigns
// edges that were previously pointing to the tombstoned nodes
func (h *hnsw) CleanUpTombstonedNodes() error {
	h.metrics.StartCleanup(1)
	defer h.metrics.EndCleanup(1)

	deleteList := h.copyTombstonesToAllowList()
	if len(deleteList) == 0 {
		return nil
	}

	if err := h.reassignNeighborsOf(deleteList); err != nil {
		return errors.Wrap(err, "reassign neighbor edges")
	}

	for id := range deleteList {
		if h.getEntrypoint() == id {
			// this a special case because:
			//
			// 1. we need to find a new entrypoint, if this is the last point on this
			// level, we need to find an entrypoint on a lower level
			// 2. there is a risk that this is the only node in the entire graph. In
			// this case we must reset the graph
			h.Lock()
			node := h.nodes[id]
			h.Unlock()
			if err := h.deleteEntrypoint(node, deleteList); err != nil {
				return errors.Wrap(err, "delete entrypoint")
			}
		}
	}

	for id := range deleteList {
		if err := h.removeTombstoneAndNode(id); err != nil {
			return err
		}
	}

	if h.isEmpty() {
		if err := h.reset(); err != nil {
			return err
		}
	}

	return nil
}

func (h *hnsw) reassignNeighborsOf(deleteList helpers.AllowList) error {
	h.Lock()
	size := len(h.nodes)
	currentEntrypoint := h.entryPointID
	h.Unlock()

	for n := 0; n < size; n++ {
		neighbor := uint64(n)
		h.Lock()
		neighborNode := h.nodes[neighbor]
		h.Unlock()

		if neighborNode == nil || deleteList.Contains(neighborNode.id) {
			continue
		}

		neighborVec, err := h.vectorForID(context.Background(), neighbor)
		if err != nil {
			var e storobj.ErrNotFound
			if errors.As(err, &e) {
				h.handleDeletedNode(e.DocID)
				continue
			} else {
				// not a typed error, we can recover from, return with err
				return errors.Wrap(err, "get neighbor vec")
			}
		}
		neighborNode.Lock()
		neighborLevel := neighborNode.level
		connections := neighborNode.connections
		neighborNode.Unlock()

		if !connectionsPointTo(connections, deleteList) {
			// nothing needs to be changed, skip
			continue
		}

		entryPointID, err := h.findBestEntrypointForNode(h.currentMaximumLayer,
			neighborLevel, currentEntrypoint, neighborVec)
		if err != nil {
			return errors.Wrap(err, "find best entrypoint")
		}

		if entryPointID == neighbor {
			// if we use ourselves as entrypoint and delete all connections in the
			// next step, we won't find any neighbors, so we need to use an
			// alternative entryPoint in this round

			if h.isOnlyNode(&vertex{id: neighbor}, deleteList) {
				neighborNode.Lock()
				// delete all existing connections before re-assigning
				neighborNode.connections = map[int][]uint64{}
				neighborNode.Unlock()

				if err := h.commitLog.ClearLinks(neighbor); err != nil {
					return err
				}
				continue
			}

			tmpDenyList := deleteList.DeepCopy()
			tmpDenyList.Insert(entryPointID)

			alternative, level := h.findNewLocalEntrypoint(tmpDenyList, h.currentMaximumLayer,
				entryPointID)
			neighborLevel = level // reduce in case no neighbor is at our level
			entryPointID = alternative
		}

		neighborNode.markAsMaintenance()
		neighborNode.Lock()
		// delete all existing connections before re-assigning
		neighborNode.connections = map[int][]uint64{}
		neighborNode.Unlock()
		if err := h.commitLog.ClearLinks(neighbor); err != nil {
			return err
		}

		if err := h.findAndConnectNeighbors(neighborNode, entryPointID, neighborVec,
			neighborLevel, h.currentMaximumLayer, deleteList); err != nil {
			return errors.Wrap(err, "find and connect neighbors")
		}
		neighborNode.unmarkAsMaintenance()

		h.metrics.CleanedUp()
	}

	return nil
}

func connectionsPointTo(connections map[int][]uint64, needles helpers.AllowList) bool {
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
	oldEntrypoint uint64) (uint64, int, bool) {
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

		h.Lock()
		maxNodes := len(h.nodes)
		h.Unlock()

		for i := 0; i < maxNodes; i++ {
			if h.getEntrypoint() != oldEntrypoint {
				// entrypoint has already been changed (this could be due to a new import
				// for example, nothing to do for us
				return 0, 0, false
			}

			if denyList.Contains(uint64(i)) {
				continue
			}
			h.Lock()
			candidate := h.nodes[i]
			h.Unlock()

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
	oldEntrypoint uint64) (uint64, int) {
	if h.getEntrypoint() != oldEntrypoint {
		// the current global entrypoint is different from our local entrypoint, so
		// we can just use the global one, as the global one is guaranteed to be
		// present on every level, i.e. it is always chosen from the highest
		// currently available level
		return h.getEntrypoint(), h.currentMaximumLayer
	}

	h.Lock()
	maxNodes := len(h.nodes)
	h.Unlock()

	for l := targetLevel; l >= 0; l-- {
		// ideally we can find a new entrypoint at the same level of the
		// to-be-deleted node. However, there is a chance it was the only node on
		// that level, in that case we need to look at the next lower level for a
		// better candidate
		for i := 0; i < maxNodes; i++ {
			if denyList.Contains(uint64(i)) {
				continue
			}
			h.Lock()
			candidate := h.nodes[i]
			h.Unlock()

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
	h.Lock()
	defer h.Unlock()

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

func (h *hnsw) removeTombstoneAndNode(id uint64) error {
	h.metrics.RemoveTombstone()
	h.tombstoneLock.Lock()
	h.nodes[id] = nil
	delete(h.tombstones, id)
	h.tombstoneLock.Unlock()

	if err := h.commitLog.DeleteNode(id); err != nil {
		return err
	}

	if err := h.commitLog.RemoveTombstone(id); err != nil {
		return err
	}

	return nil
}

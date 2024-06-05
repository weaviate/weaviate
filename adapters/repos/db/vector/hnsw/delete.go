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
	"fmt"
	"math"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	enterrors "github.com/weaviate/weaviate/entities/errors"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/cache"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storobj"
)

type breakCleanUpTombstonedNodesFunc func() bool

// Delete attaches a tombstone to an item so it can be periodically cleaned up
// later and the edges reassigned
func (h *hnsw) Delete(ids ...uint64) error {
	h.compressActionLock.RLock()
	defer h.compressActionLock.RUnlock()

	h.deleteVsInsertLock.Lock()
	defer h.deleteVsInsertLock.Unlock()

	h.deleteLock.Lock()
	defer h.deleteLock.Unlock()

	before := time.Now()
	defer h.metrics.TrackDelete(before, "total")

	if err := h.addTombstone(ids...); err != nil {
		return err
	}

	for _, id := range ids {
		h.metrics.DeleteVector()

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
			continue
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
	}

	return nil
}

func (h *hnsw) resetIfEmpty() (empty bool, err error) {
	h.resetLock.Lock()
	defer h.resetLock.Unlock()
	h.Lock()
	defer h.Unlock()

	empty = func() bool {
		h.shardedNodeLocks.RLock(h.entryPointID)
		defer h.shardedNodeLocks.RUnlock(h.entryPointID)

		return h.isEmptyUnlocked()
	}()
	// It can happen that between calls of isEmptyUnlocked and resetUnlocked
	// values of h.nodes will change (due to locks being RUnlocked and Locked again)
	// This is acceptable in order to avoid long Locking of all striped locks
	if empty {
		h.shardedNodeLocks.LockAll()
		defer h.shardedNodeLocks.UnlockAll()

		return true, h.resetUnlocked()
	}
	return false, nil
}

func (h *hnsw) resetIfOnlyNode(needle *vertex, denyList helpers.AllowList) (onlyNode bool, err error) {
	h.resetLock.Lock()
	defer h.resetLock.Unlock()
	h.Lock()
	defer h.Unlock()

	onlyNode = func() bool {
		h.shardedNodeLocks.RLockAll()
		defer h.shardedNodeLocks.RUnlockAll()

		return h.isOnlyNodeUnlocked(needle, denyList)
	}()
	// It can happen that between calls of isOnlyNodeUnlocked and resetUnlocked
	// values of h.nodes will change (due to locks being RUnlocked and Locked again)
	// This is acceptable in order to avoid long Locking of all striped locks
	if onlyNode {
		h.shardedNodeLocks.LockAll()
		defer h.shardedNodeLocks.UnlockAll()

		return true, h.resetUnlocked()
	}
	return false, nil
}

func (h *hnsw) resetUnlocked() error {
	h.resetCtxCancel()
	resetCtx, resetCtxCancel := context.WithCancel(context.Background())
	h.resetCtx = resetCtx
	h.resetCtxCancel = resetCtxCancel

	h.entryPointID = 0
	h.currentMaximumLayer = 0
	h.initialInsertOnce = &sync.Once{}
	h.nodes = make([]*vertex, cache.InitialSize)

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

	// First of all, check if we even have enough tombstones to justify a
	// cleanup.  Cleaning up tombstones requires iteration over every possible
	// node in the graph which also includes locking portions of the graph, so we
	// want to make sure we have enough tombstones to justify the cleanup.
	numberOfTombstones := int64(len(h.tombstones))
	if numberOfTombstones == 0 {
		return false, nil
	}

	if numberOfTombstones < minTombstonesPerCycle() {
		h.logger.WithFields(logrus.Fields{
			"action":           "tombstone_cleanup_skipped",
			"class":            h.className,
			"shard":            h.shardName,
			"tombstones_total": numberOfTombstones,
			"tombstones_min":   minTombstonesPerCycle(),
			"tombstones_max":   maxTombstonesPerCycle(),
		}).Debugf("class %s: shard %s: skipping tombstone cleanup, not enough tombstones", h.className, h.shardName)
		return false, nil
	}

	// If we have a very high number of tombstones, we run into scaling issues.
	// Simply operations, such as copying lists require a lot of memory
	// allocations, etc.
	//
	// In addition, the cycle would run too long, preventing feedback. With a
	// hard limit like this, we do risk that we create connections that will need
	// to be touched again in the next cycle, but this is a tradeoff we're
	// willing to make.

	elementsOnList := int64(0)
	for id := range h.tombstones {
		if elementsOnList >= maxTombstonesPerCycle() {
			// we've reached the limit of tombstones we want to process in one
			break
		}

		if lenOfNodes <= id {
			// we're trying to delete an id outside the possible range, nothing to do
			continue
		}

		deleteList.Insert(id)
		elementsOnList++
	}

	return true, deleteList
}

// CleanUpTombstonedNodes removes nodes with a tombstone and reassigns
// edges that were previously pointing to the tombstoned nodes
func (h *hnsw) CleanUpTombstonedNodes(shouldAbort cyclemanager.ShouldAbortCallback) error {
	_, err := h.cleanUpTombstonedNodes(shouldAbort)
	return err
}

func (h *hnsw) cleanUpTombstonedNodes(shouldAbort cyclemanager.ShouldAbortCallback) (bool, error) {
	h.compressActionLock.RLock()
	defer h.compressActionLock.RUnlock()
	defer func() {
		err := recover()
		if err != nil {
			h.logger.WithField("panic", err).Errorf("class %s: tombstone cleanup panicked", h.className)
			debug.PrintStack()
		}
	}()

	start := time.Now()

	h.resetLock.Lock()
	resetCtx := h.resetCtx
	h.resetLock.Unlock()

	breakCleanUpTombstonedNodes := func() bool {
		return resetCtx.Err() != nil || shouldAbort()
	}

	executed := false
	ok, deleteList := h.copyTombstonesToAllowList(breakCleanUpTombstonedNodes)
	if !ok {
		return executed, nil
	}

	h.metrics.StartCleanup(tombstoneDeletionConcurrency())
	defer h.metrics.EndCleanup(tombstoneDeletionConcurrency())

	h.metrics.SetTombstoneDeleteListSize(deleteList.Len())

	h.tombstoneLock.Lock()
	total_tombstones := len(h.tombstones)
	h.tombstoneLock.Unlock()

	h.logger.WithFields(logrus.Fields{
		"action":              "tombstone_cleanup_begin",
		"class":               h.className,
		"shard":               h.shardName,
		"tombstones_in_cycle": deleteList.Len(),
		"tombstones_total":    total_tombstones,
	}).Infof("class %s: shard %s: starting tombstone cleanup", h.className, h.shardName)

	executed = true
	if ok, err := h.reassignNeighborsOf(deleteList, breakCleanUpTombstonedNodes); err != nil {
		return executed, err
	} else if !ok {
		return executed, nil
	}
	h.reassignNeighbor(h.getEntrypoint(), deleteList, breakCleanUpTombstonedNodes)

	if ok, err := h.replaceDeletedEntrypoint(deleteList, breakCleanUpTombstonedNodes); err != nil {
		return executed, err
	} else if !ok {
		return executed, nil
	}

	if ok, err := h.removeTombstonesAndNodes(deleteList, breakCleanUpTombstonedNodes); err != nil {
		return executed, err
	} else if !ok {
		return executed, nil
	}

	if _, err := h.resetIfEmpty(); err != nil {
		return executed, err
	}

	if executed {
		took := time.Since(start)
		h.logger.WithFields(logrus.Fields{
			"action":              "tombstone_cleanup_complete",
			"class":               h.className,
			"shard":               h.shardName,
			"tombstones_in_cycle": deleteList.Len(),
			"tombstones_total":    total_tombstones,
			"duration":            took,
		}).Infof("class %s: shard %s: completed tombstone cleanup in %s", h.className, h.shardName, took)
	}

	return executed, nil
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
			h.shardedNodeLocks.RLock(id)
			node := h.nodes[id]
			h.shardedNodeLocks.RUnlock(id)

			if err := h.deleteEntrypoint(node, deleteList); err != nil {
				return false, errors.Wrap(err, "delete entrypoint")
			}
		}
	}

	return true, nil
}

func maxTombstonesPerCycle() int64 {
	if v := os.Getenv("TOMBSTONE_DELETION_MAX_PER_CYCLE"); v != "" {
		asInt, err := strconv.Atoi(v)
		if err == nil && asInt > 0 {
			return int64(asInt)
		}
	}
	return math.MaxInt64
}

func minTombstonesPerCycle() int64 {
	if v := os.Getenv("TOMBSTONE_DELETION_MIN_PER_CYCLE"); v != "" {
		asInt, err := strconv.Atoi(v)
		if err == nil && asInt > 0 {
			return int64(asInt)
		}
	}
	return 0
}

func tombstoneDeletionConcurrency() int {
	if v := os.Getenv("TOMBSTONE_DELETION_CONCURRENCY"); v != "" {
		asInt, err := strconv.Atoi(v)
		if err == nil && asInt > 0 {
			return asInt
		}
	}
	return runtime.GOMAXPROCS(0) / 2
}

func (h *hnsw) reassignNeighborsOf(deleteList helpers.AllowList, breakCleanUpTombstonedNodes breakCleanUpTombstonedNodesFunc) (ok bool, err error) {
	h.RLock()
	size := len(h.nodes)
	h.RUnlock()
	h.resetLock.Lock()
	defer h.resetLock.Unlock()

	g, ctx := enterrors.NewErrorGroupWithContextWrapper(h.logger, h.shutdownCtx)
	ch := make(chan uint64)

	for i := 0; i < tombstoneDeletionConcurrency(); i++ {
		g.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return nil
				case deletedID, ok := <-ch:
					if !ok {
						return nil
					}
					h.shardedNodeLocks.RLock(deletedID)
					if uint64(size) < deletedID || h.nodes[deletedID] == nil {
						h.shardedNodeLocks.RUnlock(deletedID)
						continue
					}
					h.shardedNodeLocks.RUnlock(deletedID)
					if h.getEntrypoint() != deletedID {
						if _, err := h.reassignNeighbor(deletedID, deleteList, breakCleanUpTombstonedNodes); err != nil {
							h.logger.WithError(err).WithField("action", "hnsw_tombstone_cleanup_error").
								Errorf("class %s: shard %s: reassign neighbor", h.className, h.shardName)
						}
					}
				}
			}
		})
	}

LOOP:
	for i := 0; i < size; i++ {
		select {
		case ch <- uint64(i):
			if i%1_000_000 == 0 {
				// the interval of 1M is rather arbitrary, but if we have less than 1M
				// nodes in the graph tombstones cleanup should be so fast, we don't
				// need to log the progress.
				h.logger.WithFields(logrus.Fields{
					"action":          "tombstone_cleanup_progress",
					"class":           h.className,
					"shard":           h.shardName,
					"total_nodes":     size,
					"processed_nodes": i,
				}).
					Debugf("class %s: shard %s: %d/%d nodes processed", h.className, h.shardName, i, size)
			}
		case <-ctx.Done():
			// before https://github.com/weaviate/weaviate/issues/4615 the context
			// would not be canceled if a routine panicked. However, with the fix, it is
			// now valid to wait for a cancelation – even if a panic occurs.
			break LOOP
		}
	}

	close(ch)

	err = g.Wait()
	if errors.Is(err, context.Canceled) {
		h.logger.Errorf("class %s: tombstone cleanup canceled", h.className)
		return false, nil
	}

	return true, err
}

func (h *hnsw) reassignNeighbor(
	neighbor uint64,
	deleteList helpers.AllowList,
	breakCleanUpTombstonedNodes breakCleanUpTombstonedNodesFunc,
) (ok bool, err error) {
	if breakCleanUpTombstonedNodes() {
		return false, nil
	}

	h.metrics.TombstoneReassignNeighbor()

	h.RLock()
	h.shardedNodeLocks.RLock(neighbor)
	neighborNode := h.nodes[neighbor]
	h.shardedNodeLocks.RUnlock(neighbor)
	currentMaximumLayer := h.currentMaximumLayer
	h.RUnlock()

	if neighborNode == nil || deleteList.Contains(neighborNode.id) {
		return true, nil
	}

	var neighborVec []float32
	var compressorDistancer compressionhelpers.CompressorDistancer
	if h.compressed.Load() {
		compressorDistancer, err = h.compressor.NewDistancerFromID(neighbor)
	} else {
		neighborVec, err = h.cache.Get(context.Background(), neighbor)
	}

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

	neighborNode.markAsMaintenance()

	// the new recursive implementation no longer needs an entrypoint, so we can
	// just pass this dummy value to make the neighborFinderConnector happy
	dummyEntrypoint := uint64(0)
	if err := h.reconnectNeighboursOf(neighborNode, dummyEntrypoint, neighborVec, compressorDistancer,
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

	h.metrics.TombstoneFindGlobalEntrypoint()

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

			h.shardedNodeLocks.RLock(uint64(i))
			candidate := h.nodes[i]
			h.shardedNodeLocks.RUnlock(uint64(i))

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

	if h.isEmpty() {
		return 0, 0, false
	}

	if h.isOnlyNode(&vertex{id: oldEntrypoint}, denyList) {
		return 0, 0, false
	}

	// we made it through the entire graph and didn't find a new entrypoint all
	// the way down to level 0. This can only mean the graph is empty, which is
	// unexpected. This situation should have been prevented by the deleteLock.
	panic(fmt.Sprintf(
		"class %s: shard %s: findNewEntrypoint called on an empty hnsw graph",
		h.className, h.shardName))
}

// returns entryPointID, level and whether a change occurred
func (h *hnsw) findNewLocalEntrypoint(denyList helpers.AllowList, targetLevel int,
	oldEntrypoint uint64,
) (uint64, int, error) {
	if h.getEntrypoint() != oldEntrypoint {
		// the current global entrypoint is different from our local entrypoint, so
		// we can just use the global one, as the global one is guaranteed to be
		// present on every level, i.e. it is always chosen from the highest
		// currently available level
		h.RLock()
		defer h.RUnlock()
		return h.entryPointID, h.currentMaximumLayer, nil
	}

	h.metrics.TombstoneFindLocalEntrypoint()

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

			h.shardedNodeLocks.RLock(uint64(i))
			candidate := h.nodes[i]
			h.shardedNodeLocks.RUnlock(uint64(i))

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
			return uint64(i), l, nil
		}
	}

	if h.isEmpty() {
		return 0, 0, nil
	}

	if h.isOnlyNode(&vertex{id: oldEntrypoint}, denyList) {
		return 0, 0, nil
	}

	return 0, 0, fmt.Errorf("class %s: shard %s: findNewLocalEntrypoint called on an empty hnsw graph", h.className, h.shardName)
}

func (h *hnsw) isOnlyNode(needle *vertex, denyList helpers.AllowList) bool {
	h.RLock()
	h.shardedNodeLocks.RLockAll()
	defer h.RUnlock()
	defer h.shardedNodeLocks.RUnlockAll()

	return h.isOnlyNodeUnlocked(needle, denyList)
}

func (h *hnsw) isOnlyNodeUnlocked(needle *vertex, denyList helpers.AllowList) bool {
	for _, node := range h.nodes {
		if node == nil || node.id == needle.id || denyList.Contains(node.id) || len(node.connections) == 0 {
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

func (h *hnsw) addTombstone(ids ...uint64) error {
	h.tombstoneLock.Lock()
	defer h.tombstoneLock.Unlock()

	for _, id := range ids {
		h.metrics.AddTombstone()
		h.tombstones[id] = struct{}{}
		if err := h.commitLog.AddTombstone(id); err != nil {
			return err
		}
	}
	return nil
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
			h.shardedNodeLocks.Lock(id)
			h.nodes[id] = nil
			h.shardedNodeLocks.Unlock(id)
			if h.compressed.Load() {
				h.compressor.Delete(context.TODO(), id)
			} else {
				h.cache.Delete(context.TODO(), id)
			}
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

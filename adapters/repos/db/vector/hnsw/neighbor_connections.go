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
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/visited"
	"github.com/weaviate/weaviate/entities/storobj"
)

func (h *hnsw) findAndConnectNeighbors(ctx context.Context, node *vertex,
	entryPointID uint64, nodeVec []float32, distancer compressionhelpers.CompressorDistancer, targetLevel, currentMaxLevel int,
	denyList helpers.AllowList,
) error {
	nfc := newNeighborFinderConnector(h, node, entryPointID, nodeVec, distancer, targetLevel,
		currentMaxLevel, denyList, false)

	return nfc.Do(ctx)
}

func (h *hnsw) reconnectNeighboursOf(ctx context.Context, node *vertex,
	entryPointID uint64, nodeVec []float32, distancer compressionhelpers.CompressorDistancer, targetLevel, currentMaxLevel int,
	denyList helpers.AllowList,
) error {
	nfc := newNeighborFinderConnector(h, node, entryPointID, nodeVec, distancer, targetLevel,
		currentMaxLevel, denyList, true)

	return nfc.Do(ctx)
}

type neighborFinderConnector struct {
	ctx             context.Context
	graph           *hnsw
	node            *vertex
	entryPointID    uint64
	entryPointDist  float32
	nodeVec         []float32
	distancer       compressionhelpers.CompressorDistancer
	targetLevel     int
	currentMaxLevel int
	denyList        helpers.AllowList
	// bufLinksLog     BufferedLinksLogger
	tombstoneCleanupNodes bool
}

func newNeighborFinderConnector(graph *hnsw, node *vertex, entryPointID uint64,
	nodeVec []float32, distancer compressionhelpers.CompressorDistancer, targetLevel, currentMaxLevel int,
	denyList helpers.AllowList, tombstoneCleanupNodes bool,
) *neighborFinderConnector {
	return &neighborFinderConnector{
		ctx:                   graph.shutdownCtx,
		graph:                 graph,
		node:                  node,
		entryPointID:          entryPointID,
		nodeVec:               nodeVec,
		distancer:             distancer,
		targetLevel:           targetLevel,
		currentMaxLevel:       currentMaxLevel,
		denyList:              denyList,
		tombstoneCleanupNodes: tombstoneCleanupNodes,
	}
}

func (n *neighborFinderConnector) Do(ctx context.Context) error {
	for level := min(n.targetLevel, n.currentMaxLevel); level >= 0; level-- {
		err := n.doAtLevel(ctx, level)
		if err != nil {
			return errors.Wrapf(err, "at level %d", level)
		}
	}

	return nil
}

func (n *neighborFinderConnector) processNode(id uint64) (float32, error) {
	var dist float32
	var err error

	if n.distancer == nil {
		dist, err = n.graph.distToNode(n.distancer, id, n.nodeVec)
	} else {
		dist, err = n.distancer.DistanceToNode(id)
	}

	var e storobj.ErrNotFound
	if errors.As(err, &e) {
		n.graph.handleDeletedNode(e.DocID, "processNode")
		return math.MaxFloat32, nil
	}
	if err != nil {
		return math.MaxFloat32, fmt.Errorf(
			"calculate distance between insert node and entrypoint: %w", err)
	}
	return dist, nil
}

func (n *neighborFinderConnector) processRecursively(from uint64, results *priorityqueue.Queue[any], visited visited.ListSet, level, top int) error {
	if top <= 0 {
		return nil
	}
	if err := n.ctx.Err(); err != nil {
		return err
	}

	n.graph.RLock()
	nodesLen := uint64(len(n.graph.nodes))
	n.graph.RUnlock()
	var pending []uint64
	// lock the nodes slice
	n.graph.shardedNodeLocks.RLock(from)
	if nodesLen < from || n.graph.nodes[from] == nil {
		n.graph.handleDeletedNode(from, "processRecursively")
		n.graph.shardedNodeLocks.RUnlock(from)
		return nil
	}
	// lock the node itself
	n.graph.nodes[from].Lock()
	if level >= len(n.graph.nodes[from].connections) {
		n.graph.nodes[from].Unlock()
		n.graph.shardedNodeLocks.RUnlock(from)
		return nil
	}
	connections := make([]uint64, len(n.graph.nodes[from].connections[level]))
	copy(connections, n.graph.nodes[from].connections[level])
	n.graph.nodes[from].Unlock()
	n.graph.shardedNodeLocks.RUnlock(from)
	for _, id := range connections {
		if visited.Visited(id) {
			continue
		}
		visited.Visit(id)
		if n.denyList.Contains(id) {
			pending = append(pending, id)
			continue
		}

		dist, err := n.processNode(id)
		if err != nil {
			var e storobj.ErrNotFound
			if errors.As(err, &e) {
				// node was deleted in the meantime
				continue
			} else {
				return err
			}
		}
		if results.Len() >= top && dist < results.Top().Dist {
			results.Pop()
			results.Insert(id, dist)
		} else if results.Len() < top {
			results.Insert(id, dist)
		}
	}
	for _, id := range pending {
		if results.Len() >= top {
			dist, err := n.processNode(id)
			if err != nil {
				var e storobj.ErrNotFound
				if errors.As(err, &e) {
					// node was deleted in the meantime
					continue
				}
				return err
			}
			if dist > results.Top().Dist {
				continue
			}
		}
		err := n.processRecursively(id, results, visited, level, top)
		if err != nil {
			return err
		}
	}
	return nil
}

func (n *neighborFinderConnector) doAtLevel(ctx context.Context, level int) error {
	before := time.Now()

	var results *priorityqueue.Queue[any]
	var extraIDs []uint64 = nil
	total := 0
	maxConnections := n.graph.maximumConnections

	if n.tombstoneCleanupNodes {
		results = n.graph.pools.pqResults.GetMax(n.graph.efConstruction)

		n.graph.pools.visitedListsLock.RLock()
		visited := n.graph.pools.visitedLists.Borrow()
		n.graph.pools.visitedListsLock.RUnlock()
		n.node.Lock()
		connections := make([]uint64, len(n.node.connections[level]))
		copy(connections, n.node.connections[level])
		n.node.Unlock()
		visited.Visit(n.node.id)
		top := n.graph.efConstruction
		var pending []uint64 = nil

		for _, id := range connections {
			visited.Visit(id)
			if n.denyList.Contains(id) {
				pending = append(pending, id)
				continue
			}
			extraIDs = append(extraIDs, id)
			top--
			total++
		}
		for _, id := range pending {
			visited.Visit(id)
			err := n.processRecursively(id, results, visited, level, top)
			if err != nil {
				n.graph.pools.visitedListsLock.RLock()
				n.graph.pools.visitedLists.Return(visited)
				n.graph.pools.visitedListsLock.RUnlock()
				return err
			}
		}
		n.graph.pools.visitedListsLock.RLock()
		n.graph.pools.visitedLists.Return(visited)
		n.graph.pools.visitedListsLock.RUnlock()
		// use dynamic max connections only during tombstone cleanup
		maxConnections = n.maximumConnections(level)
	} else {
		if err := n.pickEntrypoint(); err != nil {
			return errors.Wrap(err, "pick entrypoint at level beginning")
		}
		eps := priorityqueue.NewMin[any](1)
		eps.Insert(n.entryPointID, n.entryPointDist)
		var err error

		results, err = n.graph.searchLayerByVectorWithDistancer(ctx, n.nodeVec, eps, n.graph.efConstruction,
			level, nil, n.distancer)
		if err != nil {
			return errors.Wrapf(err, "search layer at level %d", level)
		}

		n.graph.insertMetrics.findAndConnectSearch(before)
		before = time.Now()
	}

	if err := n.graph.selectNeighborsHeuristic(results, maxConnections-total, n.denyList); err != nil {
		return errors.Wrap(err, "heuristic")
	}

	n.graph.insertMetrics.findAndConnectHeuristic(before)
	before = time.Now()

	// // for distributed spike
	// neighborsAtLevel[level] = neighbors

	neighbors := make([]uint64, total, total+results.Len())
	copy(neighbors, extraIDs)
	for results.Len() > 0 {
		id := results.Pop().ID
		neighbors = append(neighbors, id)
	}

	n.graph.pools.pqResults.Put(results)

	neighborsCpy := neighbors
	// the node will potentially own the neighbors slice (cf. hnsw.vertex#setConnectionsAtLevel).
	// if so, we need to create a copy
	owned := n.node.setConnectionsAtLevel(level, neighbors)
	if owned {
		n.node.Lock()
		neighborsCpy = make([]uint64, len(neighbors))
		copy(neighborsCpy, neighbors)
		n.node.Unlock()
	}

	if err := n.graph.commitLog.ReplaceLinksAtLevel(n.node.id, level, neighborsCpy); err != nil {
		return errors.Wrapf(err, "ReplaceLinksAtLevel node %d at level %d", n.node.id, level)
	}

	for _, neighborID := range neighborsCpy {
		if err := n.connectNeighborAtLevel(neighborID, level); err != nil {
			return errors.Wrapf(err, "connect neighbor %d", neighborID)
		}
	}

	if len(neighbors) > 0 {
		// there could be no neighbors left, if all are marked deleted, in this
		// case, don't change the entrypoint
		nextEntryPointID := neighborsCpy[len(neighbors)-1]
		if nextEntryPointID == n.node.id {
			return nil
		}

		n.entryPointID = nextEntryPointID
	}

	n.graph.insertMetrics.findAndConnectUpdateConnections(before)
	return nil
}

func (n *neighborFinderConnector) connectNeighborAtLevel(neighborID uint64,
	level int,
) error {
	neighbor := n.graph.nodeByID(neighborID)
	if skip := n.skipNeighbor(neighbor); skip {
		return nil
	}

	neighbor.Lock()
	defer neighbor.Unlock()
	if level > neighbor.level {
		// upgrade neighbor level if the level is out of sync due to a delete re-assign
		neighbor.upgradeToLevelNoLock(level)
	}
	currentConnections := neighbor.connectionsAtLevelNoLock(level)

	maximumConnections := n.maximumConnections(level)
	if len(currentConnections) < maximumConnections {
		// we can simply append
		// updatedConnections = append(currentConnections, n.node.id)
		neighbor.appendConnectionAtLevelNoLock(level, n.node.id, maximumConnections)
		if err := n.graph.commitLog.AddLinkAtLevel(neighbor.id, level, n.node.id); err != nil {
			return err
		}
	} else {
		// we need to run the heuristic

		dist, err := n.graph.distBetweenNodes(n.node.id, neighborID)
		var e storobj.ErrNotFound
		if err != nil && errors.As(err, &e) {
			n.graph.handleDeletedNode(e.DocID, "connectNeighborAtLevel")
			// it seems either the node or the neighbor were deleted in the meantime,
			// there is nothing we can do now
			return nil
		}
		if err != nil {
			return errors.Wrapf(err, "dist between %d and %d", n.node.id, neighborID)
		}

		candidates := priorityqueue.NewMax[any](len(currentConnections) + 1)
		candidates.Insert(n.node.id, dist)

		for _, existingConnection := range currentConnections {
			dist, err := n.graph.distBetweenNodes(existingConnection, neighborID)
			var e storobj.ErrNotFound
			if errors.As(err, &e) {
				n.graph.handleDeletedNode(e.DocID, "connectNeighborAtLevel")
				// was deleted in the meantime
				continue
			}
			if err != nil {
				return errors.Wrapf(err, "dist between %d and %d", existingConnection, neighborID)
			}

			candidates.Insert(existingConnection, dist)
		}

		err = n.graph.selectNeighborsHeuristic(candidates, maximumConnections, n.denyList)
		if err != nil {
			return errors.Wrap(err, "connect neighbors")
		}

		neighbor.resetConnectionsAtLevelNoLock(level)
		if err := n.graph.commitLog.ClearLinksAtLevel(neighbor.id, uint16(level)); err != nil {
			return err
		}

		for candidates.Len() > 0 {
			id := candidates.Pop().ID
			neighbor.appendConnectionAtLevelNoLock(level, id, maximumConnections)
			if err := n.graph.commitLog.AddLinkAtLevel(neighbor.id, level, id); err != nil {
				return err
			}
		}
	}

	return nil
}

func (n *neighborFinderConnector) skipNeighbor(neighbor *vertex) bool {
	if neighbor == n.node {
		// don't connect to self
		return true
	}

	if neighbor == nil || n.graph.hasTombstone(neighbor.id) {
		// don't connect to tombstoned nodes. This would only increase the
		// cleanup that needs to be done. Even worse: A tombstoned node can be
		// cleaned up at any time, also while we are connecting to it. So,
		// while the node still exists right now, it might already be nil in
		// the next line, which would lead to a nil-pointer panic.
		return true
	}

	return false
}

func (n *neighborFinderConnector) maximumConnections(level int) int {
	if level == 0 {
		return n.graph.maximumConnectionsLayerZero
	}

	return n.graph.maximumConnections
}

func (n *neighborFinderConnector) pickEntrypoint() error {
	// the neighborFinderConnector always has a suggestion for an entrypoint that
	// it got from the outside, most of the times we can use this, but in some
	// cases we can't. To see if we can use it, three conditions need to be met:
	//
	// 1. it needs to exist in the graph, i.e. be not nil
	//
	// 2. it can't be under maintenance
	//
	// 3. we need to be able to obtain a vector for it

	candidate := n.entryPointID

	// for our search we will need a copy of the current deny list, however, the
	// cost of that copy can be significant. Let's first verify if the global
	// entrypoint candidate is usable. If yes, we can return early and skip the
	// copy.
	success, err := n.tryEpCandidate(candidate)
	if err != nil {
		var e storobj.ErrNotFound
		if !errors.As(err, &e) {
			return err
		}

		// node was deleted in the meantime
		// ignore the error and move to the logic below which will try more candidates
	}

	if success {
		// the global ep candidate is usable, let's skip the following logic (and
		// therefore avoid the copy)
		return nil
	}

	// The global candidate is not usable, we need to find a new one.
	localDeny := n.denyList.WrapOnWrite()

	// make sure the loop cannot block forever. In most cases, results should be
	// found within micro to milliseconds, this is just a last resort to handle
	// the unknown somewhat gracefully, for example if there is a bug in the
	// underlying object store and we cannot retrieve the vector in time, etc.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	n.graph.logger.WithFields(logrus.Fields{
		"action":   "pick_entrypoint",
		"duration": 60 * time.Second,
	}).Debug("context.WithTimeout")

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		success, err := n.tryEpCandidate(candidate)
		if err != nil {
			var e storobj.ErrNotFound
			if !errors.As(err, &e) {
				return err
			}

			// node was deleted in the meantime
			// ignore the error and try the next candidate
		}

		if success {
			return nil
		}

		// no success so far, we need to keep going and find a better candidate
		// make sure we never visit this candidate again
		localDeny.Insert(candidate)
		// now find a new one

		alternative, err := n.graph.findNewLocalEntrypoint(localDeny, candidate)
		if err != nil {
			return err
		}
		candidate = alternative
	}
}

func (n *neighborFinderConnector) tryEpCandidate(candidate uint64) (bool, error) {
	node := n.graph.nodeByID(candidate)
	if node == nil {
		return false, nil
	}

	if node.isUnderMaintenance() {
		return false, nil
	}

	var dist float32
	var err error
	if n.distancer == nil {
		dist, err = n.graph.distToNode(n.distancer, candidate, n.nodeVec)
	} else {
		dist, err = n.distancer.DistanceToNode(candidate)
	}
	var e storobj.ErrNotFound
	if errors.As(err, &e) {
		n.graph.handleDeletedNode(e.DocID, "tryEpCandidate")
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("calculate distance between insert node and entrypoint: %w", err)
	}

	// we were able to calculate a distance, we're good
	n.entryPointDist = dist
	n.entryPointID = candidate
	return true, nil
}

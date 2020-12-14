//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package hnsw

import (
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"sync"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/adapters/repos/db/storobj"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/sirupsen/logrus"
)

type hnsw struct {
	// global lock to prevent concurrent map read/write, etc.
	sync.RWMutex

	// certain operations related to deleting, such as finding a new entrypoint
	// can only run sequentially, this separate lock helps assuring this without
	// blocking the general usage of the hnsw index
	deleteLock *sync.Mutex

	// Each node should not have more edges than this number
	maximumConnections int

	// Nodes in the lowest level have a separate (usually higher) max connection
	// limit
	maximumConnectionsLayerZero int

	// the current maximum can be smaller than the configured maximum because of
	// the exponentially decaying layer function. The initial entry is started at
	// layer 0, but this has the chance to grow with every subsequent entry
	currentMaximumLayer int

	// this is a point on the highest level, if we insert a new point with a
	// higher level it will become the new entry point. Note tat the level of
	// this point is always currentMaximumLayer
	entryPointID uint64

	// ef parameter used in construction phases, should be higher than ef during querying
	efConstruction int

	levelNormalizer float64

	nodes []*vertex

	vectorForID VectorForID

	vectorCacheDrop func()

	commitLog CommitLogger

	// a lookup of current tombstones (i.e. nodes that have received a tombstone,
	// but have not been cleaned up yet) Cleanup is the process of removal of all
	// outgoing edges to the tombstone as well as deleting the tombstone itself.
	// This process should happen periodically.
	tombstones map[uint64]struct{}

	// used for cancellation of the tombstone cleanup goroutine
	cancel chan struct{}

	// // for distributed spike, can be used to call a insertExternal on a different graph
	// insertHook func(node, targetLevel int, neighborsAtLevel map[int][]uint32)

	id       string
	rootPath string

	logger            logrus.FieldLogger
	distancerProvider distancer.Provider
}

type CommitLogger interface {
	AddNode(node *vertex) error
	SetEntryPointWithMaxLayer(id uint64, level int) error
	AddLinkAtLevel(nodeid uint64, level int, target uint64) error
	ReplaceLinksAtLevel(nodeid uint64, level int, targets []uint64) error
	AddTombstone(nodeid uint64) error
	RemoveTombstone(nodeid uint64) error
	DeleteNode(nodeid uint64) error
	ClearLinks(nodeid uint64) error
	Reset() error
	Drop() error
}

type MakeCommitLogger func() (CommitLogger, error)

type VectorForID func(ctx context.Context, id uint64) ([]float32, error)

// New creates a new HNSW index, the commit logger is provided through a thunk
// (a function which can be deferred). This is because creating a commit logger
// opens files for writing. However, checking whether a file is present, is a
// criterium for the index to see if it has to recover from disk or if its a
// truly new index. So instead the index is initialized, with un-biased disk
// checks first and only then is the commit logger created
func New(cfg Config) (*hnsw, error) {
	if err := cfg.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	if cfg.Logger == nil {
		logger := logrus.New()
		logger.Out = ioutil.Discard
		cfg.Logger = logger
	}

	vectorCache := newCache(cfg.VectorForIDThunk, cfg.Logger)
	index := &hnsw{
		maximumConnections: cfg.MaximumConnections,

		// inspired by original paper and other implementations
		maximumConnectionsLayerZero: 2 * cfg.MaximumConnections,

		// inspired by c++ implementation
		levelNormalizer:   1 / math.Log(float64(cfg.MaximumConnections)),
		efConstruction:    cfg.EFConstruction,
		nodes:             make([]*vertex, initialSize),
		vectorForID:       vectorCache.get,
		vectorCacheDrop:   vectorCache.drop,
		id:                cfg.ID,
		rootPath:          cfg.RootPath,
		tombstones:        map[uint64]struct{}{},
		logger:            cfg.Logger,
		distancerProvider: cfg.DistanceProvider,
		cancel:            make(chan struct{}),
		deleteLock:        &sync.Mutex{},
	}

	if err := index.init(cfg); err != nil {
		return nil, errors.Wrapf(err, "init index %q", index.id)
	}

	return index, nil
}

// TODO: use this for incoming replication
// func (h *hnsw) insertFromExternal(nodeId, targetLevel int, neighborsAtLevel map[int][]uint32) {
// 	defer m.addBuildingReplication(time.Now())

// 	// randomly introduce up to 50ms delay to account for network slowness
// 	time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond)

// 	var node *hnswVertex
// 	h.RLock()
// 	total := len(h.nodes)
// 	if total > nodeId {
// 		node = h.nodes[nodeId] // it could be that we implicitly added this node already because it was referenced
// 	}
// 	h.RUnlock()

// 	if node == nil {
// 		node = &hnswVertex{
// 			id:          nodeId,
// 			connections: make(map[int][]uint32),
// 			level:       targetLevel,
// 		}
// 	} else {
// 		node.level = targetLevel
// 	}

// 	if total == 0 {
// 		h.Lock()
// 		h.commitLog.SetEntryPointWithMaxLayer(node.id, 0)
// 		h.entryPointID = node.id
// 		h.currentMaximumLayer = 0
// 		node.connections = map[int][]uint32{}
// 		node.level = 0
// 		// h.nodes = make([]*hnswVertex, 100000)
// 		h.commitLog.AddNode(node)
// 		h.nodes[node.id] = node
// 		h.Unlock()
// 		return
// 	}

// 	currentMaximumLayer := h.currentMaximumLayer
// 	h.Lock()
// 	h.nodes[nodeId] = node
// 	h.commitLog.AddNode(node)
// 	h.Unlock()

// 	for level := min(targetLevel, currentMaximumLayer); level >= 0; level-- {
// 		neighbors := neighborsAtLevel[level]

// 		for _, neighborID := range neighbors {
// 			h.RLock()
// 			neighbor := h.nodes[neighborID]
// 			if neighbor == nil {
// 				// due to everything being parallel it could be that the linked neighbor
// 				// doesn't exist yet
// 				h.nodes[neighborID] = &hnswVertex{
// 					id:          int(neighborID),
// 					connections: make(map[int][]uint32),
// 				}
// 				neighbor = h.nodes[neighborID]
// 			}
// 			h.RUnlock()

// 			neighbor.linkAtLevel(level, uint32(nodeId), h.commitLog)
// 			node.linkAtLevel(level, uint32(neighbor.id), h.commitLog)

// 			neighbor.RLock()
// 			currentConnections := neighbor.connections[level]
// 			neighbor.RUnlock()

// 			maximumConnections := h.maximumConnections
// 			if level == 0 {
// 				maximumConnections = h.maximumConnectionsLayerZero
// 			}

// 			if len(currentConnections) <= maximumConnections {
// 				// nothing to do, skip
// 				continue
// 			}

// 			// TODO: support both neighbor selection algos
// 			updatedConnections := h.selectNeighborsSimpleFromId(nodeId, currentConnections, maximumConnections)

// 			neighbor.Lock()
// 			h.commitLog.ReplaceLinksAtLevel(neighbor.id, level, updatedConnections)
// 			neighbor.connections[level] = updatedConnections
// 			neighbor.Unlock()
// 		}
// 	}

// 	if targetLevel > h.currentMaximumLayer {
// 		h.Lock()
// 		h.commitLog.SetEntryPointWithMaxLayer(nodeId, targetLevel)
// 		h.entryPointID = nodeId
// 		h.currentMaximumLayer = targetLevel
// 		h.Unlock()
// 	}

// }

func (h *hnsw) Add(id uint64, vector []float32) error {
	if len(vector) == 0 {
		return fmt.Errorf("insert called with nil-vector")
	}

	node := &vertex{
		id: id,
	}

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
	node.connections = map[int][]uint64{}
	node.level = 0
	if err := h.commitLog.AddNode(node); err != nil {
		return err
	}

	h.nodes[node.id] = node

	// go h.insertHook(node.id, 0, node.connections)
	return nil
}

func (h *hnsw) insert(node *vertex, nodeVec []float32) error {
	if h.isEmpty() {
		return h.insertInitialElement(node, nodeVec)
	}
	// initially use the "global" entrypoint which is guaranteed to be on the
	// currently highest layer
	entryPointID := h.entryPointID

	// initially use the level of the entrypoint which is the highest level of
	// the h-graph in the first iteration
	currentMaximumLayer := h.currentMaximumLayer

	targetLevel := int(math.Floor(-math.Log(rand.Float64()*h.levelNormalizer))) - 1

	// before = time.Now()
	node.Lock()
	// m.addBuildingItemLocking(before)
	node.level = targetLevel
	node.connections = map[int][]uint64{}
	node.Unlock()

	// before = time.Now()
	h.Lock()
	// m.addBuildingLocking(before)
	nodeId := node.id
	err := h.growIndexToAccomodateNode(node.id, h.logger)
	if err != nil {
		h.Unlock()
		return errors.Wrapf(err, "grow HNSW index to accommodate node %d", node.id)
	}
	h.nodes[nodeId] = node
	if err := h.commitLog.AddNode(node); err != nil {
		h.Unlock()
		return err
	}

	h.Unlock()

	entryPointID, err = h.findBestEntrypointForNode(currentMaximumLayer, targetLevel,
		entryPointID, nodeVec)
	if err != nil {
		return errors.Wrap(err, "find best entrypoint")
	}

	if err := h.findAndConnectNeighbors(node, entryPointID, nodeVec,
		targetLevel, currentMaximumLayer, nil); err != nil {
		return errors.Wrap(err, "find and connect neighbors")
	}

	// go h.insertHook(nodeId, targetLevel, neighborsAtLevel)

	if targetLevel > h.currentMaximumLayer {
		// before = time.Now()
		h.Lock()
		// m.addBuildingLocking(before)
		if err := h.commitLog.SetEntryPointWithMaxLayer(nodeId, targetLevel); err != nil {
			h.Unlock()
			return err
		}

		h.entryPointID = nodeId
		h.currentMaximumLayer = targetLevel
		h.Unlock()
	}

	return nil
}

func (v *vertex) linkAtLevel(level int, target uint64, cl CommitLogger) error {
	v.Lock()
	defer v.Unlock()

	if err := cl.AddLinkAtLevel(v.id, level, target); err != nil {
		return err
	}

	if targetContained(v.connections[level], target) {
		// already linked, nothing to do
		return nil
	}

	v.connections[level] = append(v.connections[level], target)
	return nil
}

func targetContained(haystack []uint64, needle uint64) bool {
	for _, candidate := range haystack {
		if candidate == needle {
			return true
		}
	}

	return false
}

func (h *hnsw) findBestEntrypointForNode(currentMaxLevel, targetLevel int,
	entryPointID uint64, nodeVec []float32) (uint64, error) {
	// in case the new target is lower than the current max, we need to search
	// each layer for a better candidate and update the candidate
	for level := currentMaxLevel; level > targetLevel; level-- {
		tmpBST := &binarySearchTreeGeneric{}
		dist, ok, err := h.distBetweenNodeAndVec(entryPointID, nodeVec)
		if err != nil {
			return 0, errors.Wrapf(err,
				"calculate distance between insert node and entry point at level %d", level)
		}
		if !ok {
			continue
		}

		tmpBST.insert(entryPointID, dist)
		res, err := h.searchLayerByVector(nodeVec, *tmpBST, 1, level, nil)
		if err != nil {
			return 0,
				errors.Wrapf(err, "update candidate: search layer at level %d", level)
		}
		if res.root != nil {
			// if we could find a new entrypoint, use it
			entryPointID = res.minimum().index
			// in case everything was tombstoned, stick with the existing one
		}
	}

	return entryPointID, nil
}

// TODO: split up. It's not as bad as it looks at first, as it has quite some
// long comments, however, it should still be made prettier
func (h *hnsw) findAndConnectNeighbors(node *vertex,
	entryPointID uint64, nodeVec []float32, targetLevel, currentMaxLevel int,
	denyList helpers.AllowList) error {
	results := &binarySearchTreeGeneric{}
	dist, ok, err := h.distBetweenNodeAndVec(entryPointID, nodeVec)
	if err != nil {
		return errors.Wrapf(err, "calculate distance between insert node and final entrypoint")
	}
	if !ok {
		return fmt.Errorf("entrypoint was deleted in the object store, " +
			"it has been flagged for cleanup and should be fixed in the next cleanup cycle")
	}

	results.insert(entryPointID, dist)
	// neighborsAtLevel := make(map[int][]uint32) // for distributed spike

	for level := min(targetLevel, currentMaxLevel); level >= 0; level-- {
		results, err = h.searchLayerByVector(nodeVec, *results, h.efConstruction,
			level, nil)
		if err != nil {
			return errors.Wrapf(err, "find neighbors: search layer at level %d", level)
		}

		if results.contains(node.id, 0) {
			// Make sure we don't get the node we're currently assigning on the
			// result list. This could lead to a self-link, but far worse it could
			// lead to using ourself as an entry point on the next lower level. In
			// the process of (re)-assigning edges it would be fatal to use ourselves
			// as an entrypoint, as there are only two possible scenarios: 1. This is
			// a new insert, so we don't have edges yet. 2. This is a re-assign after
			// a delete, so we did originally have edges, but they were cleared in
			// preparation for the re-assignment.
			//
			// So why is it so bad to have ourselves (without connections) as an
			// entrypoint? Because the exit condition in searchLayerByVector is if
			// the candidates distance is worse than the current worst distance.
			// Naturally, the node itself has the best distance (=0) to itself, so
			// we'd ignore all other elements. However, since the node - as outlined
			// before - has no nodes, the search wouldn't find any results. Thus we
			// also can't add any new connections, leading to an isolated node in the
			// graph. If that isolated node were to become the graphs entrypoint, the
			// graph is basically unusable.
			results.delete(node.id, 0)
		}

		// TODO: support both neighbor selection algos
		neighbors := h.selectNeighborsSimple(*results, h.maximumConnections, denyList)

		// // for distributed spike
		// neighborsAtLevel[level] = neighbors

		for _, neighborID := range neighbors {
			neighbor := h.nodeByID(neighborID)
			if neighbor == node {
				// don't connect to self
				continue
			}

			if neighbor == nil || h.hasTombstone(neighbor.id) {
				// don't connect to tombstoned nodes. This would only increase the
				// cleanup that needs to be done. Even worse: A tombstoned node can be
				// cleaned up at any time, also while we are connecting to it. So,
				// while the node still exists right now, it might already be nil in
				// the next line, which would lead to a nil-pointer panic.
				continue
			}

			if err := neighbor.linkAtLevel(level, node.id, h.commitLog); err != nil {
				return err
			}

			if err := node.linkAtLevel(level, neighbor.id, h.commitLog); err != nil {
				return err
			}

			// before = time.Now()
			neighbor.RLock()
			// m.addBuildingItemLocking(before)
			currentConnections := neighbor.connections[level]
			neighbor.RUnlock()

			maximumConnections := h.maximumConnections
			if level == 0 {
				maximumConnections = h.maximumConnectionsLayerZero
			}

			if len(currentConnections) <= maximumConnections {
				// nothing to do, skip
				continue
			}

			// TODO: support both neighbor selection algos
			updatedConnections, err := h.selectNeighborsSimpleFromId(node.id,
				currentConnections, maximumConnections, denyList)
			if err != nil {
				return errors.Wrap(err, "connect neighbors")
			}

			// before = time.Now()
			neighbor.Lock()
			// m.addBuildingItemLocking(before)
			if err := h.commitLog.ReplaceLinksAtLevel(neighbor.id, level,
				updatedConnections); err != nil {
				return err
			}

			neighbor.connections[level] = updatedConnections
			neighbor.Unlock()
		}
	}

	return nil
}

type vertex struct {
	id uint64
	sync.RWMutex
	level       int
	connections map[int][]uint64 // map[level][]connectedId
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (h *hnsw) distBetweenNodes(a, b uint64) (float32, bool, error) {
	// TODO: introduce single search/transaction context instead of spawning new
	// ones
	vecA, err := h.vectorForID(context.Background(), a)
	if err != nil {
		var e storobj.ErrNotFound
		if errors.As(err, &e) {
			h.handleDeletedNode(e.DocID)
			return 0, false, nil
		} else {
			// not a typed error, we can recover from, return with err
			return 0, false, errors.Wrapf(err,
				"could not get vector of object at docID %d", a)
		}
	}

	if len(vecA) == 0 {
		return 0, false, fmt.Errorf("got a nil or zero-length vector at docID %d", a)
	}

	vecB, err := h.vectorForID(context.Background(), b)
	if err != nil {
		var e storobj.ErrNotFound
		if errors.As(err, &e) {
			h.handleDeletedNode(e.DocID)
			return 0, false, nil
		} else {
			// not a typed error, we can recover from, return with err
			return 0, false, errors.Wrapf(err,
				"could not get vector of object at docID %d", b)
		}
	}

	if len(vecB) == 0 {
		return 0, false, fmt.Errorf("got a nil or zero-length vector at docID %d", b)
	}

	// there is no performance benefit (but also no penalty) in using the
	// reusable distancer here. However, it makes it much easier to switch out
	// the distance function if there is only a single type that is used to
	// calculate distances
	return h.distancerProvider.New(vecA).Distance(vecB)
}

func (h *hnsw) distBetweenNodeAndVec(node uint64, vecB []float32) (float32, bool, error) {
	// TODO: introduce single search/transaction context instead of spawning new
	// ones
	vecA, err := h.vectorForID(context.Background(), node)
	if err != nil {
		var e storobj.ErrNotFound
		if errors.As(err, &e) {
			h.handleDeletedNode(e.DocID)
			return 0, false, nil
		} else {
			// not a typed error, we can recover from, return with err
			return 0, false, errors.Wrapf(err,
				"could not get vector of object at docID %d", node)
		}
	}

	if len(vecA) == 0 {
		return 0, false, fmt.Errorf(
			"got a nil or zero-length vector at docID %d", node)
	}

	if len(vecB) == 0 {
		return 0, false, fmt.Errorf(
			"got a nil or zero-length vector as search vector")
	}

	// there is no performance benefit (but also no penalty) in using the
	// reusable distancer here. However, it makes it much easier to switch out
	// the distance function if there is only a single type that is used to
	// calculate distances
	return h.distancerProvider.New(vecA).Distance(vecB)
}

func (h *hnsw) Stats() {
	fmt.Printf("levels: %d\n", h.currentMaximumLayer)

	perLevelCount := map[int]uint{}

	for _, node := range h.nodes {
		if node == nil {
			continue
		}
		l := node.level
		if l == 0 && len(node.connections) == 0 {
			// filter out allocated space without nodes
			continue
		}
		c, ok := perLevelCount[l]
		if !ok {
			perLevelCount[l] = 0
		}

		perLevelCount[l] = c + 1
	}

	for level, count := range perLevelCount {
		fmt.Printf("unique count on level %d: %d\n", level, count)
	}
}

func (h *hnsw) isEmpty() bool {
	h.RLock()
	defer h.RUnlock()

	for _, node := range h.nodes {
		if node != nil {
			return false
		}
	}

	return true
}

func (h *hnsw) nodeByID(id uint64) *vertex {
	h.RLock()
	defer h.RUnlock()

	return h.nodes[id]
}

func (h *hnsw) Drop() error {
	// cancel commit log goroutine
	err := h.commitLog.Drop()
	if err != nil {
		return errors.Wrap(err, "commit log drop")
	}
	// cancel vector cache goroutine
	h.vectorCacheDrop()
	// cancel tombstone cleanup goroutine
	h.cancel <- struct{}{}
	return nil
}

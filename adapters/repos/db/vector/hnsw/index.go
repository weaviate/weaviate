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
	"math"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/inverted"
)

type hnsw struct {
	sync.RWMutex

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
	entryPointID int

	// ef parameter used in construction phases, should be higher than ef during querying
	efConstruction int

	levelNormalizer float64

	nodes []*vertex

	vectorForID VectorForID

	commitLog CommitLogger

	// a lookup of current tombstones (i.e. nodes that have received a tombstone,
	// but have not been cleaned up yet) Cleanup is the process of removal of all
	// outgoing edges to the tombstone as well as deleting the tombstone itself.
	// This process should happen periodically.
	tombstones map[int]struct{}

	// // for distributed spike, can be used to call a insertExternal on a different graph
	// insertHook func(node, targetLevel int, neighborsAtLevel map[int][]uint32)

	id       string
	rootPath string
}

type CommitLogger interface {
	AddNode(node *vertex) error
	SetEntryPointWithMaxLayer(id int, level int) error
	AddLinkAtLevel(nodeid int, level int, target uint32) error
	ReplaceLinksAtLevel(nodeid int, level int, targets []uint32) error
	AddTombstone(nodeid int) error
	RemoveTombstone(nodeid int) error
	DeleteNode(nodeid int) error
	ClearLinks(nodeid int) error
	Reset() error
}

type MakeCommitLogger func() (CommitLogger, error)

type VectorForID func(ctx context.Context, id int32) ([]float32, error)

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

	vectorCache := newCache(cfg.VectorForIDThunk)
	index := &hnsw{
		maximumConnections: cfg.MaximumConnections,

		// inspired by original paper and other implementations
		maximumConnectionsLayerZero: 2 * cfg.MaximumConnections,

		// inspired by c++ implementation
		levelNormalizer: 1 / math.Log(float64(cfg.MaximumConnections)),
		efConstruction:  cfg.EFConstruction,
		nodes:           make([]*vertex, initialSize),
		vectorForID:     vectorCache.get,
		id:              cfg.ID,
		rootPath:        cfg.RootPath,
		tombstones:      map[int]struct{}{},
	}

	if err := index.restoreFromDisk(); err != nil {
		return nil, errors.Wrapf(err, "restore hnsw index %q", cfg.ID)
	}

	// init commit logger for future writes
	cl, err := cfg.MakeCommitLoggerThunk()
	if err != nil {
		return nil, errors.Wrap(err, "create commit logger")
	}

	index.commitLog = cl
	index.registerMaintainence(cfg)

	return index, nil
}

// if a commit log is already present it will be read into memory, if not we
// start with an empty model
func (h *hnsw) restoreFromDisk() error {
	fileNames, err := getCommitFileNames(h.rootPath, h.id)
	if len(fileNames) == 0 {
		// nothing to do
		return nil
	}

	fd, err := os.Open(fileNames[0]) // TODO: support more than the first one
	if err != nil {
		return errors.Wrapf(err, "open commit log %q for reading", fileNames[0])
	}

	res, err := newDeserializer().Do(fd)
	if err != nil {
		return errors.Wrapf(err, "deserialize commit log %q", fileNames[0])
	}

	h.nodes = res.nodes
	h.currentMaximumLayer = int(res.level)
	h.entryPointID = int(res.entrypoint)
	h.tombstones = res.tombstones

	return nil
}

func (h *hnsw) registerMaintainence(cfg Config) {
	h.registerTombstoneCleanup(cfg)
}

func (h *hnsw) registerTombstoneCleanup(cfg Config) {
	if cfg.TombstoneCleanupInterval == 0 {
		// user is not interested in periodically cleaning up tombstones, clean up
		// will be manual. (This is also helpful in tests where we want to
		// explicitly control the point at which a cleanup happens)
		return
	}

	go func() {
		for {
			time.Sleep(cfg.TombstoneCleanupInterval)
			err := h.CleanUpTombstonedNodes()
			if err != nil {
				// TODO: log properly
				fmt.Printf("tombstone cleanup errored: %v\n", err)
			}
		}
	}()
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

func (h *hnsw) Add(id int, vector []float32) error {
	if vector == nil || len(vector) == 0 {
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
	h.commitLog.SetEntryPointWithMaxLayer(node.id, 0)
	h.entryPointID = node.id
	h.currentMaximumLayer = 0
	node.connections = map[int][]uint32{}
	node.level = 0
	h.commitLog.AddNode(node)
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
	node.connections = map[int][]uint32{}
	node.Unlock()

	// before = time.Now()
	h.Lock()
	// m.addBuildingLocking(before)
	nodeId := node.id
	err := h.growIndexToAccomodateNode(node.id)
	if err != nil {
		h.Unlock()
		return errors.Wrapf(err, "grow HNSW index to accomodate node %d", node.id)
	}
	h.nodes[nodeId] = node
	h.commitLog.AddNode(node)
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
		h.commitLog.SetEntryPointWithMaxLayer(nodeId, targetLevel)
		h.entryPointID = nodeId
		h.currentMaximumLayer = targetLevel
		h.Unlock()
	}

	return nil
}

func (v *vertex) linkAtLevel(level int, target uint32, cl CommitLogger) {
	v.Lock()
	cl.AddLinkAtLevel(v.id, level, target)
	if targetContained(v.connections[level], target) {
		// already linked, nothing to do
		v.Unlock()
		return
	}
	v.connections[level] = append(v.connections[level], target)
	v.Unlock()
}

func targetContained(haystack []uint32, needle uint32) bool {
	for _, candidate := range haystack {
		if candidate == needle {
			return true
		}
	}

	return false
}

func (h *hnsw) findBestEntrypointForNode(currentMaxLevel, targetLevel int,
	entryPointID int, nodeVec []float32) (int, error) {

	// in case the new target is lower than the current max, we need to search
	// each layer for a better candidate and update the candidate
	for level := currentMaxLevel; level > targetLevel; level-- {
		tmpBST := &binarySearchTreeGeneric{}
		dist, err := h.distBetweenNodeAndVec(entryPointID, nodeVec)
		if err != nil {
			return 0, errors.Wrapf(err,
				"calculate distance between insert node and entry point at level %d", level)
		}
		tmpBST.insert(entryPointID, dist)
		res, err := h.searchLayerByVector(nodeVec, *tmpBST, 1, level, nil)
		if err != nil {
			return 0,
				errors.Wrapf(err, "update candidate: search layer at level %d", level)
		}
		entryPointID = res.minimum().index
	}

	return entryPointID, nil
}

func (h *hnsw) findAndConnectNeighbors(node *vertex,
	entryPointID int, nodeVec []float32, targetLevel, currentMaxLevel int,
	denyList inverted.AllowList) error {
	var results = &binarySearchTreeGeneric{}
	dist, err := h.distBetweenNodeAndVec(entryPointID, nodeVec)
	if err != nil {
		return errors.Wrapf(err, "calculate distance between insert node and final entrypoint")
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
			// entrypoint? Because the exit condidtion in searchLayerByVector is if
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
			// before := time.Now()
			h.RLock()
			// m.addBuildingReadLocking(before)
			neighbor := h.nodes[neighborID]
			h.RUnlock()

			if neighbor == node {
				// don't connect to self
				continue
			}

			neighbor.linkAtLevel(level, uint32(node.id), h.commitLog)
			node.linkAtLevel(level, uint32(neighbor.id), h.commitLog)

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
			h.commitLog.ReplaceLinksAtLevel(neighbor.id, level, updatedConnections)
			neighbor.connections[level] = updatedConnections
			neighbor.Unlock()
		}
	}

	return nil
}

type vertex struct {
	id int
	sync.RWMutex
	level       int
	connections map[int][]uint32 // map[level][]connectedId
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (h *hnsw) distBetweenNodes(a, b int) (float32, error) {
	// TODO: introduce single search/transaction context instead of spawning new
	// ones
	vecA, err := h.vectorForID(context.Background(), int32(a))
	if err != nil {
		return 0, errors.Wrapf(err, "could not get vector of object at docID %d", a)
	}

	if vecA == nil || len(vecA) == 0 {
		return 0, fmt.Errorf("got a nil or zero-length vector at docID %d", a)
	}

	vecB, err := h.vectorForID(context.Background(), int32(b))
	if err != nil {
		return 0, errors.Wrapf(err, "could not get vector of object at docID %d", b)
	}

	if vecB == nil || len(vecB) == 0 {
		return 0, fmt.Errorf("got a nil or zero-length vector at docID %d", b)
	}

	return cosineDist(vecA, vecB)
}

func (h *hnsw) distBetweenNodeAndVec(node int, vecB []float32) (float32, error) {
	// TODO: introduce single search/transaction context instead of spawning new
	// ones
	vecA, err := h.vectorForID(context.Background(), int32(node))
	if err != nil {
		return 0, errors.Wrapf(err, "could not get vector of object at docID %d", node)
	}

	if vecA == nil || len(vecA) == 0 {
		return 0, fmt.Errorf("got a nil or zero-length vector at docID %d", node)
	}

	if vecB == nil || len(vecB) == 0 {
		return 0, fmt.Errorf("got a nil or zero-length vector as search vector")
	}

	return cosineDist(vecA, vecB)
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

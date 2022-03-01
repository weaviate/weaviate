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
	"fmt"
	"io/ioutil"
	"math"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/priorityqueue"
	"github.com/semi-technologies/weaviate/entities/storobj"
	"github.com/sirupsen/logrus"
)

type hnsw struct {
	// global lock to prevent concurrent map read/write, etc.
	sync.Mutex

	// certain operations related to deleting, such as finding a new entrypoint
	// can only run sequentially, this separate lock helps assuring this without
	// blocking the general usage of the hnsw index
	deleteLock *sync.Mutex

	tombstoneLock *sync.RWMutex

	// make sure the very first insert happens just once, otherwise we
	// accidentally overwrite previous entrypoints on parallel imports on an
	// empty graph
	initialInsertOnce *sync.Once

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

	// ef at search time
	ef int64

	// only used if ef=-1
	efMin    int64
	efMax    int64
	efFactor int64

	// on filtered searches with less than n elements, perform flat search
	flatSearchCutoff int64

	levelNormalizer float64

	nodes []*vertex

	vectorForID VectorForID

	cache cache

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

	cleanupInterval time.Duration

	pools *pools

	forbidFlat bool // mostly used in testing scenarios where we want to use the index even in scenarios where we typically wouldn't
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
	ClearLinksAtLevel(nodeid uint64, level uint16) error
	Reset() error
	Drop() error
	Flush() error
}

type BufferedLinksLogger interface {
	AddLinkAtLevel(nodeid uint64, level int, target uint64) error
	ReplaceLinksAtLevel(nodeid uint64, level int, targets []uint64) error
	Close() error // Close should Flush and Close
}

type MakeCommitLogger func() (CommitLogger, error)

type VectorForID func(ctx context.Context, id uint64) ([]float32, error)

// New creates a new HNSW index, the commit logger is provided through a thunk
// (a function which can be deferred). This is because creating a commit logger
// opens files for writing. However, checking whether a file is present, is a
// criterium for the index to see if it has to recover from disk or if its a
// truly new index. So instead the index is initialized, with un-biased disk
// checks first and only then is the commit logger created
func New(cfg Config, uc UserConfig) (*hnsw, error) {
	if err := cfg.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	if cfg.Logger == nil {
		logger := logrus.New()
		logger.Out = ioutil.Discard
		cfg.Logger = logger
	}

	normalizeOnRead := false
	if cfg.DistanceProvider.Type() == "cosine-dot" {
		normalizeOnRead = true
	}

	vectorCache := newShardedLockCache(cfg.VectorForIDThunk, uc.VectorCacheMaxObjects,
		cfg.Logger, normalizeOnRead)

	index := &hnsw{
		maximumConnections: uc.MaxConnections,

		// inspired by original paper and other implementations
		maximumConnectionsLayerZero: 2 * uc.MaxConnections,

		// inspired by c++ implementation
		levelNormalizer:   1 / math.Log(float64(uc.MaxConnections)),
		efConstruction:    uc.EFConstruction,
		ef:                int64(uc.EF),
		flatSearchCutoff:  int64(uc.FlatSearchCutoff),
		nodes:             make([]*vertex, initialSize),
		cache:             vectorCache,
		vectorForID:       vectorCache.get,
		id:                cfg.ID,
		rootPath:          cfg.RootPath,
		tombstones:        map[uint64]struct{}{},
		logger:            cfg.Logger,
		distancerProvider: cfg.DistanceProvider,
		cancel:            make(chan struct{}),
		deleteLock:        &sync.Mutex{},
		tombstoneLock:     &sync.RWMutex{},
		initialInsertOnce: &sync.Once{},
		cleanupInterval:   time.Duration(uc.CleanupIntervalSeconds) * time.Second,
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

func (h *hnsw) findBestEntrypointForNode(currentMaxLevel, targetLevel int,
	entryPointID uint64, nodeVec []float32) (uint64, error) {
	// in case the new target is lower than the current max, we need to search
	// each layer for a better candidate and update the candidate
	for level := currentMaxLevel; level > targetLevel; level-- {
		eps := priorityqueue.NewMin(1)
		dist, ok, err := h.distBetweenNodeAndVec(entryPointID, nodeVec)
		if err != nil {
			return 0, errors.Wrapf(err,
				"calculate distance between insert node and entry point at level %d", level)
		}
		if !ok {
			continue
		}

		eps.Insert(entryPointID, dist)
		res, err := h.searchLayerByVector(nodeVec, eps, 1, level, nil)
		if err != nil {
			return 0,
				errors.Wrapf(err, "update candidate: search layer at level %d", level)
		}
		if res.Len() > 0 {
			// if we could find a new entrypoint, use it
			// in case everything was tombstoned, stick with the existing one
			elem := res.Pop()
			if !h.nodeByID(elem.ID).isUnderMaintenance() {
				// but not if the entrypoint is under maintenance
				entryPointID = elem.ID
			}
		}

		h.pools.pqResults.Put(res)
	}

	return entryPointID, nil
}

type vertex struct {
	id uint64
	sync.Mutex
	level       int
	connections map[int][]uint64 // map[level][]connectedId
	maintenance bool
}

func (v *vertex) markAsMaintenance() {
	v.Lock()
	defer v.Unlock()

	v.maintenance = true
}

func (v *vertex) unmarkAsMaintenance() {
	v.Lock()
	defer v.Unlock()

	v.maintenance = false
}

func (v *vertex) isUnderMaintenance() bool {
	v.Lock()
	defer v.Unlock()

	return v.maintenance
}

func (v *vertex) connectionsAtLevelNoLock(level int) []uint64 {
	return v.connections[level]
}

func (v *vertex) setConnectionsAtLevel(level int, connections []uint64) {
	v.Lock()
	defer v.Unlock()

	v.connections[level] = connections
}

// func (v *vertex) setConnectionsAtLevelNoLock(level int, connections []uint64) {
// 	v.connections[level] = connections
// }

func (v *vertex) appendConnectionAtLevelNoLock(level int, connection uint64) {
	v.connections[level] = append(v.connections[level], connection)
}

func (v *vertex) resetConnectionsAtLevelNoLock(level int) {
	v.connections[level] = v.connections[level][:0]
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

	return h.distancerProvider.SingleDist(vecA, vecB)
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

	return h.distancerProvider.SingleDist(vecA, vecB)
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
	h.Lock()
	defer h.Unlock()

	for _, node := range h.nodes {
		if node != nil {
			return false
		}
	}

	return true
}

func (h *hnsw) nodeByID(id uint64) *vertex {
	h.Lock()
	defer h.Unlock()

	if id >= uint64(len(h.nodes)) {
		// See https://github.com/semi-technologies/weaviate/issues/1838 for details.
		// This could be after a crash recovery when the object store is "further
		// ahead" than the hnsw index and we receive a delete request
		return nil
	}

	return h.nodes[id]
}

func (h *hnsw) Drop() error {
	// cancel commit log goroutine
	err := h.commitLog.Drop()
	if err != nil {
		return errors.Wrap(err, "commit log drop")
	}
	// cancel vector cache goroutine
	h.cache.drop()
	// cancel tombstone cleanup goroutine
	h.cancel <- struct{}{}
	return nil
}

func (h *hnsw) Flush() error {
	return h.commitLog.Flush()
}

func (h *hnsw) Entrypoint() uint64 {
	h.Lock()
	defer h.Unlock()

	return h.entryPointID
}

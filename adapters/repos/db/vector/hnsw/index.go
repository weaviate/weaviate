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
	"io"
	"math"
	"math/rand"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/cache"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/multivector"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/storobj"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

type hnsw struct {
	// global lock to prevent concurrent map read/write, etc.
	sync.RWMutex

	// certain operations related to deleting, such as finding a new entrypoint
	// can only run sequentially, this separate lock helps assuring this without
	// blocking the general usage of the hnsw index
	deleteLock *sync.Mutex

	tombstoneLock *sync.RWMutex

	// prevents tombstones cleanup to be performed in parallel with index reset operation
	resetLock *sync.RWMutex
	// indicates whether reset operation occurred or not - if so tombstones cleanup method
	// is aborted as it makes no sense anymore
	resetCtx       context.Context
	resetCtxCancel context.CancelFunc

	// indicates the index is shutting down
	shutdownCtx       context.Context
	shutdownCtxCancel context.CancelFunc

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
	flatSearchCutoff      int64
	flatSearchConcurrency int

	levelNormalizer float64

	nodes []*vertex

	vectorForID               common.VectorForID[float32]
	TempVectorForIDThunk      common.TempVectorForID[float32]
	TempMultiVectorForIDThunk common.TempVectorForID[[]float32]
	multiVectorForID          common.MultiVectorForID
	trackDimensionsOnce       sync.Once
	trackMuveraOnce           sync.Once
	dims                      int32

	cache               cache.Cache[float32]
	waitForCachePrefill bool

	commitLog CommitLogger

	// a lookup of current tombstones (i.e. nodes that have received a tombstone,
	// but have not been cleaned up yet) Cleanup is the process of removal of all
	// outgoing edges to the tombstone as well as deleting the tombstone itself.
	// This process should happen periodically.
	tombstones map[uint64]struct{}

	tombstoneCleanupCallbackCtrl cyclemanager.CycleCallbackCtrl

	// // for distributed spike, can be used to call a insertExternal on a different graph
	// insertHook func(node, targetLevel int, neighborsAtLevel map[int][]uint32)

	id       string
	rootPath string

	logger                 logrus.FieldLogger
	distancerProvider      distancer.Provider
	multiDistancerProvider distancer.Provider
	pools                  *pools

	forbidFlat bool // mostly used in testing scenarios where we want to use the index even in scenarios where we typically wouldn't

	metrics       *Metrics
	insertMetrics *insertMetrics

	randFunc func() float64 // added to temporarily get rid on flakiness in tombstones related tests. to be removed after fixing WEAVIATE-179

	// The deleteVsInsertLock makes sure that there are no concurrent delete and
	// insert operations happening. It uses an RW-Mutex with:
	//
	// RLock -> Insert operations, this means any number of import operations can
	// happen concurrently.
	//
	// Lock -> Delete operation. This means only a single delete operation can
	// occur at a time, no insert operation can occur simultaneously with a
	// delete. Since the delete is cheap (just marking the node as deleted), the
	// single-threadedness of deletes is not a big problem.
	//
	// This lock was introduced as part of
	// https://github.com/weaviate/weaviate/issues/2194
	//
	// See
	// https://github.com/weaviate/weaviate/pull/2191#issuecomment-1242726787
	// where we ran performance tests to make sure introducing this lock has no
	// negative impact on performance.
	deleteVsInsertLock sync.RWMutex

	compressed       atomic.Bool
	doNotRescore     bool
	acornSearch      atomic.Bool
	acornFilterRatio float64

	disableSnapshots  bool
	snapshotOnStartup bool

	compressor compressionhelpers.VectorCompressor
	pqConfig   ent.PQConfig
	bqConfig   ent.BQConfig
	sqConfig   ent.SQConfig
	// rescoring compressed vectors is disk-bound. On cold starts, we cannot
	// rescore sequentially, as that would take very long. This setting allows us
	// to define the rescoring concurrency.
	rescoreConcurrency int

	compressActionLock    *sync.RWMutex
	className             string
	shardName             string
	VectorForIDThunk      common.VectorForID[float32]
	MultiVectorForIDThunk common.VectorForID[[]float32]
	shardedNodeLocks      *common.ShardedRWLocks
	store                 *lsmkv.Store

	allocChecker            memwatch.AllocChecker
	tombstoneCleanupRunning atomic.Bool

	visitedListPoolMaxSize int

	// only used for multivector mode
	multivector   atomic.Bool
	muvera        atomic.Bool
	muveraEncoder *multivector.MuveraEncoder
	docIDVectors  map[uint64][]uint64
	vecIDcounter  uint64
	maxDocID      uint64
}

type CommitLogger interface {
	ID() string
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
	Drop(ctx context.Context) error
	Flush() error
	Shutdown(ctx context.Context) error
	RootPath() string
	SwitchCommitLogs(bool) error
	AddPQCompression(compressionhelpers.PQData) error
	AddSQCompression(compressionhelpers.SQData) error
	AddMuvera(multivector.MuveraData) error
	InitMaintenance()

	CreateSnapshot() (bool, int64, error)
	CreateAndLoadSnapshot() (*DeserializationResult, int64, error)
	LoadSnapshot() (*DeserializationResult, int64, error)
}

type BufferedLinksLogger interface {
	AddLinkAtLevel(nodeid uint64, level int, target uint64) error
	ReplaceLinksAtLevel(nodeid uint64, level int, targets []uint64) error
	Close() error // Close should Flush and Close
}

type MakeCommitLogger func() (CommitLogger, error)

// New creates a new HNSW index, the commit logger is provided through a thunk
// (a function which can be deferred). This is because creating a commit logger
// opens files for writing. However, checking whether a file is present, is a
// criterium for the index to see if it has to recover from disk or if its a
// truly new index. So instead the index is initialized, with un-biased disk
// checks first and only then is the commit logger created
func New(cfg Config, uc ent.UserConfig,
	tombstoneCallbacks cyclemanager.CycleCallbackGroup, store *lsmkv.Store,
) (*hnsw, error) {
	if err := cfg.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	if cfg.Logger == nil {
		logger := logrus.New()
		logger.Out = io.Discard
		cfg.Logger = logger
	}

	normalizeOnRead := cfg.DistanceProvider.Type() == "cosine-dot"

	var vectorCache cache.Cache[float32]

	var muveraEncoder *multivector.MuveraEncoder
	if uc.Multivector.Enabled && !uc.Multivector.MuveraConfig.Enabled {
		vectorCache = cache.NewShardedMultiFloat32LockCache(cfg.MultiVectorForIDThunk, uc.VectorCacheMaxObjects,
			cfg.Logger, normalizeOnRead, cache.DefaultDeletionInterval, cfg.AllocChecker)
	} else {
		if uc.Multivector.MuveraConfig.Enabled {
			muveraEncoder = multivector.NewMuveraEncoder(uc.Multivector.MuveraConfig, store)
			err := store.CreateOrLoadBucket(context.Background(), cfg.ID+"_muvera_vectors", lsmkv.WithStrategy(lsmkv.StrategyReplace))
			if err != nil {
				return nil, errors.Wrapf(err, "Create or load bucket (muvera store)")
			}
			muveraVectorForID := func(ctx context.Context, id uint64) ([]float32, error) {
				return muveraEncoder.GetMuveraVectorForID(id, cfg.ID+"_muvera_vectors")
			}
			vectorCache = cache.NewShardedFloat32LockCache(
				muveraVectorForID, cfg.MultiVectorForIDThunk, uc.VectorCacheMaxObjects, 1, cfg.Logger,
				normalizeOnRead, cache.DefaultDeletionInterval, cfg.AllocChecker)

		} else {
			vectorCache = cache.NewShardedFloat32LockCache(cfg.VectorForIDThunk, cfg.MultiVectorForIDThunk, uc.VectorCacheMaxObjects, 1, cfg.Logger,
				normalizeOnRead, cache.DefaultDeletionInterval, cfg.AllocChecker)
		}
	}
	resetCtx, resetCtxCancel := context.WithCancel(context.Background())
	shutdownCtx, shutdownCtxCancel := context.WithCancel(context.Background())
	index := &hnsw{
		maximumConnections: uc.MaxConnections,

		// inspired by original paper and other implementations
		maximumConnectionsLayerZero: 2 * uc.MaxConnections,

		// inspired by c++ implementation
		levelNormalizer:       1 / math.Log(float64(uc.MaxConnections)),
		efConstruction:        uc.EFConstruction,
		flatSearchCutoff:      int64(uc.FlatSearchCutoff),
		flatSearchConcurrency: max(cfg.FlatSearchConcurrency, 1),
		acornFilterRatio:      cfg.AcornFilterRatio,
		disableSnapshots:      cfg.DisableSnapshots,
		snapshotOnStartup:     cfg.SnapshotOnStartup,
		nodes:                 make([]*vertex, cache.InitialSize),
		cache:                 vectorCache,
		waitForCachePrefill:   cfg.WaitForCachePrefill,
		vectorForID:           vectorCache.Get,
		multiVectorForID:      vectorCache.MultiGet,
		id:                    cfg.ID,
		rootPath:              cfg.RootPath,
		tombstones:            map[uint64]struct{}{},
		logger:                cfg.Logger,
		distancerProvider:     cfg.DistanceProvider,
		deleteLock:            &sync.Mutex{},
		tombstoneLock:         &sync.RWMutex{},
		resetLock:             &sync.RWMutex{},
		resetCtx:              resetCtx,
		resetCtxCancel:        resetCtxCancel,
		shutdownCtx:           shutdownCtx,
		shutdownCtxCancel:     shutdownCtxCancel,
		initialInsertOnce:     &sync.Once{},

		ef:       int64(uc.EF),
		efMin:    int64(uc.DynamicEFMin),
		efMax:    int64(uc.DynamicEFMax),
		efFactor: int64(uc.DynamicEFFactor),

		metrics:   NewMetrics(cfg.PrometheusMetrics, cfg.ClassName, cfg.ShardName),
		shardName: cfg.ShardName,

		randFunc:                  rand.Float64,
		compressActionLock:        &sync.RWMutex{},
		className:                 cfg.ClassName,
		VectorForIDThunk:          cfg.VectorForIDThunk,
		MultiVectorForIDThunk:     cfg.MultiVectorForIDThunk,
		TempVectorForIDThunk:      cfg.TempVectorForIDThunk,
		TempMultiVectorForIDThunk: cfg.TempMultiVectorForIDThunk,
		pqConfig:                  uc.PQ,
		bqConfig:                  uc.BQ,
		sqConfig:                  uc.SQ,
		rescoreConcurrency:        2 * runtime.GOMAXPROCS(0), // our default for IO-bound activties
		shardedNodeLocks:          common.NewDefaultShardedRWLocks(),

		store:                  store,
		allocChecker:           cfg.AllocChecker,
		visitedListPoolMaxSize: cfg.VisitedListPoolMaxSize,

		docIDVectors:  make(map[uint64][]uint64),
		muveraEncoder: muveraEncoder,
	}
	index.acornSearch.Store(uc.FilterStrategy == ent.FilterStrategyAcorn)

	index.multivector.Store(uc.Multivector.Enabled)
	index.muvera.Store(uc.Multivector.MuveraConfig.Enabled)

	if uc.BQ.Enabled {
		var err error
		if uc.Multivector.Enabled && !uc.Multivector.MuveraConfig.Enabled {
			index.compressor, err = compressionhelpers.NewBQMultiCompressor(
				index.distancerProvider, uc.VectorCacheMaxObjects, cfg.Logger, store,
				cfg.AllocChecker)
		} else {
			index.compressor, err = compressionhelpers.NewBQCompressor(
				index.distancerProvider, uc.VectorCacheMaxObjects, cfg.Logger, store,
				cfg.AllocChecker)
		}
		if err != nil {
			return nil, err
		}
		index.compressed.Store(true)
		index.cache.Drop()
		index.cache = nil
	}

	if uc.Multivector.Enabled {
		index.multiDistancerProvider = distancer.NewDotProductProvider()
		if !uc.Multivector.MuveraConfig.Enabled {
			err := index.store.CreateOrLoadBucket(context.Background(), cfg.ID+"_mv_mappings", lsmkv.WithStrategy(lsmkv.StrategyReplace), lsmkv.WithLazySegmentLoading(cfg.LazyLoadSegments))
			if err != nil {
				return nil, errors.Wrapf(err, "Create or load bucket (multivector store)")
			}
		}
	}

	if err := index.init(cfg); err != nil {
		return nil, errors.Wrapf(err, "init index %q", index.id)
	}

	// TODO common_cycle_manager move to poststartup?
	id := strings.Join([]string{
		"hnsw", "tombstone_cleanup",
		index.className, index.shardName, index.id,
	}, "/")
	index.tombstoneCleanupCallbackCtrl = tombstoneCallbacks.Register(id, index.tombstoneCleanup)
	index.insertMetrics = newInsertMetrics(index.metrics)

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

func (h *hnsw) findBestEntrypointForNode(ctx context.Context, currentMaxLevel, targetLevel int,
	entryPointID uint64, nodeVec []float32, distancer compressionhelpers.CompressorDistancer,
) (uint64, error) {
	// in case the new target is lower than the current max, we need to search
	// each layer for a better candidate and update the candidate
	for level := currentMaxLevel; level > targetLevel; level-- {
		eps := priorityqueue.NewMin[any](1)
		var dist float32
		var err error
		if h.compressed.Load() {
			dist, err = distancer.DistanceToNode(entryPointID)
		} else {
			dist, err = h.distToNode(distancer, entryPointID, nodeVec)
		}

		var e storobj.ErrNotFound
		if errors.As(err, &e) {
			h.handleDeletedNode(e.DocID, "findBestEntrypointForNode")
			continue
		}
		if err != nil {
			return 0, errors.Wrapf(err,
				"calculate distance between insert node and entry point at level %d", level)
		}

		eps.Insert(entryPointID, dist)
		res, err := h.searchLayerByVectorWithDistancer(ctx, nodeVec, eps, 1, level, nil, distancer)
		if err != nil {
			return 0,
				errors.Wrapf(err, "update candidate: search layer at level %d", level)
		}
		if res.Len() > 0 {
			// if we could find a new entrypoint, use it
			// in case everything was tombstoned, stick with the existing one
			elem := res.Pop()
			n := h.nodeByID(elem.ID)
			if n != nil && !n.isUnderMaintenance() {
				// but not if the entrypoint is under maintenance
				entryPointID = elem.ID
			}
		}

		h.pools.pqResults.Put(res)
	}

	return entryPointID, nil
}

func (h *hnsw) distBetweenNodes(a, b uint64) (float32, error) {
	if h.compressed.Load() {
		dist, err := h.compressor.DistanceBetweenCompressedVectorsFromIDs(context.Background(), a, b)
		if err != nil {
			return 0, err
		}

		return dist, nil
	}

	// TODO: introduce single search/transaction context instead of spawning new
	// ones
	vecA, errA := h.vectorForID(context.Background(), a)

	if errA != nil {
		var e storobj.ErrNotFound
		if errors.As(errA, &e) {
			h.handleDeletedNode(e.DocID, "distBetweenNodes")
			return 0, nil
		}
		// not a typed error, we can recover from, return with err
		return 0, errors.Wrapf(errA,
			"could not get vector of object at docID %d", a)
	}

	if len(vecA) == 0 {
		return 0, fmt.Errorf("got a nil or zero-length vector at docID %d", a)
	}

	vecB, errB := h.vectorForID(context.Background(), b)

	if errB != nil {
		var e storobj.ErrNotFound
		if errors.As(errB, &e) {
			h.handleDeletedNode(e.DocID, "distBetweenNodes")
			return 0, nil
		}
		// not a typed error, we can recover from, return with err
		return 0, errors.Wrapf(errB,
			"could not get vector of object at docID %d", b)
	}

	if len(vecB) == 0 {
		return 0, fmt.Errorf("got a nil or zero-length vector at docID %d", b)
	}

	return h.distancerProvider.SingleDist(vecA, vecB)
}

func (h *hnsw) distToNode(distancer compressionhelpers.CompressorDistancer, node uint64, vecB []float32) (float32, error) {
	if h.compressed.Load() {
		dist, err := distancer.DistanceToNode(node)
		if err != nil {
			return 0, err
		}

		return dist, nil
	}

	// TODO: introduce single search/transaction context instead of spawning new
	// ones
	var vecA []float32
	var err error
	vecA, err = h.vectorForID(context.Background(), node)
	if err != nil {
		var e storobj.ErrNotFound
		if errors.As(err, &e) {
			h.handleDeletedNode(e.DocID, "distBetweenNodeAndVec")
			return 0, nil
		}
		// not a typed error, we can recover from, return with err
		return 0, errors.Wrapf(err,
			"could not get vector of object at docID %d", node)
	}

	if len(vecA) == 0 {
		return 0, fmt.Errorf(
			"got a nil or zero-length vector at docID %d", node)
	}

	if len(vecB) == 0 {
		return 0, fmt.Errorf(
			"got a nil or zero-length vector as search vector")
	}

	return h.distancerProvider.SingleDist(vecA, vecB)
}

func (h *hnsw) isEmpty() bool {
	h.RLock()
	defer h.RUnlock()
	h.shardedNodeLocks.RLock(h.entryPointID)
	defer h.shardedNodeLocks.RUnlock(h.entryPointID)

	return h.isEmptyUnlocked()
}

func (h *hnsw) isEmptyUnlocked() bool {
	return h.entryPointID > uint64(len(h.nodes)) || h.nodes[h.entryPointID] == nil
}

func (h *hnsw) nodeByID(id uint64) *vertex {
	h.RLock()
	defer h.RUnlock()

	if id >= uint64(len(h.nodes)) {
		// See https://github.com/weaviate/weaviate/issues/1838 for details.
		// This could be after a crash recovery when the object store is "further
		// ahead" than the hnsw index and we receive a delete request
		return nil
	}

	h.shardedNodeLocks.RLock(id)
	defer h.shardedNodeLocks.RUnlock(id)

	return h.nodes[id]
}

func (h *hnsw) Drop(ctx context.Context) error {
	// cancel tombstone cleanup goroutine
	if err := h.tombstoneCleanupCallbackCtrl.Unregister(ctx); err != nil {
		return errors.Wrap(err, "hnsw drop")
	}

	if h.compressed.Load() {
		err := h.compressor.Drop()
		if err != nil {
			return fmt.Errorf("failed to shutdown compressed store")
		}
	} else {
		// cancel vector cache goroutine
		h.cache.Drop()
	}

	// cancel commit logger last, as the tombstone cleanup cycle might still
	// write while it's still running
	err := h.commitLog.Drop(ctx)
	if err != nil {
		return errors.Wrap(err, "commit log drop")
	}

	return nil
}

func (h *hnsw) Shutdown(ctx context.Context) error {
	h.shutdownCtxCancel()

	if err := h.commitLog.Shutdown(ctx); err != nil {
		return errors.Wrap(err, "hnsw shutdown")
	}

	if err := h.tombstoneCleanupCallbackCtrl.Unregister(ctx); err != nil {
		return errors.Wrap(err, "hnsw shutdown")
	}

	if h.compressed.Load() {
		err := h.compressor.Drop()
		if err != nil {
			return errors.Wrap(err, "hnsw shutdown")
		}
	} else {
		h.cache.Drop()
	}

	return nil
}

func (h *hnsw) Flush() error {
	return h.commitLog.Flush()
}

func (h *hnsw) Entrypoint() uint64 {
	h.RLock()
	defer h.RUnlock()

	return h.entryPointID
}

func (h *hnsw) DistanceBetweenVectors(x, y []float32) (float32, error) {
	return h.distancerProvider.SingleDist(x, y)
}

func (h *hnsw) ContainsDoc(docID uint64) bool {
	if h.Multivector() && !h.muvera.Load() {
		h.RLock()
		vecIds, exists := h.docIDVectors[docID]
		h.RUnlock()
		return exists && !h.hasTombstones(vecIds)
	}

	h.RLock()
	h.shardedNodeLocks.RLock(docID)
	exists := len(h.nodes) > int(docID) && h.nodes[docID] != nil
	h.shardedNodeLocks.RUnlock(docID)
	h.RUnlock()

	return exists && !h.hasTombstone(docID)
}

func (h *hnsw) Iterate(fn func(docID uint64) bool) {
	if h.Multivector() && !h.muvera.Load() {
		h.iterateMulti(fn)
		return
	}
	h.iterate(fn)
}

func (h *hnsw) iterate(fn func(docID uint64) bool) {
	var id uint64

	for {
		if h.shutdownCtx.Err() != nil {
			return
		}
		if h.resetCtx.Err() != nil {
			return
		}

		h.RLock()
		h.shardedNodeLocks.RLock(id)
		stop := int(id) >= len(h.nodes)
		exists := !stop && h.nodes[id] != nil
		h.shardedNodeLocks.RUnlock(id)
		h.RUnlock()

		if stop {
			return
		}

		if exists && !h.hasTombstone(id) {
			if !fn(id) {
				return
			}
		}

		id++
	}
}

func (h *hnsw) iterateMulti(fn func(docID uint64) bool) {
	h.RLock()
	indexedDocIDs := make([]uint64, 0, len(h.docIDVectors))
	for docID := range h.docIDVectors {
		indexedDocIDs = append(indexedDocIDs, docID)
	}
	h.RUnlock()

	for _, docID := range indexedDocIDs {
		if h.shutdownCtx.Err() != nil || h.resetCtx.Err() != nil {
			return
		}

		h.RLock()
		nodes, ok := h.docIDVectors[docID]
		h.RUnlock()

		if ok && !h.hasTombstones(nodes) {
			if !fn(docID) {
				return
			}
		}
	}
}

func (h *hnsw) DistancerProvider() distancer.Provider {
	return h.distancerProvider
}

func (h *hnsw) ShouldUpgrade() (bool, int) {
	if h.sqConfig.Enabled {
		return h.sqConfig.Enabled, h.sqConfig.TrainingLimit
	}
	return h.pqConfig.Enabled, h.pqConfig.TrainingLimit
}

func (h *hnsw) ShouldCompressFromConfig(config config.VectorIndexConfig) (bool, int) {
	hnswConfig := config.(ent.UserConfig)
	if hnswConfig.SQ.Enabled {
		return hnswConfig.SQ.Enabled, hnswConfig.SQ.TrainingLimit
	}
	return hnswConfig.PQ.Enabled, hnswConfig.PQ.TrainingLimit
}

func (h *hnsw) Compressed() bool {
	return h.compressed.Load()
}

func (h *hnsw) Multivector() bool {
	return h.multivector.Load()
}

func (h *hnsw) Upgraded() bool {
	return h.Compressed()
}

func (h *hnsw) AlreadyIndexed() uint64 {
	return uint64(h.cache.CountVectors())
}

func (h *hnsw) GetKeys(id uint64) (uint64, uint64, error) {
	docID, relativeID := h.cache.GetKeys(id)
	return docID, relativeID, nil
}

func (h *hnsw) normalizeVec(vec []float32) []float32 {
	if h.distancerProvider.Type() == "cosine-dot" {
		// cosine-dot requires normalized vectors, as the dot product and cosine
		// similarity are only identical if the vector is normalized
		return distancer.Normalize(vec)
	}
	return vec
}

func (h *hnsw) normalizeVecs(vecs [][]float32) [][]float32 {
	if h.distancerProvider.Type() == "cosine-dot" {
		normalized := make([][]float32, len(vecs))
		for i, vec := range vecs {
			normalized[i] = distancer.Normalize(vec)
		}
		return normalized
	}
	return vecs
}

func IsHNSWIndex(index any) bool {
	_, ok := index.(*hnsw)
	return ok
}

func AsHNSWIndex(index any) Index {
	h, _ := index.(*hnsw)
	return h
}

// This interface exposes public methods of the HNSW index
// that are not part of the VectorIndex interface.
// It is a workaround to avoid circular dependencies.
type Index interface {
	CleanUpTombstonedNodes(shouldAbort cyclemanager.ShouldAbortCallback) error
}

type nodeLevel struct {
	nodeId uint64
	level  int
}

func (h *hnsw) calculateUnreachablePoints() []uint64 {
	h.RLock()
	defer h.RUnlock()

	visitedPairs := make(map[nodeLevel]bool)
	candidateList := []nodeLevel{{h.entryPointID, h.currentMaximumLayer}}

	for len(candidateList) > 0 {
		currentNode := candidateList[len(candidateList)-1]
		candidateList = candidateList[:len(candidateList)-1]
		if !visitedPairs[currentNode] {
			visitedPairs[currentNode] = true
			h.shardedNodeLocks.RLock(currentNode.nodeId)
			node := h.nodes[currentNode.nodeId]
			if node != nil {
				node.Lock()
				neighbors := node.connectionsAtLowerLevelsNoLock(currentNode.level, visitedPairs)
				node.Unlock()
				candidateList = append(candidateList, neighbors...)
			}
			h.shardedNodeLocks.RUnlock(currentNode.nodeId)
		}
	}

	visitedNodes := make(map[uint64]bool, len(visitedPairs))
	for k, v := range visitedPairs {
		if v {
			visitedNodes[k.nodeId] = true
		}
	}

	unvisitedNodes := []uint64{}
	for i := 0; i < len(h.nodes); i++ {
		var id uint64
		h.shardedNodeLocks.RLock(uint64(i))
		if h.nodes[i] != nil {
			id = h.nodes[i].id
		}
		h.shardedNodeLocks.RUnlock(uint64(i))
		if id == 0 {
			continue
		}
		if !visitedNodes[uint64(i)] {
			unvisitedNodes = append(unvisitedNodes, id)
		}

	}
	return unvisitedNodes
}

type HnswStats struct {
	Dimensions         int32                               `json:"dimensions"`
	EntryPointID       uint64                              `json:"entryPointID"`
	DistributionLayers map[int]uint                        `json:"distributionLayers"`
	UnreachablePoints  []uint64                            `json:"unreachablePoints"`
	NumTombstones      int                                 `json:"numTombstones"`
	CacheSize          int32                               `json:"cacheSize"`
	Compressed         bool                                `json:"compressed"`
	CompressorStats    compressionhelpers.CompressionStats `json:"compressionStats"`
	CompressionType    string                              `json:"compressionType"`
}

func (s *HnswStats) IndexType() common.IndexType {
	return common.IndexTypeHNSW
}

func (h *hnsw) Stats() (common.IndexStats, error) {
	h.RLock()
	defer h.RUnlock()
	distributionLayers := map[int]uint{}

	for _, node := range h.nodes {
		func() {
			if node == nil {
				return
			}
			node.Lock()
			defer node.Unlock()
			l := node.level
			if l == 0 && len(node.connections) == 0 {
				return
			}
			c, ok := distributionLayers[l]
			if !ok {
				distributionLayers[l] = 0
			}

			distributionLayers[l] = c + 1
		}()
	}

	stats := HnswStats{
		Dimensions:         h.dims,
		EntryPointID:       h.entryPointID,
		DistributionLayers: distributionLayers,
		UnreachablePoints:  h.calculateUnreachablePoints(),
		NumTombstones:      len(h.tombstones),
		CacheSize:          h.cache.Len(),
		Compressed:         h.compressed.Load(),
	}

	if stats.Compressed {
		stats.CompressorStats = h.compressor.Stats()
	} else {
		stats.CompressorStats = compressionhelpers.UncompressedStats{}
	}

	stats.CompressionType = stats.CompressorStats.CompressionType()

	return &stats, nil
}

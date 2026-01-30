//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hfresh

import (
	"context"
	stderrors "errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/visited"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hfresh"
)

const (
	DefaultRNGFactor = 10.0
)

var (
	ErrPostingNotFound = errors.New("posting not found")
	ErrVectorNotFound  = errors.New("vector not found")
)

var _ common.VectorIndex = (*HFresh)(nil)

// HFresh is an implementation of a vector index using the SPFresh algorithm.
// It spawns background workers to handle split, merge, and reassign operations,
// while exposing a synchronous API for searching and updating vectors.
// Note: this is a work in progress and not all features are implemented yet.
type HFresh struct {
	id               string
	logger           logrus.FieldLogger
	config           *Config // Config contains internal configuration settings.
	metrics          *Metrics
	scheduler        *queue.Scheduler
	maxPostingSizeKB uint32 // User configurable i/o budget
	maxPostingSize   uint32
	minPostingSize   uint32
	replicas         uint32
	rngFactor        float32
	searchProbe      uint32
	rescoreLimit     uint32
	store            *lsmkv.Store

	// some components require knowing the vector size beforehand
	// and can only be initialized once the first vector has been
	// received
	initDimensionsOnce sync.Once
	dims               uint32 // Number of dimensions of expected vectors
	distancer          *Distancer
	quantizer          *compressionhelpers.BinaryRotationalQuantizer

	// Internal components
	Centroids     *HNSWIndex          // Provides access to the centroids.
	PostingStore  *PostingStore       // Used for managing persistence of postings.
	IDs           *common.Sequence    // Shared monotonic counter for generating unique IDs for new postings.
	VersionMap    *VersionMap         // Stores vector versions in-memory.
	PostingMap    *PostingMap         // Maps postings to vector IDs.
	IndexMetadata *IndexMetadataStore // Stores metadata about the index.

	// ctx and cancel are used to manage the lifecycle of the background operations.
	ctx    context.Context
	cancel context.CancelFunc

	taskQueue TaskQueue

	visitedPool *visited.Pool

	postingLocks       *common.ShardedRWLocks // Locks to prevent concurrent modifications to the same posting.
	initialPostingLock sync.Mutex

	vectorForId common.VectorForID[float32]

	rootPath string
}

func New(cfg *Config, uc ent.UserConfig, store *lsmkv.Store) (*HFresh, error) {
	err := cfg.Validate()
	if err != nil {
		return nil, err
	}

	metrics := NewMetrics(cfg.PrometheusMetrics, cfg.ClassName, cfg.ShardName)

	// initialize shared bucket used for storing various metadata
	bucket, err := NewSharedBucket(store, cfg.ID, cfg.Store)
	if err != nil {
		return nil, err
	}

	// initialize posting store used for storing actual postings and their vectors
	postingStore, err := NewPostingStore(store, bucket, metrics, cfg.ID, cfg.Store)
	if err != nil {
		return nil, err
	}

	h := HFresh{
		id:            cfg.ID,
		logger:        cfg.Logger.WithField("component", "HFresh"),
		config:        cfg,
		scheduler:     cfg.Scheduler,
		store:         store,
		metrics:       metrics,
		PostingStore:  postingStore,
		vectorForId:   cfg.VectorForIDThunk,
		VersionMap:    NewVersionMap(bucket),
		PostingMap:    NewPostingMap(bucket, metrics),
		IndexMetadata: NewIndexMetadataStore(bucket),
		postingLocks:  common.NewDefaultShardedRWLocks(),
		// TODO: choose a better starting size since we can predict the max number of
		// visited vectors based on cfg.InternalPostingCandidates.
		visitedPool:      visited.NewPool(1, 512, -1),
		maxPostingSizeKB: uc.MaxPostingSizeKB,
		replicas:         uc.Replicas,
		rngFactor:        DefaultRNGFactor,
		searchProbe:      uc.SearchProbe,
		rescoreLimit:     uint32(uc.RQ.RescoreLimit),
		rootPath:         cfg.RootPath,
	}

	h.Centroids, err = NewHNSWIndex(metrics, store, cfg, 1024*1024, 1024)
	if err != nil {
		return nil, err
	}
	h.IDs, err = common.NewSequence(NewBucketStore(bucket), 1000)
	if err != nil {
		return nil, err
	}

	h.ctx, h.cancel = context.WithCancel(context.Background())

	taskQueue, err := NewTaskQueue(&h, bucket)
	if err != nil {
		return nil, err
	}
	h.taskQueue = *taskQueue

	if err = h.restoreMetadata(); err != nil {
		h.logger.Warnf("unable to restore metadata from previous run with error: %v", err)
	}

	return &h, nil
}

// Delete marks a vector as deleted in the version map.
func (h *HFresh) Delete(ids ...uint64) error {
	for _, id := range ids {
		start := time.Now()
		version, err := h.VersionMap.MarkDeleted(context.Background(), id)
		if err != nil {
			return errors.Wrapf(err, "failed to mark vector %d as deleted", id)
		}
		if version == 0 {
			return ErrVectorNotFound
		}
		h.metrics.DeleteVector(start)
	}

	return nil
}

func (h *HFresh) Type() common.IndexType {
	return common.IndexTypeHFresh
}

func (h *HFresh) UpdateUserConfig(updated schemaConfig.VectorIndexConfig, callback func()) error {
	parsed, ok := updated.(ent.UserConfig)
	if !ok {
		callback()
		return errors.Errorf("config is not UserConfig, but %T", updated)
	}

	atomic.StoreUint32(&h.searchProbe, parsed.SearchProbe)
	atomic.StoreUint32(&h.rescoreLimit, uint32(parsed.RQ.RescoreLimit))

	callback()
	return nil
}

func (h *HFresh) Drop(ctx context.Context, keepFiles bool) error {
	_ = h.Shutdown(ctx)
	// Shard::drop will take care of handling store buckets
	return nil
}

func (h *HFresh) Shutdown(ctx context.Context) error {
	if h.ctx == nil {
		return nil // Already closed or not started
	}

	if h.ctx.Err() != nil {
		return h.ctx.Err() // Context already cancelled
	}

	// Cancel the context to prevent new operations from being enqueued
	h.cancel()

	var errs []error

	err := h.taskQueue.Close()
	if err != nil {
		errs = append(errs, err)
	}

	err = h.Flush()
	if err != nil {
		errs = append(errs, err)
	}

	err = h.IDs.Flush()
	if err != nil {
		errs = append(errs, err)
	}

	err = h.Centroids.hnsw.Shutdown(ctx)
	if err != nil {
		errs = append(errs, err)
	}

	return stderrors.Join(errs...)
}

func (h *HFresh) Flush() error {
	var errs []error

	// flush the HNSW commit log
	err := h.Centroids.hnsw.Flush()
	if err != nil {
		errs = append(errs, err)
	}

	// flush the task queues
	err = h.taskQueue.Flush()
	if err != nil {
		errs = append(errs, err)
	}

	return stderrors.Join(errs...)
}

func (h *HFresh) stopTaskQueues() error {
	for _, queue := range []*queue.DiskQueue{
		h.taskQueue.analyzeQueue,
		h.taskQueue.splitQueue,
		h.taskQueue.reassignQueue,
		h.taskQueue.mergeQueue,
	} {
		queue.Pause()
		queue.Wait()
		err := queue.Flush()
		if err != nil {
			return err
		}
	}
	return nil
}

func (h *HFresh) resumeHFreshTaskQueues() {
	for _, queue := range []*queue.DiskQueue{
		h.taskQueue.analyzeQueue,
		h.taskQueue.splitQueue,
		h.taskQueue.reassignQueue,
		h.taskQueue.mergeQueue,
	} {
		queue.Resume()
	}
}

func (h *HFresh) PrepareForBackup(ctx context.Context) error {
	err := h.Centroids.hnsw.PrepareForBackup(ctx)
	if err != nil {
		return err
	}
	return h.stopTaskQueues()
}

func (h *HFresh) ListFiles(ctx context.Context, basePath string) ([]string, error) {
	hnswFiles, err := h.Centroids.hnsw.ListFiles(ctx, basePath)
	if err != nil {
		return nil, err
	}

	queueFiles, err := h.ListQueues(ctx, basePath)
	if err != nil {
		return nil, err
	}

	// combine both slices
	var allFiles []string
	allFiles = append(allFiles, hnswFiles...)
	allFiles = append(allFiles, queueFiles...)

	return allFiles, nil
}

func (h *HFresh) ListQueues(ctx context.Context, basePath string) ([]string, error) {
	files := make([]string, 0)
	// list all files in paths that end with .queue.d
	err := filepath.WalkDir(basePath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && strings.HasSuffix(d.Name(), ".queue.d") {
			// list all files in this directory
			err := filepath.WalkDir(path, func(p string, de fs.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if !de.IsDir() {
					relPath, err := filepath.Rel(basePath, p)
					if err != nil {
						return err
					}
					files = append(files, relPath)
				}
				return nil
			})
			if err != nil {
				return err
			}
			// skip walking into subdirectories of this queue directory
			return filepath.SkipDir
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list queue files: %w", err)
	}
	return files, nil
}

func (h *HFresh) PostStartup(ctx context.Context) {
	h.Centroids.hnsw.PostStartup(ctx)
}

func (h *HFresh) Compressed() bool {
	return true
}

func (h *HFresh) Multivector() bool {
	return false
}

func (h *HFresh) ContainsDoc(id uint64) bool {
	v, err := h.VersionMap.Get(context.Background(), id)
	if err != nil {
		h.logger.WithField("vectorID", id).
			Debug("vector version get failed, returning false")
		return false
	}
	return !v.Deleted() && v.Version() > 0
}

func (h *HFresh) Iterate(fn func(id uint64) bool) {
	h.logger.Warn("Iterate is not implemented for HFresh index")
}

func (h *HFresh) QueryVectorDistancer(queryVector []float32) common.QueryVectorDistancer {
	distFunc := func(id uint64) (float32, error) {
		vector, err := h.vectorForId(h.ctx, id)
		if err != nil {
			return 0, err
		}
		dist, err := h.config.DistanceProvider.SingleDist(queryVector, vector)
		if err != nil {
			return 0, err
		}
		return dist, nil
	}

	return common.QueryVectorDistancer{DistanceFunc: distFunc}
}

func (h *HFresh) CompressionStats() compressionhelpers.CompressionStats {
	if h.quantizer != nil {
		return h.quantizer.Stats()
	}
	return compressionhelpers.UncompressedStats{}
}

func (h *HFresh) Preload(id uint64, vector []float32) {
	// for now, nothing to do here
}

// deduplicator is a simple thread-safe structure to prevent duplicate values.
type deduplicator struct {
	m *xsync.Map[uint64, struct{}]
}

func newDeduplicator() *deduplicator {
	return &deduplicator{
		m: xsync.NewMap[uint64, struct{}](),
	}
}

// tryAdd attempts to add an ID to the deduplicator.
// Returns true if the ID was added, false if it already exists.
func (d *deduplicator) tryAdd(id uint64) bool {
	_, loaded := d.m.LoadOrStore(id, struct{}{})
	return !loaded
}

// done marks an ID as processed, removing it from the deduplicator.
func (d *deduplicator) done(id uint64) {
	d.m.Delete(id)
}

// contains checks if an ID is already in the deduplicator.
func (d *deduplicator) contains(id uint64) bool {
	_, exists := d.m.Load(id)
	return exists
}

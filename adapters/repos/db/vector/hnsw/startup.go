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

package hnsw

import (
	"context"
	"encoding/binary"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/visited"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/entities/vectorindex/compression"
)

func (h *hnsw) init(cfg Config) error {
	h.pools = newPools(h.maximumConnectionsLayerZero, h.visitedListPoolMaxSize)

	// init commit logger for future writes
	cl, err := cfg.MakeCommitLoggerThunk()
	if err != nil {
		return errors.Wrap(err, "create commit logger")
	}

	if err := h.restoreFromDisk(cl); err != nil {
		return errors.Wrapf(err, "restore hnsw index %q", cfg.ID)
	}
	h.commitLog = cl

	// report the vector_index_size at server startup.
	// otherwise on server restart, prometheus reports
	// a vector_index_size of 0 until more vectors are
	// added.
	h.metrics.SetSize(len(h.nodes))

	return nil
}

// if a commit log is already present it will be read into memory, if not we
// start with an empty model
func (h *hnsw) restoreFromDisk(cl CommitLogger) error {
	beforeAll := time.Now()
	defer h.metrics.TrackStartupTotal(beforeAll)
	defer func() {
		h.logger.WithField("action", "restore_from_disk").
			WithField("duration", time.Since(beforeAll).String()).
			Info("restored data from disk")
	}()

	var state *DeserializationResult
	var stateTimestamp int64
	var err error

	if !h.disableSnapshots {
		if h.snapshotOnStartup {
			// This will opportunistically create a snapshot if it does not exist yet,
			// as we are loading state from disk. Otherwise, it simply loads
			// the last snapshot.
			state, stateTimestamp, err = cl.CreateAndLoadSnapshot()
		} else {
			state, stateTimestamp, err = cl.LoadSnapshot()
		}

		if err != nil {
			// errors reading snapshots are not fatal
			// we can still read the commit log from the beginning
			h.logger.
				WithError(err).
				WithField("action", "restore_from_disk").
				Error("failed to read last snapshot, loading from commit log")

			state = nil
			stateTimestamp = 0
		} else if state == nil {
			h.logger.
				WithField("action", "restore_from_disk").
				Info("no snapshot found, loading from commit log")
		}
	} else {
		h.logger.
			WithField("action", "restore_from_disk").
			Info("snapshots disabled, loading from commit log")
	}

	fileNames, err := getCommitFileNames(h.rootPath, h.id, stateTimestamp, h.fs)
	if err != nil {
		return err
	}

	state, err = loadCommitLoggerState(h.fs, h.logger, fileNames, state, h.metrics)
	if err != nil {
		return errors.Wrap(err, "load commit logger state")
	}

	if state == nil {
		// Mark the cache as prefilled for fresh indexes so that compression
		// (e.g. RQ via checkAndCompress) can proceed immediately.
		h.cachePrefilled.Store(true)
		return nil
	}

	h.Lock()
	h.shardedNodeLocks.LockAll()
	h.nodes = state.Nodes
	h.shardedNodeLocks.UnlockAll()

	h.currentMaximumLayer = int(state.Level)
	h.entryPointID = state.Entrypoint
	h.Unlock()

	h.tombstoneLock.Lock()
	h.tombstones = state.Tombstones
	h.tombstoneLock.Unlock()

	if h.multivector.Load() {
		if !h.muvera.Load() {
			if err := h.restoreDocMappings(); err != nil {
				return errors.Wrapf(err, "restore doc mappings %q", h.id)
			}
		} else if state.MuveraEnabled {
			h.trackMuveraOnce.Do(func() {
				h.muveraEncoder.LoadMuveraConfig(*state.EncoderMuvera)
			})
			h.muvera.Store(true)
		}
	}
	if state.Compressed {
		h.compressed.Store(state.Compressed)
		h.cache.Drop()
		if state.CompressionPQData != nil {
			data := state.CompressionPQData
			h.dims.Store(int32(data.Dimensions))

			if len(data.Encoders) > 0 {
				// 0 means it was created using the default value. The user did not set the value, we calculated for him/her
				if h.pqConfig.Segments == 0 {
					h.pqConfig.Segments = int(data.Dimensions)
				}
				if !h.multivector.Load() || h.muvera.Load() {
					h.compressor, err = compressionhelpers.RestoreHNSWPQCompressor(
						h.pqConfig,
						h.distancerProvider,
						int(data.Dimensions),
						// ToDo: we need to read this value from somewhere
						1e12,
						h.logger,
						data.Encoders,
						h.store,
						h.makeBucketOptions,
						h.allocChecker,
						h.getTargetVector(),
						h.vectorForID,
					)
				} else {
					h.compressor, err = compressionhelpers.RestoreHNSWPQMultiCompressor(
						h.pqConfig,
						h.distancerProvider,
						int(data.Dimensions),
						1e12,
						h.logger,
						data.Encoders,
						h.store,
						h.makeBucketOptions,
						h.allocChecker,
						h.getTargetVector(),
						h.multiVectorForNodeID,
					)
				}
				if err != nil {
					return errors.Wrap(err, "Restoring compressed data.")
				}
			}
		} else if state.CompressionSQData != nil {
			data := state.CompressionSQData
			h.dims.Store(int32(data.Dimensions))
			if !h.multivector.Load() || h.muvera.Load() {
				h.compressor, err = compressionhelpers.RestoreHNSWSQCompressor(
					h.distancerProvider,
					1e12,
					h.logger,
					data.A,
					data.B,
					data.Dimensions,
					h.store,
					h.makeBucketOptions,
					h.allocChecker,
					h.getTargetVector(),
					h.vectorForID,
				)
			} else {
				h.compressor, err = compressionhelpers.RestoreHNSWSQMultiCompressor(
					h.distancerProvider,
					1e12,
					h.logger,
					data.A,
					data.B,
					data.Dimensions,
					h.store,
					h.makeBucketOptions,
					h.allocChecker,
					h.getTargetVector(),
					h.multiVectorForNodeID,
				)
			}
			if err != nil {
				return errors.Wrap(err, "Restoring compressed data.")
			}
		} else if state.CompressionRQData != nil {
			if err := h.restoreRotationalQuantization(state.CompressionRQData); err != nil {
				return errors.Wrap(err, "Restoring compressed data.")
			}
		} else if state.CompressionBRQData != nil {
			if err := h.restoreBinaryRotationalQuantization(state.CompressionBRQData); err != nil {
				return errors.Wrap(err, "Restoring compressed data.")
			}
		} else {
			return errors.New("unsupported type while loading compression data")
		}
		// make sure the compressed cache fits the current size
		h.compressor.GrowCache(uint64(len(h.nodes)))
	} else if !h.compressed.Load() {
		// make sure the cache fits the current size
		h.cache.Grow(uint64(len(h.nodes)))

		if h.multivector.Load() && !h.muvera.Load() {
			h.populateKeys()
		}
	} else {
		h.compressor.GrowCache(uint64(len(h.nodes)))
	}

	if h.dims.Load() == 0 {
		h.setDimensionsFromEntrypoint()
	}

	if h.compressed.Load() && h.multivector.Load() && !h.muvera.Load() {
		h.compressor.GrowCache(uint64(len(h.nodes)))
		h.populateKeys()
	}

	h.resetTombstoneMetric()

	// make sure the visited list pool fits the current size
	h.pools.visitedLists = visited.NewPool(len(h.nodes) + 512)

	return nil
}

func (h *hnsw) setDimensionsFromEntrypoint() {
	if len(h.nodes) > 0 {
		if vec, err := h.VectorForIDThunk(context.Background(), h.entryPointID); err == nil {
			h.dims.Store(int32(len(vec)))
		}
	}
}

func (h *hnsw) restoreRotationalQuantization(data *compression.RQData) error {
	h.dims.Store(int32(data.InputDim))
	var err error
	if !h.multivector.Load() || h.muvera.Load() {
		h.trackRQOnce.Do(func() {
			h.compressor, err = compressionhelpers.RestoreRQCompressor(
				h.distancerProvider,
				1e12,
				h.logger,
				int(data.InputDim),
				int(data.Bits),
				int(data.Rotation.OutputDim),
				int(data.Rotation.Rounds),
				data.Rotation.Swaps,
				data.Rotation.Signs,
				nil,
				h.store,
				h.allocChecker,
				h.makeBucketOptions,
				h.getTargetVector(),
				h.vectorForID,
			)
		})
	} else {
		h.trackRQOnce.Do(func() {
			h.compressor, err = compressionhelpers.RestoreRQMultiCompressor(
				h.distancerProvider,
				1e12,
				h.logger,
				int(data.InputDim),
				int(data.Bits),
				int(data.Rotation.OutputDim),
				int(data.Rotation.Rounds),
				data.Rotation.Swaps,
				data.Rotation.Signs,
				nil,
				h.store,
				h.allocChecker,
				h.makeBucketOptions,
				h.getTargetVector(),
				h.multiVectorForNodeID,
			)
		})
	}

	return err
}

func (h *hnsw) restoreBinaryRotationalQuantization(data *compression.BRQData) error {
	var err error
	if !h.multivector.Load() || h.muvera.Load() {
		h.trackRQOnce.Do(func() {
			h.compressor, err = compressionhelpers.RestoreRQCompressor(
				h.distancerProvider,
				1e12,
				h.logger,
				int(data.InputDim),
				1,
				int(data.Rotation.OutputDim),
				int(data.Rotation.Rounds),
				data.Rotation.Swaps,
				data.Rotation.Signs,
				data.Rounding,
				h.store,
				h.allocChecker,
				h.makeBucketOptions,
				h.getTargetVector(),
				h.vectorForID,
			)
		})
	} else {
		h.trackRQOnce.Do(func() {
			h.compressor, err = compressionhelpers.RestoreRQMultiCompressor(
				h.distancerProvider,
				1e12,
				h.logger,
				int(data.InputDim),
				1,
				int(data.Rotation.OutputDim),
				int(data.Rotation.Rounds),
				data.Rotation.Swaps,
				data.Rotation.Signs,
				data.Rounding,
				h.store,
				h.allocChecker,
				h.makeBucketOptions,
				h.getTargetVector(),
				h.multiVectorForNodeID,
			)
		})
	}
	return err
}

func (h *hnsw) restoreDocMappings() error {
	prevDocID := uint64(0)
	relativeID := uint64(0)
	maxNodeID := uint64(0)
	maxDocID := uint64(0)
	buf := make([]byte, 8)

	// Get the mappings bucket - handle case where it might be nil
	bucket := h.store.Bucket(h.id + "_mv_mappings")
	if bucket == nil {
		err := errors.New("multivector mappings bucket not found")
		h.logger.WithField("action", "restore_doc_mappings").
			WithError(err)
		return err
	}

	for _, node := range h.nodes {
		if node == nil {
			continue
		}
		binary.BigEndian.PutUint64(buf, node.id)
		docIDBytes, err := bucket.Get(buf)
		if err != nil {
			// If the mapping is not found (e.g., due to corrupted state after ungraceful shutdown),
			// log a warning and skip this node instead of failing completely
			h.logger.WithFields(map[string]interface{}{
				"action":  "restore_doc_mappings",
				"node_id": node.id,
				"error":   err.Error(),
			}).Error("skipping node with missing doc mapping")
			h.nodes[node.id] = nil
			continue
		}

		// Validate that we have enough bytes for a uint64 (8 bytes)
		if len(docIDBytes) < 8 {
			h.logger.WithFields(map[string]interface{}{
				"action":       "restore_doc_mappings",
				"node_id":      node.id,
				"bytes_length": len(docIDBytes),
			}).Error("skipping node with invalid doc mapping data")
			h.nodes[node.id] = nil
			continue
		}

		docID := binary.BigEndian.Uint64(docIDBytes)
		if docID != prevDocID {
			relativeID = 0
			prevDocID = docID
		}
		h.Lock()
		h.docIDVectors[docID] = append(h.docIDVectors[docID], node.id)
		h.Unlock()
		relativeID++
		if node.id > maxNodeID {
			maxNodeID = node.id
		}
		if docID > maxDocID {
			maxDocID = docID
		}
	}
	h.Lock()
	h.vecIDcounter = maxNodeID + 1
	h.maxDocID = maxDocID
	h.Unlock()
	return nil
}

func (h *hnsw) populateKeys() {
	for docID, nodeIDs := range h.docIDVectors {
		for relativeID, nodeID := range nodeIDs {
			if h.compressed.Load() {
				h.compressor.SetKeys(nodeID, docID, uint64(relativeID))
			} else {
				h.cache.SetKeys(nodeID, docID, uint64(relativeID))
			}
		}
	}
}

// multiVectorForNodeID resolves a nodeID to its raw float32 vector by looking
// up the (docID, relativeID) mapping from the compressor's cache, then fetching
// from the object store. Used as the recovery callback for compressed multi-vector
// indexes instead of h.vectorForID, which points to the dropped float32 cache.
func (h *hnsw) multiVectorForNodeID(ctx context.Context, nodeID uint64) ([]float32, error) {
	docID, relativeID := h.compressor.GetKeys(nodeID)
	vecs, err := h.MultiVectorForIDThunk(ctx, docID)
	if err != nil {
		var e storobj.ErrNotFound
		if errors.As(err, &e) {
			// key not-found errors by the requested node id, not the internal
			// docID fetch
			return nil, storobj.NewErrNotFoundf(nodeID,
				"multi-vector recovery (docID %d): %v", docID, err)
		}
		return nil, errors.Wrapf(err, "multi-vector recovery for nodeID %d (docID %d)", nodeID, docID)
	}
	if int(relativeID) >= len(vecs) {
		return nil, errors.Errorf("multi-vector recovery: relativeID %d out of bounds for docID %d (nodeID %d, got %d vecs)",
			relativeID, docID, nodeID, len(vecs))
	}
	return vecs[relativeID], nil
}

func (h *hnsw) tombstoneCleanup(shouldAbort cyclemanager.ShouldAbortCallback) bool {
	if !h.cachePrefilled.Load() {
		return false
	}

	if h.allocChecker != nil {
		// allocChecker is optional, we can only check if it was actually set

		// It's hard to estimate how much memory we'd need to do a successful
		// hnsw delete cleanup. The value below is probably vastly overstated.
		// However, without a doubt, delete cleanup could lead to temporary
		// memory increases, either because it loads vectors into cache or
		// because it rewrites connections in a way that they could need more
		// memory than before. Either way, it's probably a good idea not to
		// start a cleanup cycle if we are already this close to running out of
		// memory.
		memoryNeeded := int64(tombstoneCleanupMemoryNeeded)

		if err := h.allocChecker.CheckAlloc(memoryNeeded); err != nil {
			h.logger.WithFields(logrus.Fields{
				"action": "hnsw_tombstone_cleanup",
				"event":  "cleanup_skipped_oom",
				"class":  h.className,
			}).Warnf("skipping hnsw cleanup due to memory pressure: %v", err)
			return false
		}
	}
	executed, err := h.cleanUpTombstonedNodes(shouldAbort)
	if err != nil {
		h.logger.WithField("action", "hnsw_tombstone_cleanup").
			Error(err)
	}
	return executed
}

// The vector_index_tombstones metric is represented as a counter so on
// restart we need to reset it to the current number of tombstones read from
// the commit log.
func (h *hnsw) resetTombstoneMetric() {
	h.tombstoneLock.Lock()
	defer h.tombstoneLock.Unlock()
	if len(h.tombstones) > 0 {
		h.metrics.SetTombstone(len(h.tombstones))
	}
}

// PostStartup triggers routines that should happen after startup. The startup
// process is triggered during the creation which in turn happens as part of
// the shard creation. Some post-startup routines, such as prefilling the
// vector cache, however, depend on the shard being ready as they will call
// getVectorForID.
// Nothing here may start after Drop or Shutdown. PostStartup can arrive that late,
// since Drop never cancels shutdownCtx.
func (h *hnsw) PostStartup(ctx context.Context) {
	if !h.initMaintenanceUnlessTornDown() {
		h.logger.WithFields(logrus.Fields{
			"action":   "hnsw_post_startup",
			"index_id": h.id,
		}).Debug("skipping post-startup: index is torn down")
		return
	}
	h.prefillCache(ctx)
}

func (h *hnsw) prefillCache(ctx context.Context) {
	// If the cache is already marked as prefilled (e.g. fresh index with no
	// commit-log state), there is nothing to do. Skipping avoids launching a
	// goroutine that could race with checkAndCompress on h.cache/h.compressor.
	if h.cachePrefilled.Load() {
		return
	}

	// limit is read only by the serial prefiller, which runs under the same
	// !h.compressed.Load() a compressed index never reaches. h.cache is nil there, so
	// nothing below may touch it either.
	nodes, limit := h.nodeCount(), 0
	if !h.compressed.Load() {
		maxSize := h.cache.CopyMaxSize()

		// A cache that cannot hold every node cannot keep what a prefill loads: the
		// first query on an uncached node takes the count to maxSize and replaceIfFull
		// drops the lot. Filling it anyway costs a whole pass, one random seek per
		// vector on the serial path, for a cache that is empty again on first use.
		if !cacheHoldsEveryNode(maxSize, nodes) {
			h.cachePrefilled.Store(true)
			h.logger.WithFields(logrus.Fields{
				"action":     "hnsw_vector_cache_prefill",
				"index_id":   h.id,
				"cache_size": maxSize,
				"nodes":      nodes,
			}).Info("skipping vector cache prefill: cache too small to hold every node")
			return
		}

		limit = int(maxSize) - prefillReservedCacheSlots
	}

	// Registered before the goroutine starts, so Shutdown and Drop cannot miss it.
	// Teardown can also land between here and PostStartup's own check.
	prefillCtx, cancel := context.WithCancel(ctx)
	if !h.registerPrefill(cancel) {
		cancel()
		h.logger.WithFields(logrus.Fields{
			"action":   "hnsw_vector_cache_prefill",
			"index_id": h.id,
		}).Debug("skipping vector cache prefill: index is shutting down or already prefilling")
		return
	}

	prefillCacheFunc := func() {
		defer h.prefillWG.Done()
		defer cancel()
		// A false cachePrefilled permanently disables tombstone cleanup and every
		// compression path, so it is deferred rather than trailing: on the async branch
		// GoWrapper recovers a panic and a trailing store would never run. LIFO puts it
		// before Done, so disablePostStartup still sees it set.
		defer h.cachePrefilled.Store(true)

		h.logPrefillStopped(h.runPrefill(prefillCtx, limit), prefillCtx)
	}

	// index_id and nodes on both: without them two of a thousand shards are told apart
	// only by which fields the line happens to carry, and a resident count means little
	// without the total it is a fraction of.
	entry := h.logger.WithFields(logrus.Fields{
		"index_id": h.id,
		"nodes":    nodes,
	})
	if h.waitForCachePrefill {
		entry.WithFields(logrus.Fields{
			"action":                 "hnsw_prefill_cache_sync",
			"wait_for_cache_prefill": true,
		}).Info("waiting for vector cache prefill to complete")
		prefillCacheFunc()
	} else {
		entry.WithFields(logrus.Fields{
			"action":                 "hnsw_prefill_cache_async",
			"wait_for_cache_prefill": false,
		}).Info("not waiting for vector cache prefill, running in background")
		enterrors.GoWrapper(prefillCacheFunc, h.logger)
	}
}

// runPrefill picks the filler for this index's cache. limit applies only to the serial
// by-id prefiller; the scan carries its own budget and the compressor its own cache.
func (h *hnsw) runPrefill(ctx context.Context, limit int) error {
	if h.compressed.Load() {
		if !h.multivector.Load() || h.muvera.Load() {
			h.compressor.PrefillCache(ctx)
		} else {
			h.compressor.PrefillMultiCache(ctx, h.docIDVectors)
		}
		return nil
	}

	if scan, reason := h.useParallelPrefill(); scan {
		// scanning the objects bucket beats looking every vector up by id, which is
		// disk-seek bound; prefillCacheParallel logs which of the two scans it picked
		return h.prefillCacheParallel(ctx)
	} else {
		h.logPrefillPath(prefillPathSerial, reason)
		return newVectorCachePrefiller(h.cache, h, h.logger).Prefill(ctx, limit)
	}
}

// prefillStoppedByShutdown tells a prefill that was stopped from one that failed. Both
// scans report context.Canceled for a teardown and for a read failing against an
// already-cancelled parent, so only the prefill's own context separates them, since
// nothing but teardown cancels that. Both latch the first error before cancelling, so a
// genuine failure never arrives here as context.Canceled.
func prefillStoppedByShutdown(err error, prefillCtx context.Context) bool {
	return errors.Is(err, context.Canceled) && prefillCtx.Err() != nil
}

func (h *hnsw) logPrefillStopped(err error, prefillCtx context.Context) {
	if err == nil {
		return
	}
	entry := h.logger.WithFields(logrus.Fields{
		"action":   "hnsw_vector_cache_prefill",
		"index_id": h.id,
	})
	if prefillStoppedByShutdown(err, prefillCtx) {
		entry.Debug("vector cache prefill stopped: context canceled")
		return
	}
	entry.Error(err)
}

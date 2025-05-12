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
	"encoding/binary"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	enterrors "github.com/weaviate/weaviate/entities/errors"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/visited"
	"github.com/weaviate/weaviate/entities/cyclemanager"
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

	fileNames, err := getCommitFileNames(h.rootPath, h.id, stateTimestamp)
	if err != nil {
		return err
	}

	state, err = loadCommitLoggerState(h.logger, fileNames, state, h.metrics)
	if err != nil {
		return errors.Wrap(err, "load commit logger state")
	}

	if state == nil {
		// nothing to do
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
		}
	}

	if state.Compressed {
		h.compressed.Store(state.Compressed)
		h.cache.Drop()
		if state.CompressionPQData != nil {
			data := state.CompressionPQData
			h.dims = int32(data.Dimensions)

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
						h.allocChecker,
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
						h.allocChecker,
					)
				}
				if err != nil {
					return errors.Wrap(err, "Restoring compressed data.")
				}
			}
		} else if state.CompressionSQData != nil {
			data := state.CompressionSQData
			h.dims = int32(data.Dimensions)
			if !h.multivector.Load() || h.muvera.Load() {
				h.compressor, err = compressionhelpers.RestoreHNSWSQCompressor(
					h.distancerProvider,
					1e12,
					h.logger,
					data.A,
					data.B,
					data.Dimensions,
					h.store,
					h.allocChecker,
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
					h.allocChecker,
				)
			}
			if err != nil {
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
		if len(h.nodes) > 0 {
			if vec, err := h.vectorForID(context.Background(), h.entryPointID); err == nil {
				h.dims = int32(len(vec))
			}
		}
	}

	if h.compressed.Load() && h.multivector.Load() && !h.muvera.Load() {
		h.compressor.GrowCache(uint64(len(h.nodes)))
		h.populateKeys()
	}

	h.resetTombstoneMetric()

	// make sure the visited list pool fits the current size
	h.pools.visitedLists.Destroy()
	h.pools.visitedLists = nil
	h.pools.visitedLists = visited.NewPool(1, len(h.nodes)+512, h.visitedListPoolMaxSize)

	return nil
}

func (h *hnsw) restoreDocMappings() error {
	prevDocID := uint64(0)
	relativeID := uint64(0)
	maxNodeID := uint64(0)
	maxDocID := uint64(0)
	buf := make([]byte, 8)
	for _, node := range h.nodes {
		if node == nil {
			continue
		}
		binary.BigEndian.PutUint64(buf, node.id)
		docIDBytes, err := h.store.Bucket(h.id + "_mv_mappings").Get(buf)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("failed to get %s_mv_mappings from the bucket", h.id))
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

func (h *hnsw) tombstoneCleanup(shouldAbort cyclemanager.ShouldAbortCallback) bool {
	if h.allocChecker != nil {
		// allocChecker is optional, we can only check if it was actually set

		// It's hard to estimate how much memory we'd need to do a successful
		// hnsw delete cleanup. The value below is probalby vastly overstated.
		// However, without a doubt, delete cleanup could lead to temporary
		// memory increases, either because it loads vectors into cache or
		// because it rewrites connections in a way that they could need more
		// memory than before. Either way, it's probably a good idea not to
		// start a cleanup cycle if we are already this close to running out of
		// memory.
		memoryNeeded := int64(100 * 1024 * 1024)

		if err := h.allocChecker.CheckAlloc(memoryNeeded); err != nil {
			h.logger.WithFields(logrus.Fields{
				"action": "hnsw_tombstone_cleanup",
				"event":  "cleanup_skipped_oom",
				"class":  h.className,
			}).WithError(err).
				Warnf("skipping hnsw cleanup due to memory pressure")
			return false
		}
	}
	executed, err := h.cleanUpTombstonedNodes(shouldAbort)
	if err != nil {
		h.logger.WithField("action", "hnsw_tombstone_cleanup").
			WithError(err).Error("tombstone cleanup errord")
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
func (h *hnsw) PostStartup() {
	h.commitLog.InitMaintenance()
	h.prefillCache()
}

func (h *hnsw) prefillCache() {
	limit := 0
	if h.compressed.Load() {
		limit = int(h.compressor.GetCacheMaxSize())
	} else {
		limit = int(h.cache.CopyMaxSize())
	}

	f := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Minute)
		defer cancel()

		h.logger.WithFields(logrus.Fields{
			"action":   "prefill_cache",
			"duration": 60 * time.Minute,
		}).Debug("context.WithTimeout")

		var err error
		if h.compressed.Load() {
			if !h.multivector.Load() || h.muvera.Load() {
				h.compressor.PrefillCache()
			} else {
				h.compressor.PrefillMultiCache(h.docIDVectors)
			}
		} else {
			err = newVectorCachePrefiller(h.cache, h, h.logger).Prefill(ctx, limit)
		}

		if err != nil {
			h.logger.WithError(err).Error("prefill vector cache")
		}
	}

	if h.waitForCachePrefill {
		h.logger.WithFields(logrus.Fields{
			"action":                 "hnsw_prefill_cache_sync",
			"wait_for_cache_prefill": true,
		}).Info("waiting for vector cache prefill to complete")
		f()
	} else {
		h.logger.WithFields(logrus.Fields{
			"action":                 "hnsw_prefill_cache_async",
			"wait_for_cache_prefill": false,
		}).Info("not waiting for vector cache prefill, running in background")
		enterrors.GoWrapper(f, h.logger)
	}
}

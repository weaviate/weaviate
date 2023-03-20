//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hnsw

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/visited"
	ssdhelpers "github.com/weaviate/weaviate/adapters/repos/db/vector/ssdhelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/diskio"
)

func (h *hnsw) init(cfg Config) error {
	h.pools = newPools(h.maximumConnectionsLayerZero)

	if err := h.restoreFromDisk(); err != nil {
		return errors.Wrapf(err, "restore hnsw index %q", cfg.ID)
	}

	// init commit logger for future writes
	cl, err := cfg.MakeCommitLoggerThunk()
	if err != nil {
		return errors.Wrap(err, "create commit logger")
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
func (h *hnsw) restoreFromDisk() error {
	beforeAll := time.Now()
	defer h.metrics.TrackStartupTotal(beforeAll)

	fileNames, err := getCommitFileNames(h.rootPath, h.id)
	if err != nil {
		return err
	}

	if len(fileNames) == 0 {
		// nothing to do
		return nil
	}

	fileNames, err = NewCorruptedCommitLogFixer(h.logger).Do(fileNames)
	if err != nil {
		return errors.Wrap(err, "corrupted commit log fixer")
	}

	var state *DeserializationResult
	for i, fileName := range fileNames {
		beforeIndividual := time.Now()

		fd, err := os.Open(fileName)
		if err != nil {
			return errors.Wrapf(err, "open commit log %q for reading", fileName)
		}

		defer fd.Close()

		metered := diskio.NewMeteredReader(fd,
			h.metrics.TrackStartupReadCommitlogDiskIO)
		fdBuf := bufio.NewReaderSize(metered, 256*1024)

		var valid int
		state, valid, err = NewDeserializer(h.logger).Do(fdBuf, state, false)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				// we need to check for both EOF or UnexpectedEOF, as we don't know where
				// the commit log got corrupted, a field ending that weset a longer
				// encoding for would return EOF, whereas a field read with binary.Read
				// with a fixed size would return UnexpectedEOF. From our perspective both
				// are unexpected.

				h.logger.WithField("action", "hnsw_load_commit_log_corruption").
					WithField("path", fileName).
					Error("write-ahead-log ended abruptly, some elements may not have been recovered")

				// we need to truncate the file to its valid length!
				if err := os.Truncate(fileName, int64(valid)); err != nil {
					return errors.Wrapf(err, "truncate corrupt commit log %q", fileName)
				}
			} else {
				// only return an actual error on non-EOF errors, otherwise we'll end
				// up in a startup crashloop
				return errors.Wrapf(err, "deserialize commit log %q", fileName)
			}
		}

		h.metrics.StartupProgress(float64(i+1) / float64(len(fileNames)))
		h.metrics.TrackStartupIndividual(beforeIndividual)
	}

	h.nodes = state.Nodes
	h.currentMaximumLayer = int(state.Level)
	h.entryPointID = state.Entrypoint
	h.tombstones = state.Tombstones
	h.compressed.Store(state.Compressed)

	if state.Compressed {
		err := h.initCompressedStore()
		if err != nil {
			return err
		}
		h.cache.drop()
		h.pq, err = ssdhelpers.NewProductQuantizerWithEncoders(
			int(state.PQData.M),
			int(state.PQData.Ks),
			state.PQData.UseBitsEncoding,
			h.distancerProvider,
			int(state.PQData.Dimensions), state.PQData.EncoderType,
			state.PQData.Encoders,
		)
		if err != nil {
			return errors.Wrap(err, "Restoring PQ data.")
		}
	} else {
		// make sure the cache fits the current size
		h.cache.grow(uint64(len(h.nodes)))
	}

	// make sure the visited list pool fits the current size
	h.pools.visitedLists.Destroy()
	h.pools.visitedLists = nil
	h.pools.visitedLists = visited.NewPool(1, len(h.nodes)+512)

	return nil
}

func (h *hnsw) tombstoneCleanup(stopFunc cyclemanager.ShouldBreakFunc) {
	if err := h.CleanUpTombstonedNodes(stopFunc); err != nil {
		h.logger.WithField("action", "hnsw_tombstone_cleanup").
			WithError(err).Error("tombstone cleanup errord")
	}
}

// PostStartup triggers routines that should happen after startup. The startup
// process is triggered during the creation which in turn happens as part of
// the shard creation. Some post-startup routines, such as prefilling the
// vector cache, however, depend on the shard being ready as they will call
// getVectorForID.
func (h *hnsw) PostStartup() {
	h.tombstoneCleanupCycle.Start()
	h.prefillCache()
}

func (h *hnsw) prefillCache() {
	limit := 0
	if h.compressed.Load() {
		limit = int(h.compressedVectorsCache.copyMaxSize())
	} else {
		limit = int(h.cache.copyMaxSize())
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Minute)
		defer cancel()

		var err error
		if h.compressed.Load() {
			cursor := h.compressedStore.Bucket(helpers.CompressedObjectsBucketLSM).Cursor()
			for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
				id := binary.LittleEndian.Uint64(k)
				h.compressedVectorsCache.grow(id)
				h.compressedVectorsCache.preload(id, v)
			}
			cursor.Close()
		} else {
			err = newVectorCachePrefiller(h.cache, h, h.logger).Prefill(ctx, limit)
		}

		if err != nil {
			h.logger.WithError(err).Error("prefill vector cache")
		}
	}()
}

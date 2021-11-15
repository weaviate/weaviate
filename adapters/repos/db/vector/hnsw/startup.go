//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package hnsw

import (
	"bufio"
	"context"
	"io"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/visited"
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
	h.registerMaintainence()

	return nil
}

// if a commit log is already present it will be read into memory, if not we
// start with an empty model
func (h *hnsw) restoreFromDisk() error {
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
	for _, fileName := range fileNames {
		fd, err := os.Open(fileName)
		if err != nil {
			return errors.Wrapf(err, "open commit log %q for reading", fileName)
		}

		defer fd.Close()

		fdBuf := bufio.NewReaderSize(fd, 256*1024)

		var valid int
		state, valid, err = NewDeserializer2(h.logger).Do(fdBuf, state, false)
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
	}

	h.nodes = state.Nodes
	h.currentMaximumLayer = int(state.Level)
	h.entryPointID = state.Entrypoint
	h.tombstones = state.Tombstones

	// make sure the cache fits the current size
	h.cache.grow(uint64(len(h.nodes)))

	// make sure the visited list pool fits the current size
	h.pools.visitedLists.Destroy()
	h.pools.visitedLists = nil
	h.pools.visitedLists = visited.NewPool(1, len(h.nodes)+500)

	return nil
}

func (h *hnsw) registerMaintainence() {
	h.registerTombstoneCleanup()
}

func (h *hnsw) registerTombstoneCleanup() {
	if h.cleanupInterval == 0 {
		// user is not interested in periodically cleaning up tombstones, clean up
		// will be manual. (This is also helpful in tests where we want to
		// explicitly control the point at which a cleanup happens)
		return
	}

	go func() {
		t := time.Tick(h.cleanupInterval)
		for {
			select {
			case <-h.cancel:
				return
			case <-t:
				err := h.CleanUpTombstonedNodes()
				if err != nil {
					h.logger.WithField("action", "hnsw_tombstone_cleanup").
						WithError(err).Error("tombstone cleanup errord")
				}
			}
		}
	}()
}

// PostStartup triggers routines that should happen after startup. The startup
// process is triggered during the creation which in turn happens as part of
// the shard creation. Some post-startup routines, such as prefilling the
// vector cache, however, depend on the shard being ready as they will call
// getVectorForID.
func (h *hnsw) PostStartup() {
	h.prefillCache()
}

func (h *hnsw) prefillCache() {
	// The motivation behind having a limit that is lower than the overall cache
	// limit, is so we don't fill up the whole cache right away. This would lead
	// to it overflowing on the next request which would reset it and in turn
	// diminish the benefit of prefilling it in the first place.
	//
	// By setting the level lower, we make sure the topmost layers are present in
	// the cache and anything that is cached subsequently follows user
	// demand based on actual load as opposed to our predictions.
	limit := 500000000 / 2 // TODO: v1 make configurable when cache is configurable.

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Minute)
		defer cancel()

		err := newVectorCachePrefiller(h.cache, h, h.logger).Prefill(ctx, limit)
		if err != nil {
			h.logger.WithError(err).Error("prefill vector cache")
		}
	}()
}

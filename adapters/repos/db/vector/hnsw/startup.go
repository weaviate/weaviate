package hnsw

import (
	"os"
	"time"

	"github.com/pkg/errors"
)

func (h *hnsw) init(cfg Config) error {
	if err := h.restoreFromDisk(); err != nil {
		return errors.Wrapf(err, "restore hnsw index %q", cfg.ID)
	}

	// init commit logger for future writes
	cl, err := cfg.MakeCommitLoggerThunk()
	if err != nil {
		return errors.Wrap(err, "create commit logger")
	}

	h.commitLog = cl
	h.registerMaintainence(cfg)

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

		state, err = NewDeserializer(h.logger).Do(fd, state)
		if err != nil {
			return errors.Wrapf(err, "deserialize commit log %q", fileName)
		}
	}

	h.nodes = state.Nodes
	h.currentMaximumLayer = int(state.Level)
	h.entryPointID = state.Entrypoint
	h.tombstones = state.Tombstones

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
		t := time.Tick(cfg.TombstoneCleanupInterval)
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

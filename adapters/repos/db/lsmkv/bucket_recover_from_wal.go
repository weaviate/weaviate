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

package lsmkv

import (
	"bufio"
	"context"
	errors2 "errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	bolterrors "go.etcd.io/bbolt/errors"

	"github.com/weaviate/weaviate/entities/diskio"
)

var logOnceWhenRecoveringFromWAL sync.Once

func (b *Bucket) mayRecoverFromCommitLogs(ctx context.Context, sg *SegmentGroup, files map[string]int64) (err error) {
	// the context is only ever checked once at the beginning, as there is no
	// point in aborting an ongoing recovery. It makes more sense to let it
	// complete and have the next recovery (this is called once per bucket) run
	// into this error. This way in a crashloop we'd eventually recover each
	// bucket until there is nothing left to recover and startup could complete
	// in time
	if err := ctx.Err(); err != nil {
		return errors.Wrap(err, "recover commit log")
	}

	// WAL-less buckets have no commit logs to recover from.
	// Durability is provided by the RAFT log instead.
	if b.walDisabled {
		return nil
	}

	var walFileNames []string
	for file, size := range files {
		if filepath.Ext(file) != ".wal" {
			// skip, this could be disk segments, etc.
			continue
		}

		path := filepath.Join(b.dir, file)

		if size == 0 {
			err := os.Remove(path)
			if err != nil {
				return errors.Wrap(err, "remove empty wal file")
			}
			continue
		}

		walFileNames = append(walFileNames, file)
	}

	if len(walFileNames) == 0 {
		// nothing to do
		return nil
	}

	// Names are segment-<unix-nano>.wal (fixed-width, so lexicographic == chronological).
	// Recovery relies on order: only the last WAL is kept as the active memtable, the
	// rest are flushed to segments. The source is a map, whose iteration order is random.
	sort.Strings(walFileNames)

	logOnceWhenRecoveringFromWAL.Do(func() {
		b.logger.WithField("action", "lsm_recover_from_active_wal").
			WithField("path", b.dir).
			Debug("active write-ahead-log found")
	})

	start := time.Now()

	b.metrics.IncWalRecoveryCount(b.strategy)
	b.metrics.IncWalRecoveryInProgress(b.strategy)

	defer func() {
		b.metrics.DecWalRecoveryInProgress(b.strategy)

		if err != nil {
			b.metrics.IncWalRecoveryFailureCount(b.strategy)
			return
		}

		b.metrics.ObserveWalRecoveryDuration(b.strategy, time.Since(start))
	}()

	recovered := false

	// Data in these WALs predates any strip progress the edit-ops sidecar has
	// recorded: keeping the last WAL's memtable as the live one would hold
	// pre-strip bytes OUTSIDE the pending-segment bookkeeping, and a drop that
	// already recorded those bytes as stripped would see them resurrect — with
	// nothing left to re-clean them once its op is gone. With ops present,
	// every recovered WAL is flushed into a segment, and that segment is
	// durably pended for every op BEFORE the flush deletes the WAL
	// (PendForAllOps below). The pend is load-bearing: a WAL can hold PRE-ARM
	// bytes the arm's snapshot never covered — sidecars written by an older
	// binary whose b.flushing clobber (since fixed by flushAndSwitchLocked's
	// leftover drain) orphaned a failed flush's memtable, or any future
	// regression of that shape. Without the pend such a segment reads as
	// clean and the dropped vector survives finalize.
	sidecarHasOps := false
	sidecarUsable := false
	if sg.editOps != nil {
		hasOps, opsErr := sg.editOps.HasOps()
		switch {
		case opsErr == nil:
			sidecarHasOps, sidecarUsable = hasOps, true
		case errors2.Is(opsErr, bolterrors.ErrTimeout):
			// Still flocked by a previous instance — same hard-fail as the
			// sidecar recovery in newSegmentGroup: loading blind is how a
			// completed drop's data got resurrected.
			return errors.Wrap(opsErr, "probe edit-ops sidecar before WAL recovery")
		default:
			// Torn/corrupt-but-unlocked sidecar: mirror recoverEditOps's
			// policy — never brick the shard over drop-progress bookkeeping.
			// Fail-safe direction: assume ops exist, so every WAL flushes to
			// segments; the pending cover cannot be recorded through the
			// broken sidecar (sidecarUsable stays false), so the drop stalls
			// on this shard — every sidecar read fails too, which blocks the
			// drain poll and the finalize-time drained check from ever
			// reporting success falsely.
			b.logger.WithField("path", b.dir).
				Warnf("probe edit-ops sidecar before WAL recovery failed; flushing all WALs as a precaution: %v", opsErr)
			sidecarHasOps = true
		}
	}

	// recover from each log
	for i, fname := range walFileNames {
		if err := func() error {
			walForActiveMemtable := i == len(walFileNames)-1

			path := filepath.Join(b.dir, strings.TrimSuffix(fname, ".wal"))

			cl, err := newCommitLogger(path, b.strategy, files[fname])
			if err != nil {
				return errors.Wrap(err, "init commit logger")
			}
			if !walForActiveMemtable {
				defer cl.close()
			}

			cl.pause()
			defer cl.unpause()

			mt, err := newMemtable(cl, b.metrics, b.logger, b.allocChecker, memtableConfig{
				path:                         path,
				strategy:                     b.strategy,
				secondaryIndices:             b.secondaryIndices,
				enableChecksumValidation:     b.enableChecksumValidation,
				writeSegmentInfoIntoFileName: b.writeSegmentInfoIntoFileName,
				shouldSkipKeyFunc:            b.shouldSkipKey,
				skipSecondaryKeyCheck:        b.skipSecondaryKeyCheck,
				bm25config:                   b.bm25Config,
			})
			if err != nil {
				return err
			}

			_, err = cl.file.Seek(0, io.SeekStart)
			if err != nil {
				return err
			}

			meteredReader := diskio.NewMeteredReader(cl.file, b.metrics.TrackStartupReadWALDiskIO)
			errRecovery := newCommitLoggerParser(b.strategy, bufio.NewReaderSize(meteredReader, 32*1024), mt).Do()
			if errRecovery != nil {
				b.logger.WithField("action", "lsm_recover_from_active_wal_corruption").
					WithField("path", filepath.Join(b.dir, fname)).
					Error(errors.Wrap(errRecovery, "write-ahead-log ended abruptly, some elements may not have been recovered"))
			}

			if mt.strategy == StrategyInverted {
				mt.averagePropLength, mt.propLengthCount = sg.GetAveragePropertyLength()
			}

			// immediately flush the .wal file if there have been any damages during recovery. This means that the file is
			// damaged and cannot be used for new writes.
			if walForActiveMemtable && errRecovery == nil && !sidecarHasOps {
				_, err = cl.file.Seek(0, io.SeekEnd)
				if err != nil {
					return err
				}
				b.active = mt
			} else {
				switch {
				case sidecarHasOps && sidecarUsable:
					// Durably cover the flush target BEFORE mt.flush deletes
					// the WAL — its segment ID derives from the WAL name, so
					// it is known up front. Committing the row after the
					// flush would leave a crash window in which the WAL is
					// gone and the segment reads clean; a startup crash-loop
					// would run that window repeatedly. A row whose flush
					// never produced a segment is pruned by the sidecar
					// recovery that follows. Fatal on failure: the probe just
					// read this sidecar cleanly, so a write error is a real
					// anomaly, and flushing without the cover is exactly the
					// escape this exists to close.
					if perr := sg.editOps.PendForAllOps(segmentID(path)); perr != nil {
						return errors.Wrap(perr, "pend WAL-recovery flush target")
					}
				case sidecarHasOps:
					// Torn sidecar: the cover cannot be recorded, and failing
					// the load would brick the shard over bookkeeping. Flush
					// anyway — the broken sidecar blocks every drain/finalize
					// read, so the drop stalls loudly instead of completing
					// falsely.
					b.logger.WithField("path", b.dir).
						Warnf("flushing WAL %s without recording pending cover (sidecar unreadable); drop-vector cleanup on this shard is stalled until the sidecar is repaired", fname)
				}
				segmentPath, err := mt.flush()
				if err != nil {
					return errors.Wrap(err, "flush memtable after WAL recovery")
				}

				if mt.Size() == 0 {
					return nil
				}

				if err := sg.add(segmentPath); err != nil {
					return err
				}
			}

			if b.strategy == StrategyReplace && b.monitorCount {
				// having just flushed the memtable we now have the most up2date count which
				// is a good place to update the metric
				b.metrics.ObjectCount(sg.count())
			}

			b.logger.WithField("action", "lsm_recover_from_active_wal_success").
				WithField("path", filepath.Join(b.dir, fname)).
				Debug("successfully recovered from write-ahead-log")

			return nil
		}(); err != nil {
			return err
		}

		recovered = true
	}

	// force re-sort if any segment was added
	if recovered {
		sort.Slice(sg.segments, func(i, j int) bool {
			return sg.segments[i].getPath() < sg.segments[j].getPath()
		})
	}

	return nil
}

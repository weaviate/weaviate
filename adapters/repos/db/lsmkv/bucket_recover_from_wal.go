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
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/entities/diskio"
)

var logOnceWhenRecoveringFromWAL sync.Once

// recoverFromCommitLogsReadOnly replays every write-ahead-log of a read-only
// follower bucket into a single in-memory memtable, which becomes the bucket's
// active memtable. Nothing is written to disk: WAL files are opened O_RDONLY,
// the memtable's commit logger is a noop, empty WALs are left in place (not
// removed), and no memtable is ever flushed into a segment. Because WAL data is
// always newer than the on-disk segments (it is the not-yet-flushed tail), and
// the read path overlays the active memtable on top of the segments, merging all
// WALs into one memtable preserves correct precedence. WALs are replayed
// oldest→newest so newer entries override older ones; the file name is
// segment-<unixnano>.wal, a fixed-width key for which lexical order equals
// chronological order. A torn WAL tail (expected in a crash-consistent snapshot
// of a live writer) is tolerated: recovery stops at the damage and keeps what it
// read, consistent with the robust-reader principle.
func (b *Bucket) recoverFromCommitLogsReadOnly(ctx context.Context, sg *SegmentGroup, files map[string]int64) error {
	if err := ctx.Err(); err != nil {
		return errors.Wrap(err, "recover commit log (read-only)")
	}

	var walFileNames []string
	for file, size := range files {
		if filepath.Ext(file) != ".wal" {
			// skip, this could be disk segments, etc.
			continue
		}
		if size == 0 {
			// An empty WAL would normally be removed; a read-only follower
			// leaves it untouched and ignores it.
			continue
		}
		walFileNames = append(walFileNames, file)
	}

	if len(walFileNames) == 0 {
		// nothing to do; b.active is created later by createNewActiveMemtable
		return nil
	}

	sort.Strings(walFileNames)

	logOnceWhenRecoveringFromWAL.Do(func() {
		b.logger.WithField("action", "lsm_recover_from_active_wal").
			WithField("path", b.dir).
			Debug("active write-ahead-log found (read-only follower)")
	})

	start := time.Now()
	b.metrics.IncWalRecoveryCount(b.strategy)
	b.metrics.IncWalRecoveryInProgress(b.strategy)
	defer func() {
		b.metrics.DecWalRecoveryInProgress(b.strategy)
		b.metrics.ObserveWalRecoveryDuration(b.strategy, time.Since(start))
	}()

	// One in-memory memtable accumulates every WAL. Its path mirrors the newest
	// WAL so any code that inspects walPath sees a consistent name; nothing is
	// written there.
	memtablePath := filepath.Join(b.dir, strings.TrimSuffix(walFileNames[len(walFileNames)-1], ".wal"))
	cl := newNoopCommitLogger(memtablePath)
	mt, err := newMemtable(cl, b.metrics, b.logger, b.allocChecker, memtableConfig{
		path:                         memtablePath,
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

	for _, fname := range walFileNames {
		if err := func() error {
			path := filepath.Join(b.dir, fname)
			f, err := os.Open(path)
			if err != nil {
				return errors.Wrapf(err, "open wal %q read-only", fname)
			}
			defer f.Close()

			meteredReader := diskio.NewMeteredReader(f, b.metrics.TrackStartupReadWALDiskIO)
			errRecovery := newCommitLoggerParser(b.strategy, bufio.NewReaderSize(meteredReader, 32*1024), mt).Do()
			if errRecovery != nil {
				b.logger.WithField("action", "lsm_recover_from_active_wal_corruption").
					WithField("path", path).
					Warnf("write-ahead-log ended abruptly, some elements may not have been recovered: %v", errRecovery)
			}
			return nil
		}(); err != nil {
			return err
		}
	}

	if mt.strategy == StrategyInverted {
		mt.averagePropLength, mt.propLengthCount = sg.GetAveragePropertyLength()
	}

	b.active = mt

	if b.strategy == StrategyReplace && b.monitorCount {
		b.metrics.ObjectCount(sg.count())
	}

	return nil
}

func (b *Bucket) mayRecoverFromCommitLogs(ctx context.Context, sg *SegmentGroup, files map[string]int64) (err error) {
	if b.readOnly {
		// A read-only follower must not reopen the WAL for append, flush a
		// recovered memtable into a new segment, or remove an empty WAL — all
		// writes. It replays the WAL into an in-memory memtable instead.
		return b.recoverFromCommitLogsReadOnly(ctx, sg, files)
	}
	// the context is only ever checked once at the beginning, as there is no
	// point in aborting an ongoing recovery. It makes more sense to let it
	// complete and have the next recovery (this is called once per bucket) run
	// into this error. This way in a crashloop we'd eventually recover each
	// bucket until there is nothing left to recover and startup could complete
	// in time
	if err := ctx.Err(); err != nil {
		return errors.Wrap(err, "recover commit log")
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
					Error(errors.Wrap(err, "write-ahead-log ended abruptly, some elements may not have been recovered"))
			}

			if mt.strategy == StrategyInverted {
				mt.averagePropLength, mt.propLengthCount = sg.GetAveragePropertyLength()
			}

			// immediately flush the .wal file if there have been any damages during recovery. This means that the file is
			// damaged and cannot be used for new writes.
			if walForActiveMemtable && errRecovery == nil {
				_, err = cl.file.Seek(0, io.SeekEnd)
				if err != nil {
					return err
				}
				b.active = mt
			} else {
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

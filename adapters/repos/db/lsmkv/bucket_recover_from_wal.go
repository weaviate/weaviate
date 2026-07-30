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

	var walFileNames []string
	for file, size := range files {
		if filepath.Ext(file) != ".wal" {
			// skip, this could be disk segments, etc.
			continue
		}

		if size == 0 {
			if b.immutable {
				continue
			}
			if err := os.Remove(filepath.Join(b.dir, file)); err != nil {
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
	// Recovery relies on chronological order, and the source is a map, whose iteration
	// order is random.
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

	if b.immutable {
		return b.replayCommitLogsIntoMemtable(sg, walFileNames)
	}

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

			mt, err := b.newMemtableAt(path, cl)
			if err != nil {
				return err
			}

			_, err = cl.file.Seek(0, io.SeekStart)
			if err != nil {
				return err
			}

			errRecovery := b.parseCommitLog(cl.file, mt, filepath.Join(b.dir, fname))

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

// replayCommitLogsIntoMemtable reads the write-ahead-logs of an immutable bucket, oldest
// first, into a single active memtable. Merging them in memory keeps the same read
// precedence as flushing the older ones to segments would, without creating, changing or
// removing a single file in the bucket directory.
func (b *Bucket) replayCommitLogsIntoMemtable(sg *SegmentGroup, walFileNames []string) error {
	mt, err := b.createNewActiveMemtable()
	if err != nil {
		return err
	}

	for _, fname := range walFileNames {
		if err := b.readCommitLogFile(filepath.Join(b.dir, fname), mt); err != nil {
			return err
		}
	}

	if mt.strategy == StrategyInverted {
		mt.averagePropLength, mt.propLengthCount = sg.GetAveragePropertyLength()
	}

	b.active = mt
	return nil
}

// readCommitLogFile opens a write-ahead-log read-only and reads it into mt.
func (b *Bucket) readCommitLogFile(path string, mt *Memtable) error {
	f, err := os.Open(path)
	if err != nil {
		return errors.Wrap(err, "open write-ahead-log")
	}
	defer f.Close()

	// an abrupt end is already logged, and what was read before it stays readable
	_ = b.parseCommitLog(f, mt, path)
	return nil
}

// parseCommitLog reads a write-ahead-log into mt and returns the error of a log that ended
// abruptly. The entries read before that point stay in the memtable.
func (b *Bucket) parseCommitLog(r diskio.Reader, mt *Memtable, logPath string) error {
	meteredReader := diskio.NewMeteredReader(r, b.metrics.TrackStartupReadWALDiskIO)

	err := newCommitLoggerParser(b.strategy, bufio.NewReaderSize(meteredReader, 32*1024), mt).Do()
	if err != nil {
		b.logger.WithField("action", "lsm_recover_from_active_wal_corruption").
			WithField("path", logPath).
			Errorf("write-ahead-log ended abruptly, some elements may not have been recovered: %v", err)
	}

	return err
}

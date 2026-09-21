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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/entities/diskio"
	"github.com/weaviate/weaviate/usecases/config"
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
	memtableThreshold := b.walReplayMemtableThreshold()

	// recover from each log
	for i, fname := range walFileNames {
		walForActiveMemtable := i == len(walFileNames)-1
		if err := b.recoverFromWAL(sg, fname, files[fname], walForActiveMemtable,
			memtableThreshold); err != nil {
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

// defaultWALReplayMemtableThreshold is the size a busy bucket's memtable settles
// at, so a replay cuts where the flush cycle would have.
const defaultWALReplayMemtableThreshold = config.DefaultPersistenceMemtablesMaxSize * 1024 * 1024

// walReplayMemtableThreshold is the resizer's configured max rather than
// b.memtableThreshold, which during recovery still holds the initial size.
func (b *Bucket) walReplayMemtableThreshold() uint64 {
	if b.memtableResizer != nil && b.memtableResizer.active {
		return uint64(b.memtableResizer.cfg.maxSize)
	}

	return defaultWALReplayMemtableThreshold
}

// chunkSegmentPath gives chunk 0 the WAL's own name, so a whole-WAL replay and
// the first chunk of a chunked one write the same file.
func chunkSegmentPath(dir string, walTimestamp int64, chunk int) string {
	return filepath.Join(dir, fmt.Sprintf("segment-%d", walTimestamp+int64(chunk)))
}

// recoverFromWAL consumes a chunked WAL whole, so NewBucket's "b.active == nil"
// arm gives the bucket a fresh memtable. Adopting the recovered one would leave it
// named after segment-<T>, and its next flush would overwrite the chunk already
// written there.
func (b *Bucket) recoverFromWAL(sg *SegmentGroup, fname string, walSize int64,
	forActiveMemtable bool, memtableThreshold uint64,
) error {
	path := filepath.Join(b.dir, strings.TrimSuffix(fname, ".wal"))

	cl, err := newCommitLogger(path, b.strategy, walSize)
	if err != nil {
		return errors.Wrap(err, "init commit logger")
	}
	// covers the error returns before the terminal branches below, which either
	// close the WAL or hand it to b.active
	closeOnReturn := true
	defer func() {
		if closeOnReturn {
			cl.close()
		}
	}()

	cl.pause()
	defer cl.unpause()

	mt, err := b.newMemtableAt(cl, path)
	if err != nil {
		return err
	}

	if _, err := cl.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	// counts bytes pulled from the file, so it runs ahead of what the parser has
	// consumed by at most one bufio fill
	var walBytesRead int64
	meteredReader := diskio.NewMeteredReader(cl.file, func(read, nanoseconds int64) {
		walBytesRead += read
		b.metrics.TrackStartupReadWALDiskIO(read, nanoseconds)
	})
	parser := newCommitLoggerParser(b.strategy, bufio.NewReaderSize(meteredReader, 32*1024), mt)

	chunks := 0
	// a WAL whose name carries no id cannot name its chunks
	walTimestamp, errTimestamp := parseSegmentTimestamp(fname)
	if errTimestamp == nil {
		parser.replayInChunks(chunkedReplay{
			memtableThreshold: memtableThreshold,
			walThreshold:      int64(b.walThreshold),
			walBytesRead:      func() int64 { return walBytesRead },
			writeChunk: func(full *Memtable) (*Memtable, error) {
				if err := b.writeRecoveredSegment(sg, full,
					chunkSegmentPath(b.dir, walTimestamp, chunks)); err != nil {
					return nil, err
				}
				chunks++

				return b.newMemtableAt(cl, path)
			},
		})
	}

	errRecovery := parser.Do()

	// a WAL this cannot read any further is consumed and unlinked below, which is
	// only safe while every chunk it did read reached the disk
	if parser.writeErr != nil {
		return errors.Wrap(parser.writeErr, "write a chunk of the write-ahead-log")
	}

	if errRecovery != nil {
		b.logger.WithField("action", "lsm_recover_from_active_wal_corruption").
			WithField("path", filepath.Join(b.dir, fname)).
			Error(errors.Wrap(errRecovery, "write-ahead-log ended abruptly, some elements may not have been recovered"))
	}

	// immediately flush the .wal file if there have been any damages during recovery. This means that the file is
	// damaged and cannot be used for new writes.
	if forActiveMemtable && chunks == 0 && errRecovery == nil {
		if mt.strategy == StrategyInverted {
			mt.averagePropLength, mt.propLengthCount = sg.GetAveragePropertyLength()
		}

		if _, err := cl.file.Seek(0, io.SeekEnd); err != nil {
			return err
		}
		b.active = mt
		closeOnReturn = false
	} else {
		tailPath := path
		if chunks > 0 {
			// every chunk boundary replaced parser.memtable, so the tail is there
			tailPath = chunkSegmentPath(b.dir, walTimestamp, chunks)
		}

		if err := b.writeRecoveredSegment(sg, parser.memtable, tailPath); err != nil {
			return err
		}

		// unlinking the WAL is the single commit point for every segment it
		// produced, so it happens only once all of them are on disk
		if err := cl.close(); err != nil {
			return errors.Wrap(err, "close commit log file")
		}
		closeOnReturn = false

		if err := cl.delete(); err != nil {
			return errors.Wrap(err, "delete commit log file")
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
}

// writeRecoveredSegment seeds the property lengths per chunk rather than once per
// WAL, which is what lets the running average accumulate across the chunks.
func (b *Bucket) writeRecoveredSegment(sg *SegmentGroup, mt *Memtable, path string) error {
	if mt.Size() == 0 {
		return nil
	}

	if mt.strategy == StrategyInverted {
		mt.averagePropLength, mt.propLengthCount = sg.GetAveragePropertyLength()
	}

	segmentPath, err := mt.writeSegment(path)
	if err != nil {
		return errors.Wrap(err, "flush memtable after WAL recovery")
	}

	return sg.add(segmentPath)
}

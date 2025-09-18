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

func (b *Bucket) mayRecoverFromCommitLogs(ctx context.Context, sg *SegmentGroup, files map[string]int64) error {
	beforeAll := time.Now()
	defer b.metrics.TrackStartupBucketRecovery(beforeAll)

	recovered := false

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

	if len(walFileNames) > 0 {
		logOnceWhenRecoveringFromWAL.Do(func() {
			b.logger.WithField("action", "lsm_recover_from_active_wal").
				WithField("path", b.dir).
				Debug("active write-ahead-log found")
		})
	}

	// recover from each log
	for i, fname := range walFileNames {
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

		mt, err := newMemtable(path, b.strategy, b.secondaryIndices,
			cl, b.metrics, b.logger, b.enableChecksumValidation, b.bm25Config, b.writeSegmentInfoIntoFileName, b.allocChecker)
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
			mt.averagePropLength, _ = sg.GetAveragePropertyLength()
		}
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
				continue
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

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
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/entities/diskio"
	"github.com/weaviate/weaviate/entities/models"
)

var logOnceWhenRecoveringFromWAL sync.Once

func (sg *SegmentGroup) mayRecoverFromCommitLogs(ctx context.Context, secondaryIndices uint16, bm25Config *models.BM25Config) error {
	beforeAll := time.Now()
	defer sg.metrics.TrackStartupBucketRecovery(beforeAll)

	// the context is only ever checked once at the beginning, as there is no
	// point in aborting an ongoing recovery. It makes more sense to let it
	// complete and have the next recovery (this is called once per bucket) run
	// into this error. This way in a crashloop we'd eventually recover each
	// bucket until there is nothing left to recover and startup could complete
	// in time
	if err := ctx.Err(); err != nil {
		return errors.Wrap(err, "recover commit log")
	}

	list, err := os.ReadDir(sg.dir)
	if err != nil {
		return err
	}

	var walFileNames []string
	for _, fileInfo := range list {
		if filepath.Ext(fileInfo.Name()) != ".wal" {
			// skip, this could be disk segments, etc.
			continue
		}

		path := filepath.Join(sg.dir, fileInfo.Name())

		stat, err := os.Stat(path)
		if err != nil {
			return errors.Wrap(err, "stat commit log")
		}

		if stat.Size() == 0 {
			err := os.Remove(path)
			if err != nil {
				return errors.Wrap(err, "remove empty wal file")
			}
			continue
		}

		walFileNames = append(walFileNames, fileInfo.Name())
	}

	if len(walFileNames) > 0 {
		logOnceWhenRecoveringFromWAL.Do(func() {
			sg.logger.WithField("action", "lsm_recover_from_active_wal").
				WithField("path", sg.dir).
				Debug("active write-ahead-log found")
		})
	}

	// recover from each log
	for i, fname := range walFileNames {
		walForActiveMemtable := i == len(walFileNames)-1

		path := filepath.Join(sg.dir, strings.TrimSuffix(fname, ".wal"))

		cl, err := newCommitLogger(path, sg.strategy)
		if err != nil {
			return errors.Wrap(err, "init commit logger")
		}
		if !walForActiveMemtable {
			defer cl.close()
		}

		cl.pause()
		defer cl.unpause()

		mt, err := newMemtable(path, sg.strategy, secondaryIndices,
			cl, sg.metrics, sg.logger, sg.enableChecksumValidation, bm25Config)
		if err != nil {
			return err
		}

		_, err = cl.file.Seek(0, io.SeekStart)
		if err != nil {
			return err
		}

		meteredReader := diskio.NewMeteredReader(cl.file, sg.metrics.TrackStartupReadWALDiskIO)
		bufio.NewReaderSize(meteredReader, 32*1024)
		err = newCommitLoggerParser(sg.strategy, meteredReader, mt).Do()
		if err != nil {
			sg.logger.WithField("action", "lsm_recover_from_active_wal_corruption").
				WithField("path", filepath.Join(sg.dir, fname)).
				Error(errors.Wrap(err, "write-ahead-log ended abruptly, some elements may not have been recovered"))
		}

		if mt.strategy == StrategyInverted {
			mt.averagePropLength, _ = sg.GetAveragePropertyLength()
		}
		if walForActiveMemtable {
			_, err = cl.file.Seek(0, io.SeekEnd)
			if err != nil {
				return err
			}
		} else {
			if err := mt.flush(); err != nil {
				return errors.Wrap(err, "flush memtable after WAL recovery")
			}

			if mt.Size() == 0 {
				continue
			}

			if err := sg.add(path + ".db"); err != nil {
				return err
			}
		}

		if sg.strategy == StrategyReplace && sg.monitorCount {
			// having just flushed the memtable we now have the most up2date count which
			// is a good place to update the metric
			sg.metrics.ObjectCount(sg.count())
		}

		sg.logger.WithField("action", "lsm_recover_from_active_wal_success").
			WithField("path", filepath.Join(sg.dir, fname)).
			Debug("successfully recovered from write-ahead-log")
	}

	return nil
}

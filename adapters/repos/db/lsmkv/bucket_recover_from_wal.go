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
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/diskio"
)

func (b *Bucket) mayRecoverFromCommitLogs(ctx context.Context) error {
	beforeAll := time.Now()
	defer b.metrics.TrackStartupBucketRecovery(beforeAll)

	// the context is only ever checked once at the beginning, as there is no
	// point in aborting an ongoing recovery. It makes more sense to let it
	// complete and have the next recovery (this is called once per bucket) run
	// into this error. This way in a crashloop we'd eventually recover each
	// bucket until there is nothing left to recover and startup could complete
	// in time
	if err := ctx.Err(); err != nil {
		return errors.Wrap(err, "recover commit log")
	}

	list, err := os.ReadDir(b.dir)
	if err != nil {
		return err
	}

	var walFileNames []string
	for _, fileInfo := range list {
		if filepath.Ext(fileInfo.Name()) != ".wal" {
			// skip, this could be disk segments, etc.
			continue
		}

		walFileNames = append(walFileNames, fileInfo.Name())
	}

	// recover from each log
	for _, fname := range walFileNames {
		path := filepath.Join(b.dir, strings.TrimSuffix(fname, ".wal"))

		cl, err := newCommitLogger(path)
		if err != nil {
			return errors.Wrap(err, "init commit logger")
		}
		defer cl.close()

		cl.pause()
		defer cl.unpause()

		mt, err := newMemtable(path, b.strategy, b.secondaryIndices, cl, b.metrics)
		if err != nil {
			return err
		}

		b.logger.WithField("action", "lsm_recover_from_active_wal").
			WithField("path", path).
			Warning("active write-ahead-log found. Did weaviate crash prior to this? Trying to recover...")

		meteredReader := diskio.NewMeteredReader(bufio.NewReader(cl.file), b.metrics.TrackStartupReadWALDiskIO)

		err = newCommitLoggerParser(b.strategy, meteredReader, mt).Do()
		if err != nil {
			b.logger.WithField("action", "lsm_recover_from_active_wal_corruption").
				WithField("path", filepath.Join(b.dir, fname)).
				Error(errors.Wrap(err, "write-ahead-log ended abruptly, some elements may not have been recovered"))
		}

		if err := mt.flush(); err != nil {
			return errors.Wrap(err, "flush memtable after WAL recovery")
		}

		if mt.Size() == 0 {
			continue
		}

		if err := b.disk.add(path + ".db"); err != nil {
			return err
		}

		if b.strategy == StrategyReplace && b.monitorCount {
			// having just flushed the memtable we now have the most up2date count which
			// is a good place to update the metric
			b.metrics.ObjectCount(b.disk.count())
		}

		b.logger.WithField("action", "lsm_recover_from_active_wal_success").
			WithField("path", filepath.Join(b.dir, fname)).
			Info("successfully recovered from write-ahead-log")
	}

	return nil
}

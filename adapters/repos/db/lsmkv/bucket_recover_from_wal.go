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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"
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

		segmentFileName := strings.TrimSuffix(fileInfo.Name(), ".wal") + ".db"
		ok, err := fileExists(filepath.Join(b.dir, segmentFileName))
		if err != nil {
			return fmt.Errorf("check for presence of segment file for wal %s: %w", fileInfo.Name(), err)
		}

		if !ok {
			// Note (jeroiraz): deletion may not be needed when integrity checking of wal files is incorporated
			if err := os.Remove(filepath.Join(b.dir, fileInfo.Name())); err != nil {
				return fmt.Errorf("delete potentially corrupt wal file %s: %w", fileInfo.Name(), err)
			}

			b.logger.WithField("action", "lsm_recover_from_active_wal").
				WithField("path", filepath.Join(b.dir, fileInfo.Name())).
				WithField("segment_path", segmentFileName).
				Info("Discarded (potentially corrupted) WAL file, because no segment file for " +
					"the same WAL file was found.")

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

		mt, err := newMemtable(path, b.strategy, b.secondaryIndices, cl, b.metrics)
		if err != nil {
			return err
		}

		b.logger.WithField("action", "lsm_recover_from_active_wal").
			WithField("path", path).
			Warning("active write-ahead-log found. Did weaviate crash prior to this? Trying to recover...")

		err = newCommitLoggerParser(bufio.NewReader(cl.file), mt, b.strategy, b.metrics).Do()
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			// we need to check for both EOF or UnexpectedEOF, as we don't know where
			// the commit log got corrupted, a field ending that weset a longer
			// encoding for would return EOF, whereas a field read with binary.Read
			// with a fixed size would return UnexpectedEOF. From our perspective both
			// are unexpected.

			b.logger.WithField("action", "lsm_recover_from_active_wal_corruption").
				WithField("path", filepath.Join(b.dir, fname)).
				Error("write-ahead-log ended abruptly, some elements may not have been recovered")
		} else if err != nil {
			return errors.Wrapf(err, "ingest wal %q", fname)
		}

		if err := mt.flush(); err != nil {
			return errors.Wrap(err, "flush memtable after WAL recovery")
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

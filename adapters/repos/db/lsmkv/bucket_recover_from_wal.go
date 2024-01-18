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
	"context"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
)

func (b *Bucket) recoverFromCommitLogs(ctx context.Context) error {
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

		if filepath.Join(b.dir, fileInfo.Name()) == b.active.path+".wal" {
			// this is the new one which was just created
			continue
		}

		walFileNames = append(walFileNames, fileInfo.Name())
	}

	if len(walFileNames) == 0 {
		// nothing to recover from
		return nil
	}

	// recover from each log
	for _, fname := range walFileNames {
		b.logger.WithField("action", "lsm_recover_from_active_wal").
			WithField("path", filepath.Join(b.dir, fname)).
			Warning("active write-ahead-log found. Did weaviate crash prior to this? Trying to recover...")

		if err := b.parseWALIntoMemtable(filepath.Join(b.dir, fname)); err != nil {
			return errors.Wrapf(err, "ingest wal %q", fname)
		}

		b.logger.WithField("action", "lsm_recover_from_active_wal_success").
			WithField("path", filepath.Join(b.dir, fname)).
			Info("successfully recovered from write-ahead-log")
	}

	if b.active.size > 0 {
		if err := b.FlushAndSwitch(); err != nil {
			return errors.Wrap(err, "flush memtable after WAL recovery")
		}
	}

	// delete the commit logs as we can now be sure that they are part of a disk
	// segment
	for _, fname := range walFileNames {
		if err := os.RemoveAll(filepath.Join(b.dir, fname)); err != nil {
			return errors.Wrap(err, "clean up commit log")
		}
	}

	return nil
}

func (b *Bucket) parseWALIntoMemtable(fname string) error {
	// pause commit logging while reading the old log to avoid creating a
	// duplicate of the log
	b.active.commitlog.pause()
	defer b.active.commitlog.unpause()

	err := newCommitLoggerParser(fname, b.active, b.strategy, b.metrics).Do()
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		// we need to check for both EOF or UnexpectedEOF, as we don't know where
		// the commit log got corrupted, a field ending that weset a longer
		// encoding for would return EOF, whereas a field read with binary.Read
		// with a fixed size would return UnexpectedEOF. From our perspective both
		// are unexpected.

		b.logger.WithField("action", "lsm_recover_from_active_wal_corruption").
			WithField("path", filepath.Join(b.dir, fname)).
			Error("write-ahead-log ended abruptly, some elements may not have been recovered")

		return nil
	}

	return err
}

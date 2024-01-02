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
	"io/fs"
	"path"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/storagestate"
)

// FlushMemtable flushes any active memtable and returns only once the memtable
// has been fully flushed and a stable state on disk has been reached.
//
// This is a preparatory stage for creating backups.
//
// Method should be run only if flushCycle is not running
// (was not started, is stopped, or noop impl is provided)
func (b *Bucket) FlushMemtable() error {
	if b.isReadOnly() {
		return errors.Wrap(storagestate.ErrStatusReadOnly, "flush memtable")
	}

	// this lock does not currently _need_ to be
	// obtained, as the only other place that
	// grabs this lock is the flush cycle, which
	// has just been stopped above.
	//
	// that being said, we will lock here anyway
	// as flushLock may be added elsewhere in the
	// future
	b.flushLock.Lock()
	if b.active == nil && b.flushing == nil {
		b.flushLock.Unlock()
		return nil
	}
	b.flushLock.Unlock()

	stat, err := b.active.commitlog.file.Stat()
	if err != nil {
		b.logger.WithField("action", "lsm_wal_stat").
			WithField("path", b.dir).
			WithError(err).
			Fatal("bucket backup memtable flush failed")
	}

	// attempting a flush&switch on when the active memtable
	// or WAL is empty results in a corrupted backup attempt
	if b.active.Size() > 0 || stat.Size() > 0 {
		if err := b.FlushAndSwitch(); err != nil {
			return err
		}
	}
	return nil
}

// ListFiles lists all files that currently exist in the Bucket. The files are only
// in a stable state if the memtable is empty, and if compactions are paused. If one
// of those conditions is not given, it errors
func (b *Bucket) ListFiles(ctx context.Context, basePath string) ([]string, error) {
	var (
		bucketRoot = b.disk.dir
		files      []string
	)

	err := filepath.WalkDir(bucketRoot, func(currPath string, d fs.DirEntry, err error) error {
		if d.IsDir() {
			return nil
		}
		// ignore .wal files because they are not immutable
		if filepath.Ext(currPath) == ".wal" {
			return nil
		}
		files = append(files, path.Join(basePath, path.Base(currPath)))
		return nil
	})
	if err != nil {
		return nil, errors.Errorf("failed to list files for bucket: %s", err)
	}

	return files, nil
}

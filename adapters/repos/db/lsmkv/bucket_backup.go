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
	"os"
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

	return b.FlushAndSwitch()
}

// ListFiles lists all files that currently exist in the Bucket. The files are only
// in a stable state if the memtable is empty, and if compactions are paused. If one
// of those conditions is not given, it errors
func (b *Bucket) ListFiles(ctx context.Context, basePath string) ([]string, error) {
	bucketRoot := b.disk.dir

	entries, err := os.ReadDir(bucketRoot)
	if err != nil {
		return nil, errors.Errorf("failed to list files for bucket: %s", err)
	}

	var files []string
	for _, entry := range entries {
		// Skip directories as they are used as scratch spaces (e.g. for compaction or flushing).
		// All stable files are in the root of the bucket.
		if entry.IsDir() {
			continue
		}

		ext := filepath.Ext(entry.Name())

		// ignore .wal files because they are not immutable,
		// ignore .tmp files because they are temporary files created during compaction or flushing
		// and are not part of the stable state of the bucket
		if ext == ".wal" || ext == ".tmp" {
			continue
		}

		files = append(files, path.Join(basePath, entry.Name()))
	}
	return files, nil
}

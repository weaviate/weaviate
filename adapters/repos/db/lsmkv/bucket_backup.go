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

	return b.FlushAndSwitch()
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

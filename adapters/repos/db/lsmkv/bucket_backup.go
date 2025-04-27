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
	"bytes"
	"context"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/theOneTrueFileStore"
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
	bucketRoot := b.dir

	fileList := []string{}

	//MapFunc(f func([]byte, []byte) error) (map[string]bool, error)
	theOneTrueFileStore.theOneTrueFileStore.TheOneTrueFileStore().MapFunc(func(key, value []byte) error {
		if bytes.HasPrefix(key, []byte(bucketRoot)) {
			fileList = append(fileList, string(key))
		}
		return nil
	})

	return fileList, nil
}

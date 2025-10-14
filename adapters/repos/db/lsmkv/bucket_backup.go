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
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
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

	files, err := b.listFiles(bucketRoot, basePath)
	if err != nil {
		return nil, err
	}

	// This is temporary check for the vectors_compressed folder, should be removed in v1.36
	// We need to be able to downgrade to a version that doesn't contain the named vectors with quantization fix
	// in order to be able to do that we need to also look for vectors_compressed folder and include it during backup
	if strings.Contains(basePath, fmt.Sprintf("%s_", helpers.VectorsCompressedBucketLSM)) {
		vectorCompressedFiles, err := b.tryTolistLegacyVectorCompressed(basePath)
		if err != nil {
			return nil, errors.Errorf("failed to list files in legacy vectors_compressed folder for bucket: %s", err)
		}
		files = append(files, vectorCompressedFiles...)
	}

	return files, nil
}

func (b *Bucket) listFiles(bucketRoot, basePath string) ([]string, error) {
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

		// ignore .wal files because they are not immutable
		if filepath.Ext(entry.Name()) == ".wal" {
			continue
		}

		files = append(files, path.Join(basePath, entry.Name()))
	}

	return files, nil
}

func (b *Bucket) tryTolistLegacyVectorCompressed(basePath string) ([]string, error) {
	vectorsCompressedBucketRoot := filepath.Join(b.GetRootDir(), "lsm", helpers.VectorsCompressedBucketLSM)
	vectorsCompressedBasePath := filepath.Join(basePath[:strings.LastIndex(basePath, "/")], helpers.VectorsCompressedBucketLSM)
	if _, err := os.Stat(vectorsCompressedBucketRoot); !os.IsNotExist(err) {
		return b.listFiles(vectorsCompressedBucketRoot, vectorsCompressedBasePath)
	}
	return nil, nil
}

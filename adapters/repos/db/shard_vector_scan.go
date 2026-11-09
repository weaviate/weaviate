//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/entities/storobj"
)

// vectorScanWindow bounds how long one cursor stays open: an open cursor keeps
// the segments it started with on disk and holds up the bucket's shutdown.
type vectorScanWindow struct {
	maxDuration time.Duration
	checkEvery  int
}

var defaultVectorScanWindow = vectorScanWindow{maxDuration: 10 * time.Minute, checkEvery: 1000}

// scanVectors calls fn once for every stored vector of the named vector
// targetVector, skipping objects that do not have it. It returns nil once
// every object was read, ctx's error when cancelled and any other error on
// failure. The view is not fixed: objects written during the scan may or may
// not be seen.
func (s *Shard) scanVectors(ctx context.Context, targetVector string, multiVector bool,
	fn func(common.VectorRecord) error,
) error {
	return s.scanVectorsInWindows(ctx, targetVector, multiVector, defaultVectorScanWindow, fn)
}

func (s *Shard) scanVectorsInWindows(ctx context.Context, targetVector string, multiVector bool,
	window vectorScanWindow, fn func(common.VectorRecord) error,
) error {
	if targetVector == "" {
		return errors.New("scan vectors: a named vector is required")
	}

	var lastKey []byte
	for {
		next, done, err := s.scanVectorsWindow(ctx, targetVector, multiVector, window, lastKey, fn)
		if err != nil {
			return fmt.Errorf("scan vectors of %q: %w", targetVector, err)
		}
		if done {
			return nil
		}
		lastKey = next
	}
}

// scanVectorsWindow reads from just after lastKey, or from the start when it
// is nil, until the window ends or the bucket is exhausted. It returns the last
// key it read when the window ended first.
func (s *Shard) scanVectorsWindow(ctx context.Context, targetVector string, multiVector bool,
	window vectorScanWindow, lastKey []byte, fn func(common.VectorRecord) error,
) (next []byte, done bool, err error) {
	// The cursor panics on read errors. Ending the scan instead would report
	// an incomplete index as complete.
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()

	bucket, release := s.store.AcquireBucketForRead(helpers.ObjectsBucketLSM)
	defer release()
	if bucket == nil {
		return nil, false, fmt.Errorf("objects bucket of shard %q: %w", s.name, lsmkv.ErrBucketNotFound)
	}

	cursor := bucket.CursorReplaceReusable()
	defer cursor.Close()

	var k, v []byte
	if lastKey == nil {
		k, v = cursor.First()
	} else {
		k, v = cursor.Seek(lastKey)
		if bytes.Equal(k, lastKey) {
			k, v = cursor.Next()
		}
	}

	started := time.Now()
	read := 0
	for ; k != nil; k, v = cursor.Next() {
		err = ctx.Err()
		if err != nil {
			return nil, false, err
		}

		var record common.VectorRecord
		record, err = vectorRecordFromBinary(v, targetVector, multiVector)
		if err != nil {
			return nil, false, fmt.Errorf("object %x: %w", k, err)
		}
		if record != nil {
			err = fn(record)
			if err != nil {
				return nil, false, err
			}
		}

		read++
		if read%window.checkEvery == 0 && time.Since(started) >= window.maxDuration {
			return bytes.Clone(k), false, nil
		}
	}

	return nil, true, nil
}

// vectorRecordFromBinary returns nil for an object without targetVector.
func vectorRecordFromBinary(data []byte, targetVector string, multiVector bool) (common.VectorRecord, error) {
	docID, err := storobj.DocIDFromBinary(data)
	if err != nil {
		return nil, err
	}

	var notFound storobj.ErrTargetVectorNotFound
	if multiVector {
		vector, err := storobj.MultiVectorFromBinary(data, targetVector)
		if errors.As(err, &notFound) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		if len(vector) == 0 {
			return nil, nil
		}
		return &common.Vector[[][]float32]{ID: docID, Vector: vector}, nil
	}

	vector, err := storobj.VectorFromBinary(data, nil, targetVector)
	if errors.As(err, &notFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(vector) == 0 {
		return nil, nil
	}
	return &common.Vector[[]float32]{ID: docID, Vector: vector}, nil
}

//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package aggregator

import (
	"bytes"
	"context"
	"runtime"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

const contextCheckInterval = 50 // check context every 50 iterations, every iteration adds too much overhead

type Cursor interface {
	First() (k, v []byte, vv [][]byte, bi *sroar.Bitmap)
	Next() (k, v []byte, vv [][]byte, bi *sroar.Bitmap)
	Seek(b []byte) (k, v []byte, vv [][]byte, bi *sroar.Bitmap)
	Close()
}

type ReplaceCursor struct {
	cursor *lsmkv.CursorReplace
}

func (c ReplaceCursor) First() ([]byte, []byte, [][]byte, *sroar.Bitmap) {
	k, v := c.cursor.First()
	return k, v, nil, nil
}

func (c ReplaceCursor) Next() ([]byte, []byte, [][]byte, *sroar.Bitmap) {
	k, v := c.cursor.Next()
	return k, v, nil, nil
}

func (c ReplaceCursor) Seek(b []byte) ([]byte, []byte, [][]byte, *sroar.Bitmap) {
	k, v := c.cursor.Seek(b)
	return k, v, nil, nil
}

func (c ReplaceCursor) Close() {
	c.cursor.Close()
}

type SetCursor struct {
	cursor *lsmkv.CursorSet
}

func (c SetCursor) First() ([]byte, []byte, [][]byte, *sroar.Bitmap) {
	k, v := c.cursor.First()
	return k, nil, v, nil
}

func (c SetCursor) Next() ([]byte, []byte, [][]byte, *sroar.Bitmap) {
	k, v := c.cursor.Next()
	return k, nil, v, nil
}

func (c SetCursor) Seek(b []byte) ([]byte, []byte, [][]byte, *sroar.Bitmap) {
	k, v := c.cursor.Seek(b)
	return k, nil, v, nil
}

func (c SetCursor) Close() {
	c.cursor.Close()
}

type RoaringCursor struct {
	cursor lsmkv.CursorRoaringSet
}

func (c RoaringCursor) First() ([]byte, []byte, [][]byte, *sroar.Bitmap) {
	k, b := c.cursor.First()
	return k, nil, nil, b
}

func (c RoaringCursor) Next() ([]byte, []byte, [][]byte, *sroar.Bitmap) {
	k, b := c.cursor.Next()
	return k, nil, nil, b
}

func (c RoaringCursor) Seek(b []byte) ([]byte, []byte, [][]byte, *sroar.Bitmap) {
	k, bb := c.cursor.Seek(b)
	return k, nil, nil, bb
}

func (c RoaringCursor) Close() {
	c.cursor.Close()
}

func iteratorConcurrently(ctx context.Context, b *lsmkv.Bucket, newCursor func() Cursor, aggregateFunc func(k []byte, v []byte, vv [][]byte, b *sroar.Bitmap) error, logger logrus.FieldLogger) error {
	// we're looking at the whole object, so this is neither a Set, nor a Map, but
	// a Replace strategy

	seedCount := 2 * runtime.GOMAXPROCS(0)
	seeds := b.QuantileKeys(seedCount)
	// in case all keys are in memory
	if len(seeds) == 0 {
		c := newCursor()
		defer c.Close()
		for k, v, vv, bi := c.First(); k != nil; k, v, vv, bi = c.Next() {
			err := aggregateFunc(k, v, vv, bi)
			if err != nil {
				return err
			}
		}
		return nil
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	eg, newContext := enterrors.NewErrorGroupWithContextWrapper(logger, ctx)

	// There are three scenarios:
	// 1. Read from beginning to first checkpoint
	// 2. Read from checkpoint n to checkpoint n+1
	// 3. Read from last checkpoint to end

	// S1: Read from beginning to first checkpoint:
	eg.Go(func() error {
		c := newCursor()
		defer c.Close()

		count := 0
		for k, v, vv, bi := c.First(); k != nil && bytes.Compare(k, seeds[0]) < 0; k, v, vv, bi = c.Next() {
			err := aggregateFunc(k, v, vv, bi)
			if err != nil {
				return err
			}
			count++
			if count%contextCheckInterval == 0 && newContext.Err() != nil {
				return newContext.Err()
			}
		}
		return nil
	})

	// S2: Read from checkpoint n to checkpoint n+1, stop at last checkpoint:
	for i := 0; i < len(seeds)-1; i++ {
		start := seeds[i]
		end := seeds[i+1]

		eg.Go(func() error {
			c := newCursor()
			defer c.Close()

			count := 0
			for k, v, vv, bi := c.Seek(start); k != nil && bytes.Compare(k, end) < 0; k, v, vv, bi = c.Next() {
				err := aggregateFunc(k, v, vv, bi)
				if err != nil {
					return err
				}
				count++
				if count%contextCheckInterval == 0 && newContext.Err() != nil {
					return newContext.Err()
				}
			}
			return nil
		})
	}

	// S3: Read from last checkpoint to end:
	eg.Go(func() error {
		c := newCursor()
		defer c.Close()

		count := 0
		for k, v, vv, bi := c.Seek(seeds[len(seeds)-1]); k != nil; k, v, vv, bi = c.Next() {
			err := aggregateFunc(k, v, vv, bi)
			if err != nil {
				return err
			}
			count++
			if count%contextCheckInterval == 0 && newContext.Err() != nil {
				return newContext.Err()
			}

		}
		return nil
	})

	return eg.Wait()
}

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

package aggregator

import (
	"bytes"
	"runtime"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

func iteratorConcurrentlyReplace(b *lsmkv.Bucket, aggregateFunc func(b []byte) error, logger logrus.FieldLogger) error {
	// we're looking at the whole object, so this is neither a Set, nor a Map, but
	// a Replace strategy

	seedCount := 2*runtime.GOMAXPROCS(0) - 1
	seeds := b.QuantileKeys(seedCount)
	// in case all keys are in memory
	if len(seeds) == 0 {
		c := b.Cursor()
		defer c.Close()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			err := aggregateFunc(v)
			if err != nil {
				return err
			}
		}
		return nil
	}

	eg := enterrors.NewErrorGroupWrapper(logger)

	// There are three scenarios:
	// 1. Read from beginning to first checkpoint
	// 2. Read from checkpoint n to checkpoint n+1
	// 3. Read from last checkpoint to end

	// S1: Read from beginning to first checkpoint:
	eg.Go(func() error {
		c := b.Cursor()
		defer c.Close()

		for k, v := c.First(); k != nil && bytes.Compare(k, seeds[0]) < 0; k, v = c.Next() {
			err := aggregateFunc(v)
			if err != nil {
				return err
			}
		}
		return nil
	})

	// S2: Read from checkpoint n to checkpoint n+1, stop at last checkpoint:
	for i := 0; i < len(seeds)-1; i++ {
		start := seeds[i]
		end := seeds[i+1]

		eg.Go(func() error {
			c := b.Cursor()
			defer c.Close()

			for k, v := c.Seek(start); k != nil && bytes.Compare(k, end) < 0; k, v = c.Next() {
				err := aggregateFunc(v)
				if err != nil {
					return err
				}
			}
			return nil
		})
	}

	// S3: Read from last checkpoint to end:
	enterrors.GoWrapper(func() {
		c := b.Cursor()
		defer c.Close()

		for k, v := c.Seek(seeds[len(seeds)-1]); k != nil; k, v = c.Next() {
			err := aggregateFunc(v)
			if err != nil {
				return
			}
		}
	}, logger)

	return eg.Wait()
}

func iteratorConcurrentlySet(b *lsmkv.Bucket, aggregateFunc func(k []byte, v [][]byte) error, logger logrus.FieldLogger) error {
	seedCount := 2*runtime.GOMAXPROCS(0) - 1
	seeds := b.QuantileKeys(seedCount)
	// in case all keys are in memory
	if len(seeds) == 0 {
		c := b.SetCursor()
		defer c.Close()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			err := aggregateFunc(k, v)
			if err != nil {
				return err
			}
		}
		return nil
	}

	eg := enterrors.NewErrorGroupWrapper(logger)

	// There are three scenarios:
	// 1. Read from beginning to first checkpoint
	// 2. Read from checkpoint n to checkpoint n+1
	// 3. Read from last checkpoint to end

	// S1: Read from beginning to first checkpoint:
	eg.Go(func() error {
		c := b.SetCursor()
		defer c.Close()

		for k, v := c.First(); k != nil && bytes.Compare(k, seeds[0]) < 0; k, v = c.Next() {
			err := aggregateFunc(k, v)
			if err != nil {
				return err
			}
		}
		return nil
	})

	// S2: Read from checkpoint n to checkpoint n+1, stop at last checkpoint:
	for i := 0; i < len(seeds)-1; i++ {
		start := seeds[i]
		end := seeds[i+1]

		eg.Go(func() error {
			c := b.SetCursor()
			defer c.Close()

			for k, v := c.Seek(start); k != nil && bytes.Compare(k, end) < 0; k, v = c.Next() {
				err := aggregateFunc(k, v)
				if err != nil {
					return err
				}
			}
			return nil
		})
	}

	// S3: Read from last checkpoint to end:
	enterrors.GoWrapper(func() {
		c := b.SetCursor()
		defer c.Close()

		for k, v := c.Seek(seeds[len(seeds)-1]); k != nil; k, v = c.Next() {
			err := aggregateFunc(k, v)
			if err != nil {
				return
			}
		}
	}, logger)

	return eg.Wait()
}

func iteratorConcurrentlyRoaringSet(b *lsmkv.Bucket, aggregateFunc func(k []byte, v *sroar.Bitmap) error, logger logrus.FieldLogger) error {
	seedCount := 2*runtime.GOMAXPROCS(0) - 1
	seeds := b.QuantileKeys(seedCount)
	// in case all keys are in memory
	if len(seeds) == 0 {
		c := b.CursorRoaringSet()
		defer c.Close()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			err := aggregateFunc(k, v)
			if err != nil {
				return err
			}
		}
		return nil
	}

	eg := enterrors.NewErrorGroupWrapper(logger)

	// There are three scenarios:
	// 1. Read from beginning to first checkpoint
	// 2. Read from checkpoint n to checkpoint n+1
	// 3. Read from last checkpoint to end

	// S1: Read from beginning to first checkpoint:
	eg.Go(func() error {
		c := b.CursorRoaringSet()
		defer c.Close()

		for k, v := c.First(); k != nil && bytes.Compare(k, seeds[0]) < 0; k, v = c.Next() {
			err := aggregateFunc(k, v)
			if err != nil {
				return err
			}
		}
		return nil
	})

	// S2: Read from checkpoint n to checkpoint n+1, stop at last checkpoint:
	for i := 0; i < len(seeds)-1; i++ {
		start := seeds[i]
		end := seeds[i+1]

		eg.Go(func() error {
			c := b.CursorRoaringSet()
			defer c.Close()

			for k, v := c.Seek(start); k != nil && bytes.Compare(k, end) < 0; k, v = c.Next() {
				err := aggregateFunc(k, v)
				if err != nil {
					return err
				}
			}
			return nil
		})
	}

	// S3: Read from last checkpoint to end:
	enterrors.GoWrapper(func() {
		c := b.CursorRoaringSet()
		defer c.Close()

		for k, v := c.Seek(seeds[len(seeds)-1]); k != nil; k, v = c.Next() {
			err := aggregateFunc(k, v)
			if err != nil {
				return
			}
		}
	}, logger)

	return eg.Wait()
}

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

package inverted

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/filters"
)

// RowReaderFrequency reads one or many row(s) depending on the specified operator
type RowReaderFrequency struct {
	value         []byte
	bucket        *lsmkv.Bucket
	operator      filters.Operator
	keyOnly       bool
	shardVersion  uint16
	bitmapFactory *roaringset.BitmapFactory
}

func NewRowReaderFrequency(bucket *lsmkv.Bucket, value []byte,
	operator filters.Operator, keyOnly bool, shardVersion uint16,
	bitmapFactory *roaringset.BitmapFactory,
) *RowReaderFrequency {
	return &RowReaderFrequency{
		bucket:        bucket,
		value:         value,
		operator:      operator,
		keyOnly:       keyOnly,
		shardVersion:  shardVersion,
		bitmapFactory: bitmapFactory,
	}
}

func (rr *RowReaderFrequency) Read(ctx context.Context, readFn ReadFn) error {
	switch rr.operator {
	case filters.OperatorEqual:
		return rr.equal(ctx, readFn)
	case filters.OperatorNotEqual:
		return rr.notEqual(ctx, readFn)
	case filters.OperatorGreaterThan:
		return rr.greaterThan(ctx, readFn, false)
	case filters.OperatorGreaterThanEqual:
		return rr.greaterThan(ctx, readFn, true)
	case filters.OperatorLessThan:
		return rr.lessThan(ctx, readFn, false)
	case filters.OperatorLessThanEqual:
		return rr.lessThan(ctx, readFn, true)
	case filters.OperatorLike:
		return rr.like(ctx, readFn)
	default:
		return fmt.Errorf("operator %v supported", rr.operator)
	}
}

// equal is a special case, as we don't need to iterate, but just read a single
// row
func (rr *RowReaderFrequency) equal(ctx context.Context, readFn ReadFn) error {
	v, err := rr.equalHelper(ctx)
	if err != nil {
		return err
	}

	_, err = readFn(rr.value, rr.transformToBitmap(v), noopRelease)
	return err
}

func (rr *RowReaderFrequency) notEqual(ctx context.Context, readFn ReadFn) error {
	v, err := rr.equalHelper(ctx)
	if err != nil {
		return err
	}

	// Invert the Equal results for an efficient NotEqual
	inverted, release := rr.bitmapFactory.GetBitmap()
	inverted.AndNotConc(rr.transformToBitmap(v), concurrency.SROAR_MERGE)
	_, err = readFn(rr.value, inverted, release)
	return err
}

// greaterThan reads from the specified value to the end. The first row is only
// included if allowEqual==true, otherwise it starts with the next one
func (rr *RowReaderFrequency) greaterThan(ctx context.Context, readFn ReadFn,
	allowEqual bool,
) error {
	c := rr.newCursor()
	defer c.Close()

	for k, v := c.Seek(ctx, rr.value); k != nil; k, v = c.Next(ctx) {
		if err := ctx.Err(); err != nil {
			return err
		}

		if bytes.Equal(k, rr.value) && !allowEqual {
			continue
		}

		continueReading, err := readFn(k, rr.transformToBitmap(v), noopRelease)
		if err != nil {
			return err
		}

		if !continueReading {
			break
		}
	}

	return nil
}

// lessThan reads from the very begging to the specified  value. The last
// matching row is only included if allowEqual==true, otherwise it ends one
// prior to that.
func (rr *RowReaderFrequency) lessThan(ctx context.Context, readFn ReadFn,
	allowEqual bool,
) error {
	c := rr.newCursor()
	defer c.Close()

	for k, v := c.First(ctx); k != nil && bytes.Compare(k, rr.value) != 1; k, v = c.Next(ctx) {
		if err := ctx.Err(); err != nil {
			return err
		}

		if bytes.Equal(k, rr.value) && !allowEqual {
			continue
		}

		continueReading, err := readFn(k, rr.transformToBitmap(v), noopRelease)
		if err != nil {
			return err
		}

		if !continueReading {
			break
		}
	}

	return nil
}

func (rr *RowReaderFrequency) like(ctx context.Context, readFn ReadFn) error {
	like, err := parseLikeRegexp(rr.value)
	if err != nil {
		return fmt.Errorf("parse like value: %w", err)
	}

	// TODO: don't we need to check here if this is a doc id vs a object search?
	// Or is this not a problem because the latter removes duplicates anyway?
	c := rr.newCursor(lsmkv.MapListAcceptDuplicates())
	defer c.Close()

	var (
		initialK []byte
		initialV []lsmkv.MapPair
	)

	if like.optimizable {
		initialK, initialV = c.Seek(ctx, like.min)
	} else {
		initialK, initialV = c.First(ctx)
	}

	for k, v := initialK, initialV; k != nil; k, v = c.Next(ctx) {
		if err := ctx.Err(); err != nil {
			return err
		}

		if like.optimizable {
			// if the query is optimizable, i.e. it doesn't start with a wildcard, we
			// can abort once we've moved past the point where the fixed characters
			// no longer match
			if len(k) < len(like.min) {
				break
			}

			if bytes.Compare(like.min, k[:len(like.min)]) == -1 {
				break
			}
		}

		if !like.regexp.Match(k) {
			continue
		}

		continueReading, err := readFn(k, rr.transformToBitmap(v), noopRelease)
		if err != nil {
			return err
		}

		if !continueReading {
			break
		}
	}

	return nil
}

// newCursor will either return a regular cursor - or a key-only cursor if
// keyOnly==true
func (rr *RowReaderFrequency) newCursor(
	opts ...lsmkv.MapListOption,
) *lsmkv.CursorMap {
	if rr.shardVersion < 2 {
		opts = append(opts, lsmkv.MapListLegacySortingRequired())
	}

	if rr.keyOnly {
		return rr.bucket.MapCursorKeyOnly(opts...)
	}

	return rr.bucket.MapCursor(opts...)
}

func (rr *RowReaderFrequency) transformToBitmap(pairs []lsmkv.MapPair) *sroar.Bitmap {
	out := sroar.NewBitmap()
	for _, pair := range pairs {
		// this entry has a frequency, but that's only used for bm25, not for
		// pure filtering, so we can ignore it here
		if rr.shardVersion < 2 {
			out.Set(binary.LittleEndian.Uint64(pair.Key))
		} else {
			out.Set(binary.BigEndian.Uint64(pair.Key))
		}
	}
	return out
}

// equalHelper exists, because the Equal and NotEqual operators share this functionality
func (rr *RowReaderFrequency) equalHelper(ctx context.Context) (v []lsmkv.MapPair, err error) {
	if err = ctx.Err(); err != nil {
		return
	}

	if rr.shardVersion < 2 {
		v, err = rr.bucket.MapList(ctx, rr.value, lsmkv.MapListAcceptDuplicates(),
			lsmkv.MapListLegacySortingRequired())
		if err != nil {
			return
		}
	} else {
		v, err = rr.bucket.MapList(ctx, rr.value, lsmkv.MapListAcceptDuplicates())
		if err != nil {
			return
		}
	}
	return
}

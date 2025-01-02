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

// RowReader reads one or many row(s) depending on the specified operator
type RowReader struct {
	value         []byte
	bucket        *lsmkv.Bucket
	operator      filters.Operator
	keyOnly       bool
	bitmapFactory *roaringset.BitmapFactory
}

// If keyOnly is set, the RowReader will request key-only cursors wherever
// cursors are used, the specified value arguments in the ReadFn will always be
// nil
func NewRowReader(bucket *lsmkv.Bucket, value []byte, operator filters.Operator,
	keyOnly bool, bitmapFactory *roaringset.BitmapFactory,
) *RowReader {
	return &RowReader{
		bucket:        bucket,
		value:         value,
		operator:      operator,
		keyOnly:       keyOnly,
		bitmapFactory: bitmapFactory,
	}
}

// Read a row using the specified ReadFn. If RowReader was created with
// keysOnly==true, the values argument in the readFn will always be nil on all
// requests involving cursors
func (rr *RowReader) Read(ctx context.Context, readFn ReadFn) error {
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
	case filters.OperatorIsNull: // we need to fetch a row with a given value (there is only nil and !nil) and can reuse equal to get the correct row
		return rr.equal(ctx, readFn)
	default:
		return fmt.Errorf("operator %v not supported", rr.operator)
	}
}

// equal is a special case, as we don't need to iterate, but just read a single
// row
func (rr *RowReader) equal(ctx context.Context, readFn ReadFn) error {
	v, err := rr.equalHelper(ctx)
	if err != nil {
		return err
	}

	_, err = readFn(rr.value, rr.transformToBitmap(v), noopRelease)
	return err
}

func (rr *RowReader) notEqual(ctx context.Context, readFn ReadFn) error {
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
func (rr *RowReader) greaterThan(ctx context.Context, readFn ReadFn,
	allowEqual bool,
) error {
	c := rr.newCursor()
	defer c.Close()

	for k, v := c.Seek(rr.value); k != nil; k, v = c.Next() {
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
func (rr *RowReader) lessThan(ctx context.Context, readFn ReadFn,
	allowEqual bool,
) error {
	c := rr.newCursor()
	defer c.Close()

	for k, v := c.First(); k != nil && bytes.Compare(k, rr.value) != 1; k, v = c.Next() {
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

func (rr *RowReader) like(ctx context.Context, readFn ReadFn) error {
	like, err := parseLikeRegexp(rr.value)
	if err != nil {
		return fmt.Errorf("parse like value: %w", err)
	}

	c := rr.newCursor()
	defer c.Close()

	var (
		initialK []byte
		initialV [][]byte
	)

	if like.optimizable {
		initialK, initialV = c.Seek(like.min)
	} else {
		initialK, initialV = c.First()
	}

	for k, v := initialK, initialV; k != nil; k, v = c.Next() {
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
func (rr *RowReader) newCursor() *lsmkv.CursorSet {
	if rr.keyOnly {
		return rr.bucket.SetCursorKeyOnly()
	}

	return rr.bucket.SetCursor()
}

func (rr *RowReader) transformToBitmap(ids [][]byte) *sroar.Bitmap {
	out := sroar.NewBitmap()
	for _, asBytes := range ids {
		out.Set(binary.LittleEndian.Uint64(asBytes))
	}
	return out
}

// equalHelper exists, because the Equal and NotEqual operators share this functionality
func (rr *RowReader) equalHelper(ctx context.Context) ([][]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	v, err := rr.bucket.SetList(rr.value)
	if err != nil {
		return nil, err
	}
	return v, nil
}

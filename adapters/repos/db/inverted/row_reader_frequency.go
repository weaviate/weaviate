//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package inverted

import (
	"bytes"
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/lsmkv"
	"github.com/semi-technologies/weaviate/adapters/repos/db/notimplemented"
	"github.com/semi-technologies/weaviate/entities/filters"
)

// RowReaderFrequency reads one or many row(s) depending on the specified operator
type RowReaderFrequency struct {
	value    []byte
	bucket   *lsmkv.Bucket
	operator filters.Operator
}

func NewRowReaderFrequency(bucket *lsmkv.Bucket, value []byte,
	operator filters.Operator) *RowReaderFrequency {
	return &RowReaderFrequency{
		bucket:   bucket,
		value:    value,
		operator: operator,
	}
}

// ReadFnFrequency will be called 1..n times per match. This means it will also be
// called on a non-match, in this case v == nil.
// It is up to the caller to decide if that is an error case or not.
//
// Note that because what we are parsing is an inverted index row, it can
// sometimes become confusing what a key and value actually resembles. The
// variables k and v are the literal row key and value. So this means, the
// data-value as in "less than 17" where 17 would be the "value" is in the key
// variable "k". The value will contain the docCount, hash and list of pointers
// (with optional frequency) to the docIDs
//
// The boolean return argument is a way to stop iteration (e.g. when a limit is
// reached) without producing an error. In normal operation always return true,
// if false is returned once, the loop is broken.
type ReadFnFrequency func(k []byte, values []lsmkv.MapPair) (bool, error)

func (rr *RowReaderFrequency) Read(ctx context.Context, readFn ReadFnFrequency) error {
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
		return fmt.Errorf("operator not supported in standalone "+
			"mode, see %s for details", notimplemented.Link)
	}
}

// equal is a special case, as we don't need to iterate, but just read a single
// row
func (rr *RowReaderFrequency) equal(ctx context.Context, readFn ReadFnFrequency) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	v, err := rr.bucket.MapList(rr.value, lsmkv.MapListAcceptDeleted(),
		lsmkv.MapListAcceptDuplicates())
	if err != nil {
		return err
	}

	_, err = readFn(rr.value, v)
	return err
}

// greaterThan reads from the specified value to the end. The first row is only
// included if allowEqual==true, otherwise it starts with the next one
func (rr *RowReaderFrequency) greaterThan(ctx context.Context, readFn ReadFnFrequency,
	allowEqual bool) error {
	c := rr.bucket.MapCursor()
	defer c.Close()

	for k, v := c.Seek(rr.value); k != nil; k, v = c.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}

		if bytes.Equal(k, rr.value) && !allowEqual {
			continue
		}

		continueReading, err := readFn(k, v)
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
func (rr *RowReaderFrequency) lessThan(ctx context.Context, readFn ReadFnFrequency,
	allowEqual bool) error {
	c := rr.bucket.MapCursor()
	defer c.Close()

	for k, v := c.First(); k != nil && bytes.Compare(k, rr.value) != 1; k, v = c.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}

		if bytes.Equal(k, rr.value) && !allowEqual {
			continue
		}

		continueReading, err := readFn(k, v)
		if err != nil {
			return err
		}

		if !continueReading {
			break
		}
	}

	return nil
}

// notEqual is another special case, as it's the opposite of equal. So instead
// of reading just one row, we read all but one row.
func (rr *RowReaderFrequency) notEqual(ctx context.Context, readFn ReadFnFrequency) error {
	c := rr.bucket.MapCursor()
	defer c.Close()

	for k, v := c.First(); k != nil; k, v = c.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}

		if bytes.Equal(k, rr.value) {
			continue
		}

		continueReading, err := readFn(k, v)
		if err != nil {
			return err
		}

		if !continueReading {
			break
		}
	}

	return nil
}

func (rr *RowReaderFrequency) like(ctx context.Context, readFn ReadFnFrequency) error {
	like, err := parseLikeRegexp(rr.value)
	if err != nil {
		return errors.Wrapf(err, "parse like value")
	}

	c := rr.bucket.MapCursor()
	defer c.Close()

	var (
		initialK []byte
		initialV []lsmkv.MapPair
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

		continueReading, err := readFn(k, v)
		if err != nil {
			return err
		}

		if !continueReading {
			break
		}
	}

	return nil
}

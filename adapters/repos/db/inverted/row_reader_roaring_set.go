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

package inverted

import (
	"bytes"
	"context"
	"fmt"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/filters"
)

// RowReaderRoaringSet reads one or many row(s) depending on the specified
// operator
type RowReaderRoaringSet struct {
	value      []byte
	operator   filters.Operator
	newCursor  func() lsmkv.CursorRoaringSet
	getter     func(key []byte) (*sroar.Bitmap, func(), error)
	isDenyList bool
	// keyPrefix, when non-nil, restricts cursor-based reads to keys that start
	// with this prefix. Used for nested property buckets where multiple
	// sub-properties share a single bucket, keyed by hash8(path)+encodedValue.
	keyPrefix []byte
}

// If keyOnly is set, the RowReaderRoaringSet will request key-only cursors
// wherever cursors are used, the specified value arguments in the
// ReadFn will always be empty
func NewRowReaderRoaringSet(bucket *lsmkv.Bucket, value []byte, operator filters.Operator,
	keyOnly bool,
) *RowReaderRoaringSet {
	getter := bucket.RoaringSetGet
	newCursor := bucket.CursorRoaringSet
	if keyOnly {
		newCursor = bucket.CursorRoaringSetKeyOnly
	}

	return &RowReaderRoaringSet{
		value:     value,
		operator:  operator,
		newCursor: newCursor,
		getter:    getter,
	}
}

// NewRowReaderRoaringSetWithPrefix creates a RowReaderRoaringSet that restricts
// cursor-based reads to keys beginning with keyPrefix. value must already
// include keyPrefix as a leading byte sequence. Used for nested property
// buckets where the key format is keyPrefix+encodedValue.
func NewRowReaderRoaringSetWithPrefix(bucket *lsmkv.Bucket, value []byte,
	operator filters.Operator, keyOnly bool, keyPrefix []byte,
) *RowReaderRoaringSet {
	rr := NewRowReaderRoaringSet(bucket, value, operator, keyOnly)
	rr.keyPrefix = keyPrefix
	return rr
}

// inPrefix reports whether k starts with rr.keyPrefix. Always true when no
// prefix is set (standard flat-property behaviour).
func (rr *RowReaderRoaringSet) inPrefix(k []byte) bool {
	return len(rr.keyPrefix) == 0 || bytes.HasPrefix(k, rr.keyPrefix)
}

// fullKey returns keyPrefix+value. When no prefix is set it returns value
// directly to avoid an allocation on the common flat-property path.
func (rr *RowReaderRoaringSet) fullKey() []byte {
	if len(rr.keyPrefix) == 0 {
		return rr.value
	}
	return append(rr.keyPrefix, rr.value...)
}

// bareKey strips keyPrefix from k, returning just the value portion.
// For flat properties (no prefix) this is a no-op.
func (rr *RowReaderRoaringSet) bareKey(k []byte) []byte {
	return k[len(rr.keyPrefix):]
}

// ctxExpired reports whether ctx has been cancelled or timed out. It uses a
// non-blocking channel receive, which avoids the mutex acquisition that
// ctx.Err() performs on every call — important for tight cursor loops.
func ctxExpired(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

// ReadFn will be called 1..n times per match. This means it will also
// be called on a non-match, in this case v == empty bitmap.
// It is up to the caller to decide if that is an error case or not.
//
// Note that because what we are parsing is an inverted index row, it can
// sometimes become confusing what a key and value actually resembles. The
// variables k and v are the literal row key and value. So this means, the
// data-value as in "less than 17" where 17 would be the "value" is in the key
// variable "k". The value will contain bitmap with docIDs having value "k"
//
// The boolean return argument is a way to stop iteration (e.g. when a limit is
// reached) without producing an error. In normal operation always return true,
// if false is returned once, the loop is broken.
type ReadFn func(k []byte, v *sroar.Bitmap, release func()) (bool, error)

// Read a row using the specified ReadFn. If RowReader was created with
// keysOnly==true, the values argument in the readFn will always be nil on all
// requests involving cursors
func (rr *RowReaderRoaringSet) Read(ctx context.Context, readFn ReadFn) error {
	switch rr.operator {
	case filters.OperatorEqual, filters.OperatorIsNull:
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
		return fmt.Errorf("operator %v not supported", rr.operator)
	}
}

// equal is a special case, as we don't need to iterate, but just read a single
// row
func (rr *RowReaderRoaringSet) equal(ctx context.Context,
	readFn ReadFn,
) error {
	if err := ctxExpired(ctx); err != nil {
		return err
	}

	v, release, err := rr.getter(rr.fullKey())
	if err != nil {
		return err
	}

	_, err = readFn(rr.value, v, release)
	return err
}

func (rr *RowReaderRoaringSet) notEqual(ctx context.Context,
	readFn ReadFn,
) error {
	rr.isDenyList = true
	return rr.equal(ctx, readFn)
}

// greaterThan reads from the specified value to the end. The first row is only
// included if allowEqual==true, otherwise it starts with the next one
func (rr *RowReaderRoaringSet) greaterThan(ctx context.Context,
	readFn ReadFn, allowEqual bool,
) error {
	c := rr.newCursor()
	defer c.Close()

	fk := rr.fullKey()
	for k, v := c.Seek(fk); k != nil && rr.inPrefix(k); k, v = c.Next() {
		if err := ctxExpired(ctx); err != nil {
			return err
		}

		if bytes.Equal(k, fk) && !allowEqual {
			continue
		}

		if continueReading, err := readFn(rr.bareKey(k), v, noopRelease); err != nil {
			return err
		} else if !continueReading {
			break
		}
	}

	return nil
}

// lessThan reads from the very beginning (or from the keyPrefix boundary if
// set) to the specified value. The last matching row is only included if
// allowEqual==true, otherwise it ends one prior to that.
func (rr *RowReaderRoaringSet) lessThan(ctx context.Context,
	readFn ReadFn, allowEqual bool,
) error {
	c := rr.newCursor()
	defer c.Close()

	// When a prefix is set, start at the prefix boundary rather than the very
	// first key in the bucket, so we don't read unrelated sub-properties.
	var initialK []byte
	var initialV *sroar.Bitmap
	if len(rr.keyPrefix) > 0 {
		initialK, initialV = c.Seek(rr.keyPrefix)
	} else {
		initialK, initialV = c.First()
	}

	fk := rr.fullKey()
	cmpLimit := 0 // lessThan: k < fk → Compare(k,fk) < 0
	if allowEqual {
		cmpLimit = 1 // lessThanEqual: k <= fk → Compare(k,fk) < 1
	}
	// inPrefix is not needed here: Seek(keyPrefix) guarantees no lower-prefix
	// keys appear, and bytes.Compare(k, fk) >= cmpLimit stops the loop before
	// any higher-prefix key, since those sort after fk = keyPrefix+value.
	for k, v := initialK, initialV; k != nil && bytes.Compare(k, fk) < cmpLimit; k, v = c.Next() {
		if err := ctxExpired(ctx); err != nil {
			return err
		}

		if continueReading, err := readFn(rr.bareKey(k), v, noopRelease); err != nil {
			return err
		} else if !continueReading {
			break
		}
	}

	return nil
}

func (rr *RowReaderRoaringSet) like(ctx context.Context,
	readFn ReadFn,
) error {
	// rr.value is always the LIKE pattern string (e.g. "Ber*"), never the full
	// prefixed key. For prefix-bounded reads the cursor is positioned at
	// keyPrefix+like.min and matched against k[len(keyPrefix):].
	like, err := parseLikeRegexp(rr.value)
	if err != nil {
		return fmt.Errorf("parse like value: %w", err)
	}

	c := rr.newCursor()
	defer c.Close()

	var (
		initialK   []byte
		initialV   *sroar.Bitmap
		seekMin    []byte // full seek key used for the optimizable early-abort check
	)

	if like.optimizable {
		// Seek to prefix+like.min so we start at the right position in the bucket.
		seekMin = append(rr.keyPrefix, like.min...)
		initialK, initialV = c.Seek(seekMin)
	} else if len(rr.keyPrefix) > 0 {
		initialK, initialV = c.Seek(rr.keyPrefix)
	} else {
		initialK, initialV = c.First()
	}

	for k, v := initialK, initialV; k != nil && rr.inPrefix(k); k, v = c.Next() {
		if err := ctxExpired(ctx); err != nil {
			return err
		}

		if like.optimizable {
			// Abort once we've moved past the fixed characters of the LIKE pattern.
			if len(k) < len(seekMin) {
				break
			}
			if bytes.Compare(seekMin, k[:len(seekMin)]) == -1 {
				break
			}
		}

		// Match the regex against the value portion of the key only (strip prefix).
		keyValue := k[len(rr.keyPrefix):]
		if !like.regexp.Match(keyValue) {
			continue
		}

		if continueReading, err := readFn(rr.bareKey(k), v, noopRelease); err != nil {
			return err
		} else if !continueReading {
			break
		}
	}

	return nil
}


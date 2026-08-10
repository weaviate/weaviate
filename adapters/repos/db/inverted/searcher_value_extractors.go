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
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	ent "github.com/weaviate/weaviate/entities/inverted"
)

// The put*Key functions below produce each value type's key — delegating to
// the entities/inverted encoders for the numeric families and writing the
// single bool byte directly. Both the extract*Value methods (untyped single
// values, desugared and range paths) and the batched Contains slab encoding
// call them, so the two paths cannot drift.

// putIntKey writes v's 8-byte lexicographically sortable key into dst.
func putIntKey(dst []byte, v int) {
	ent.PutLexicographicallySortableInt64(dst, int64(v))
}

// putNumberKey writes v's 8-byte lexicographically sortable key into dst.
func putNumberKey(dst []byte, v float64) {
	ent.PutLexicographicallySortableFloat64(dst, v)
}

// putBoolKey writes v's single 0/1 key byte into dst, matching the indexed
// representation written by Analyzer.Bool.
func putBoolKey(dst []byte, v bool) {
	dst[0] = 0
	if v {
		dst[0] = 1
	}
}

// putDateTimeKey writes v's 8-byte nanosecond-precision key into dst.
func putDateTimeKey(dst []byte, v time.Time) {
	ent.PutLexicographicallySortableInt64(dst, v.UnixNano())
}

// putDateKey parses v as an RFC3339 date and writes its 8-byte key into dst.
func putDateKey(dst []byte, v string) error {
	parsed, err := time.Parse(time.RFC3339, v)
	if err != nil {
		return errors.Wrap(err, "trying parse time as RFC3339 string")
	}
	putDateTimeKey(dst, parsed)
	return nil
}

// putUUIDKey parses v as a UUID and writes its 16-byte key into dst, matching
// how analyzeValue stores UUID properties. dst must be at least 16 bytes; it
// panics otherwise.
func putUUIDKey(dst []byte, v string) error {
	parsed, err := uuid.Parse(v)
	if err != nil {
		return fmt.Errorf("parse uuid filter value: %w", err)
	}
	_ = dst[15] // bounds check: fail loudly on a short dst rather than truncate
	copy(dst, parsed[:])
	return nil
}

// encodeFixedWidthKeys encodes every already-typed value into its fixed-width
// slot of one shared slab, wrapping the first failure with its position so the
// caller can report which element was malformed.
func encodeFixedWidthKeys[T any](values []T, keyLen int, encode func(dst []byte, v T) error) (ent.SortedKeys, error) {
	kb := ent.NewFixedKeyBuilder(len(values), keyLen)
	for i := range values {
		if err := encode(kb.AppendBuf(), values[i]); err != nil {
			return ent.SortedKeys{}, fmt.Errorf("value %d: %w", i, err)
		}
	}
	// Build orders the encoded keys rather than the values: every fixed-width
	// encoding here is order-preserving, but only for its own type, and a
	// string-valued date or uuid does not sort as its encoding does. Sorting
	// the slab is uniform across all of them and moves only bytes, not slice
	// headers.
	//
	// Every error it can return is [ent.ErrInternal] — the encoders have already
	// run, so no filter value reaches it — which is what lets the caller report
	// an internal fault as one.
	return kb.Build()
}

// encode*Keys below are the slice counterparts of the extract*Value
// methods: one key per value, all keys backed by one shared slab.

func encodeIntKeys(values []int) (ent.SortedKeys, error) {
	return encodeFixedWidthKeys(values, 8, func(dst []byte, v int) error {
		putIntKey(dst, v)
		return nil
	})
}

func encodeNumberKeys(values []float64) (ent.SortedKeys, error) {
	return encodeFixedWidthKeys(values, 8, func(dst []byte, v float64) error {
		putNumberKey(dst, v)
		return nil
	})
}

func encodeBoolKeys(values []bool) (ent.SortedKeys, error) {
	return encodeFixedWidthKeys(values, 1, func(dst []byte, v bool) error {
		putBoolKey(dst, v)
		return nil
	})
}

func encodeDateKeys(values []string) (ent.SortedKeys, error) {
	return encodeFixedWidthKeys(values, 8, func(dst []byte, v string) error {
		return putDateKey(dst, v)
	})
}

func encodeUUIDKeys(values []string) (ent.SortedKeys, error) {
	return encodeFixedWidthKeys(values, 16, func(dst []byte, v string) error {
		return putUUIDKey(dst, v)
	})
}

func (s *Searcher) extractNumberValue(in interface{}) ([]byte, error) {
	value, ok := in.(float64)
	if !ok {
		return nil, fmt.Errorf("expected value to be float64, got %T", in)
	}

	out := make([]byte, 8)
	putNumberKey(out, value)
	return out, nil
}

// assumes an untyped int and stores as string-formatted int64
func (s *Searcher) extractIntValue(in interface{}) ([]byte, error) {
	value, ok := in.(int)
	if !ok {
		return nil, fmt.Errorf("expected value to be int, got %T", in)
	}

	out := make([]byte, 8)
	putIntKey(out, value)
	return out, nil
}

// assumes an untyped int and stores as string-formatted int64
func (s *Searcher) extractIntCountValue(in interface{}) ([]byte, error) {
	value, ok := in.(int)
	if !ok {
		return nil, fmt.Errorf("expected value to be int, got %T", in)
	}

	return ent.LexicographicallySortableUint64(uint64(value))
}

// assumes an untyped bool and stores as a single 0/1 byte, matching the
// indexed representation written by Analyzer.Bool
func (s *Searcher) extractBoolValue(in interface{}) ([]byte, error) {
	value, ok := in.(bool)
	if !ok {
		return nil, fmt.Errorf("expected value to be bool, got %T", in)
	}

	out := make([]byte, 1)
	putBoolKey(out, value)
	return out, nil
}

// assumes a time.Time date and stores as string-formatted int64, if it
// encounters a string it tries to parse it as a time.Time
func (s *Searcher) extractDateValue(in interface{}) ([]byte, error) {
	out := make([]byte, 8)

	switch t := in.(type) {
	case string:
		if err := putDateKey(out, t); err != nil {
			return nil, err
		}

	case time.Time:
		putDateTimeKey(out, t)

	default:
		return nil, fmt.Errorf("expected value to be time.Time (or parseable string)"+
			", got %T", in)
	}

	return out, nil
}

// extractUUIDValue parses a UUID string filter value and returns its 16-byte
// representation.
func (s *Searcher) extractUUIDValue(in interface{}) ([]byte, error) {
	asStr, ok := in.(string)
	if !ok {
		return nil, fmt.Errorf("expected uuid filter value to be a string, got %T", in)
	}
	out := make([]byte, 16)
	if err := putUUIDKey(out, asStr); err != nil {
		return nil, err
	}
	return out, nil
}

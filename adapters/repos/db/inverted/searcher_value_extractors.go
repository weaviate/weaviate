//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package inverted

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/pkg/errors"
)

func (s *Searcher) extractNumberValue(in interface{}) ([]byte, error) {
	val, err := s.getFloat64FromValueNumber(in)
	if err != nil {
		return nil, err
	}
	return LexicographicallySortableFloat64(val)
}

// assumes an untyped int and stores as string-formatted int64
func (s *Searcher) extractInt64Value(in interface{}) ([]byte, error) {
	val, err := s.getInt64FromValueInt(in)
	if err != nil {
		return nil, err
	}
	return LexicographicallySortableInt64(val)
}

// assumes an untyped int and stores as string-formatted int64
func (s *Searcher) extractInt64CountValue(in interface{}) ([]byte, error) {
	val, err := s.getInt64FromValueInt(in)
	if err != nil {
		return nil, err
	}
	return LexicographicallySortableUint64(uint64(val))
}

// assumes an untyped bool and stores as bool64
func (s *Searcher) extractBoolValue(in interface{}) ([]byte, error) {
	value, err := s.getBoolFromValueBoolean(in)
	if err != nil {
		return nil, err
	}

	buf := bytes.NewBuffer(nil)
	if err := binary.Write(buf, binary.LittleEndian, value); err != nil {
		return nil, errors.Wrap(err, "encode bool as binary")
	}

	return buf.Bytes(), nil
}

// assumes a time.Time date and stores as string-formatted int64, if it
// encounters a string it tries to parse it as a time.Time
func (s *Searcher) extractDateValue(in interface{}) ([]byte, error) {
	var asInt64 int64

	switch t := in.(type) {
	case []string:
		if len(t) > 1 {
			return nil, fmt.Errorf("expected only one string value, got %v values", len(t))
		}

		parsed, err := time.Parse(time.RFC3339, t[0])
		if err != nil {
			return nil, errors.Wrap(err, "trying parse time as RFC3339 string")
		}

		asInt64 = parsed.UnixNano()

	case string:
		parsed, err := time.Parse(time.RFC3339, t)
		if err != nil {
			return nil, errors.Wrap(err, "trying parse time as RFC3339 string")
		}

		asInt64 = parsed.UnixNano()

	case time.Time:
		asInt64 = t.UnixNano()

	default:
		return nil, fmt.Errorf("expected value to be time.Time (or parseable string)"+
			", got %T", in)
	}

	return LexicographicallySortableInt64(asInt64)
}

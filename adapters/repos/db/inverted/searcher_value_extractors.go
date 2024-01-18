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
	"encoding/binary"
	"fmt"
	"time"

	"github.com/pkg/errors"
)

func (s *Searcher) extractNumberValue(in interface{}) ([]byte, error) {
	value, ok := in.(float64)
	if !ok {
		return nil, fmt.Errorf("expected value to be float64, got %T", in)
	}

	return LexicographicallySortableFloat64(value)
}

// assumes an untyped int and stores as string-formatted int64
func (s *Searcher) extractIntValue(in interface{}) ([]byte, error) {
	value, ok := in.(int)
	if !ok {
		return nil, fmt.Errorf("expected value to be int, got %T", in)
	}

	return LexicographicallySortableInt64(int64(value))
}

// assumes an untyped int and stores as string-formatted int64
func (s *Searcher) extractIntCountValue(in interface{}) ([]byte, error) {
	value, ok := in.(int)
	if !ok {
		return nil, fmt.Errorf("expected value to be int, got %T", in)
	}

	return LexicographicallySortableUint64(uint64(value))
}

// assumes an untyped bool and stores as bool64
func (s *Searcher) extractBoolValue(in interface{}) ([]byte, error) {
	value, ok := in.(bool)
	if !ok {
		return nil, fmt.Errorf("expected value to be bool, got %T", in)
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

//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package inverted

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
)

func (fs Searcher) extractTextValue(in interface{}) ([]byte, error) {
	value, ok := in.(string)
	if !ok {
		return nil, fmt.Errorf("expected value to be string, got %T", in)
	}

	parts := helpers.TokenizeText(value)
	if len(parts) > 1 {
		return nil, fmt.Errorf("expected single search term, got: %v", parts)
	}

	return []byte(parts[0]), nil
}

func (fs Searcher) extractStringValue(in interface{}) ([]byte, error) {
	value, ok := in.(string)
	if !ok {
		return nil, fmt.Errorf("expected value to be string, got %T", in)
	}

	parts := helpers.TokenizeString(value)
	if len(parts) > 1 {
		return nil, fmt.Errorf("expected single search term, got: %v", parts)
	}

	return []byte(parts[0]), nil
}

func (fs Searcher) extractNumberValue(in interface{}) ([]byte, error) {
	value, ok := in.(float64)
	if !ok {
		return nil, fmt.Errorf("expected value to be float64, got %T", in)
	}

	return LexicographicallySortableFloat64(value)
}

// assumes an untyped int and stores as string-formatted int64
func (fs Searcher) extractIntValue(in interface{}) ([]byte, error) {
	value, ok := in.(int)
	if !ok {
		return nil, fmt.Errorf("expected value to be int, got %T", in)
	}

	return LexicographicallySortableInt64(int64(value))
}

// assumes an untyped int and stores as string-formatted int64
func (fs Searcher) extractIntCountValue(in interface{}) ([]byte, error) {
	value, ok := in.(int)
	if !ok {
		return nil, fmt.Errorf("expected value to be int, got %T", in)
	}

	return LexicographicallySortableUint32(uint32(value))
}

// assumes an untyped bool and stores as bool64
func (fs Searcher) extractBoolValue(in interface{}) ([]byte, error) {
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

// assumes a time.Time date and stores as string-formatted int64
func (fs Searcher) extractDateValue(in interface{}) ([]byte, error) {
	value, ok := in.(time.Time)
	if !ok {
		return nil, fmt.Errorf("expected value to be time.Time, got %T", in)
	}

	return LexicographicallySortableInt64(value.UnixNano())
}

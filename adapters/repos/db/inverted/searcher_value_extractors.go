//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package inverted

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"unicode"

	"github.com/pkg/errors"
)

func (fs Searcher) extractTextValue(in interface{}) ([]byte, error) {
	value, ok := in.(string)
	if !ok {
		return nil, fmt.Errorf("expected value to be string, got %T", in)
	}
	parts := strings.FieldsFunc(value, func(c rune) bool {
		return !unicode.IsLetter(c) && !unicode.IsNumber(c)
	})

	// TODO: gh-1150 allow multi search term
	if len(parts) > 1 {
		return nil, fmt.Errorf("only single search term allowed at the moment, got: %v", parts)
	}

	return []byte(parts[0]), nil
}

func (fs Searcher) extractStringValue(in interface{}) ([]byte, error) {
	value, ok := in.(string)
	if !ok {
		return nil, fmt.Errorf("expected value to be string, got %T", in)
	}

	parts := strings.FieldsFunc(value, func(c rune) bool {
		return unicode.IsSpace(c)
	})

	// TODO: gh-1150 allow multi search term
	if len(parts) > 1 {
		return nil, fmt.Errorf("only single search term allowed at the moment, got: %v", parts)
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

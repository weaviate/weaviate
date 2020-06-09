package filtersearcher

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"unicode"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/inverted"
)

func (fs FilterSearcher) extractTextValue(in interface{}) ([]byte, error) {
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

func (fs FilterSearcher) extractStringValue(in interface{}) ([]byte, error) {
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

func (fs FilterSearcher) extractNumberValue(in interface{}) ([]byte, error) {
	value, ok := in.(float64)
	if !ok {
		return nil, fmt.Errorf("expected value to be float64, got %T", in)
	}

	return inverted.LexicographicallySortableFloat64(value)
}

// assumes an untyped int and stores as string-formatted int64
func (fs FilterSearcher) extractIntValue(in interface{}) ([]byte, error) {
	value, ok := in.(int)
	if !ok {
		return nil, fmt.Errorf("expected value to be int, got %T", in)
	}

	return inverted.LexicographicallySortableInt64(int64(value))
}

// assumes an untyped bool and stores as bool64
func (fs FilterSearcher) extractBoolValue(in interface{}) ([]byte, error) {
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

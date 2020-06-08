package filtersearcher

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/pkg/errors"
)

func (fs FilterSearcher) extractTextValue(in interface{}) ([]byte, error) {
	value, ok := in.(string)
	if !ok {
		return nil, fmt.Errorf("expected value to be string, got %T", in)
	}

	return []byte(value), nil
}

func (fs FilterSearcher) extractNumberValue(in interface{}) ([]byte, error) {
	value, ok := in.(float64)
	if !ok {
		return nil, fmt.Errorf("expected value to be float64, got %T", in)
	}

	buf := bytes.NewBuffer(nil)
	if err := binary.Write(buf, binary.LittleEndian, value); err != nil {
		return nil, errors.Wrap(err, "encode float64 as binary")
	}

	return buf.Bytes(), nil
}

// assumes an untyped int and stores as int64
func (fs FilterSearcher) extractIntValue(in interface{}) ([]byte, error) {
	value, ok := in.(int)
	if !ok {
		return nil, fmt.Errorf("expected value to be int, got %T", in)
	}

	asInt64 := int64(value)
	buf := bytes.NewBuffer(nil)
	if err := binary.Write(buf, binary.LittleEndian, asInt64); err != nil {
		return nil, errors.Wrap(err, "encode int as binary")
	}

	return buf.Bytes(), nil
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

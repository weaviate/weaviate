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

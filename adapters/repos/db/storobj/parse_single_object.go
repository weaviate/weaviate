package storobj

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"

	"github.com/pkg/errors"
)

func ParseAndExtractTextProp(data []byte, propName string) (string, bool, error) {
	var version uint8
	r := bytes.NewReader(data)
	le := binary.LittleEndian
	if err := binary.Read(r, le, &version); err != nil {
		return "", false, err
	}

	if version != 1 {
		return "", false, errors.Errorf("unsupported binary marshaller version %d", version)
	}

	io.CopyN(io.Discard, r, discardBytesPreVector)

	var vecLen uint16
	if err := binary.Read(r, le, &vecLen); err != nil {
		return "", false, err
	}

	io.CopyN(io.Discard, r, int64(vecLen*4))

	var classNameLen uint16
	if err := binary.Read(r, le, &classNameLen); err != nil {
		return "", false, err
	}

	io.CopyN(io.Discard, r, int64(classNameLen))

	var propsLen uint32
	if err := binary.Read(r, le, &propsLen); err != nil {
		return "", false, err
	}

	start := int64(1 + discardBytesPreVector + 2 + 4*vecLen + 2 + classNameLen + 4)
	end := start + int64(propsLen)

	// propsBytes := make([]byte, propsLen)
	// if _, err := r.Read(propsBytes); err != nil {
	// 	return "", false, err
	// }
	propsBytes := data[start:end]
	// _ = propsBytes
	// return "", false, nil

	var propsMap map[string]interface{}
	// if err := json.Unmarshal(propsBytes, &propsMap); err != nil {
	// 	return "", false, err
	// }

	if err := json.NewDecoder(bytes.NewReader(propsBytes)).Decode(&propsMap); err != nil {
		return "", false, err
	}

	prop, ok := propsMap[propName]
	if !ok {
		return "", false, nil
	}

	return prop.(string), true, nil
}

const discardBytesPreVector = 8 + 1 + 16 + 8 + 8

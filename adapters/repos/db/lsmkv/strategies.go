package lsmkv

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/pkg/errors"
)

const (
	// StrategyReplace allows for idem-potent PUT where the latest takes presence
	StrategyReplace       = "replace"
	StrategySetCollection = "setcollection"
	StrategyMapCollection = "mapcollection"
)

type SegmentStrategy uint16

const (
	SegmentStrategyReplace SegmentStrategy = iota
	SegmentStrategySetCollection
	SegmentStrategyMapCollection
)

func SegmentStrategyFromString(in string) SegmentStrategy {
	switch in {
	case StrategyReplace:
		return SegmentStrategyReplace
	case StrategySetCollection:
		return SegmentStrategySetCollection
	case StrategyMapCollection:
		return SegmentStrategyMapCollection
	default:
		panic("unsupport strategy")
	}
}

type MapPair struct {
	Key       []byte
	Value     []byte
	Tombstone bool
}

func (kv MapPair) Bytes() ([]byte, error) {
	// make sure the 2 byte length indicators will never overflow:
	if len(kv.Key) >= math.MaxUint16 {
		return nil, errors.Errorf("mapCollection key must be smaller than %d",
			math.MaxUint16)
	}
	keyLen := uint16(len(kv.Key))

	if len(kv.Value) >= math.MaxUint16 {
		return nil, errors.Errorf("mapCollection value must be smaller than %d",
			math.MaxUint16)
	}
	valueLen := uint16(len(kv.Value))

	out := bytes.NewBuffer(nil)

	if err := binary.Write(out, binary.LittleEndian, &keyLen); err != nil {
		return nil, errors.Wrap(err, "write map key length indicator")
	}

	if _, err := out.Write(kv.Key); err != nil {
		return nil, errors.Wrap(err, "write map key")
	}

	if err := binary.Write(out, binary.LittleEndian, &valueLen); err != nil {
		return nil, errors.Wrap(err, "write map value length indicator")
	}

	if _, err := out.Write(kv.Value); err != nil {
		return nil, errors.Wrap(err, "write map value")
	}

	return out.Bytes(), nil
}

func (kv *MapPair) FromBytes(in []byte) error {
	var read int
	var keyLen uint16
	var valueLen uint16
	r := bytes.NewReader(in)

	if err := binary.Read(r, binary.LittleEndian, &keyLen); err != nil {
		return errors.Wrap(err, "read map key length indicator")
	}
	read += 2 // uint16 -> 2 bytes

	kv.Key = make([]byte, keyLen)
	if n, err := r.Read(kv.Key); err != nil {
		return errors.Wrap(err, "read map key")
	} else {
		read += n
	}

	if err := binary.Read(r, binary.LittleEndian, &valueLen); err != nil {
		return errors.Wrap(err, "read map value length indicator")
	}
	read += 2 // uint16 -> 2 bytes

	kv.Value = make([]byte, valueLen)
	if n, err := r.Read(kv.Value); err != nil {
		return errors.Wrap(err, "read map value")
	} else {
		read += n
	}

	if read != len(in) {
		return errors.Errorf("inconsistent map pair: read %d out of %d bytes",
			read, len(in))
	}

	return nil
}

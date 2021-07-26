//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package lsmkv

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/pkg/errors"
)

type mapDecoder struct{}

func newMapDecoder() *mapDecoder {
	return &mapDecoder{}
}

func (m *mapDecoder) Do(in []value) ([]MapPair, error) {
	seenKeys := map[string]uint{}
	kvs := make([]MapPair, len(in))

	for i, pair := range in {
		kv := MapPair{}
		err := kv.FromBytes(pair.value, pair.tombstone)
		if err != nil {
			return nil, err
		}
		kv.Tombstone = pair.tombstone
		kvs[i] = kv
		count := seenKeys[string(kv.Key)]
		seenKeys[string(kv.Key)] = count + 1
	}

	out := make([]MapPair, len(in))
	i := 0
	for _, pair := range kvs {
		count := seenKeys[string(pair.Key)]
		if count != 1 {
			seenKeys[string(pair.Key)] = count - 1
			continue

		}

		if pair.Tombstone {
			continue
		}

		out[i] = pair
		i++
	}

	return out[:i], nil
}

// DoPartial keeps "unused" tombstones
func (m *mapDecoder) DoPartial(in []value) ([]MapPair, error) {
	seenKeys := map[string]uint{}
	kvs := make([]MapPair, len(in))

	for i, pair := range in {
		kv := MapPair{}
		err := kv.FromBytes(pair.value, pair.tombstone)
		if err != nil {
			return nil, err
		}
		kv.Tombstone = pair.tombstone
		kvs[i] = kv
		count := seenKeys[string(kv.Key)]
		seenKeys[string(kv.Key)] = count + 1
	}

	out := make([]MapPair, len(in))
	i := 0
	for _, pair := range kvs {
		count := seenKeys[string(pair.Key)]
		if count != 1 {
			seenKeys[string(pair.Key)] = count - 1
			continue

		}

		out[i] = pair
		i++
	}

	return out[:i], nil
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

func (kv *MapPair) FromBytes(in []byte, keyOnly bool) error {
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

	if keyOnly {
		return nil
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

func (kv *MapPair) FromBytesReusable(in []byte, keyOnly bool) error {
	var read uint16

	keyLen := binary.LittleEndian.Uint16(in[:2])
	read += 2 // uint16 -> 2 bytes

	if int(keyLen) > cap(kv.Key) {
		kv.Key = make([]byte, keyLen)
	} else {
		kv.Key = kv.Key[:keyLen]
	}
	copy(kv.Key, in[read:read+keyLen])
	read += keyLen

	if keyOnly {
		return nil
	}

	valueLen := binary.LittleEndian.Uint16(in[read : read+2])
	read += 2

	if int(valueLen) > cap(kv.Value) {
		kv.Value = make([]byte, valueLen)
	} else {
		kv.Value = kv.Value[:valueLen]
	}
	copy(kv.Value, in[read:read+valueLen])
	read += valueLen

	if read != uint16(len(in)) {
		return errors.Errorf("inconsistent map pair: read %d out of %d bytes",
			read, len(in))
	}

	return nil
}

type mapEncoder struct{}

func newMapEncoder() *mapEncoder {
	return &mapEncoder{}
}

func (m *mapEncoder) Do(kv MapPair) ([]value, error) {
	v, err := kv.Bytes()
	if err != nil {
		return nil, err
	}

	out := make([]value, 1)
	out[0] = value{
		tombstone: kv.Tombstone,
		value:     v,
	}

	return out, nil
}

func (m *mapEncoder) DoMulti(kvs []MapPair) ([]value, error) {
	out := make([]value, len(kvs))

	for i, kv := range kvs {
		v, err := kv.Bytes()
		if err != nil {
			return nil, err
		}

		out[i] = value{
			tombstone: kv.Tombstone,
			value:     v,
		}
	}

	return out, nil
}

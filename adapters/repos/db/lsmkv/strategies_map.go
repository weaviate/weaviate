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

func (m *mapDecoder) Do(in []value, acceptDuplicates bool) ([]MapPair, error) {
	// if acceptDuplicates {
	// 	return m.doSimplified(in)
	// }

	seenKeys := map[string]uint{}
	kvs := make([]MapPair, len(in))

	// unmarshalling := time.Duration(0)

	// beforeFirst := time.Now()
	for i, pair := range in {
		kv := MapPair{}
		// beforeUnmarshal := time.Now()
		err := kv.FromBytes(pair.value, pair.tombstone)
		if err != nil {
			return nil, err
		}
		// unmarshalling += time.Since(beforeUnmarshal)
		kv.Tombstone = pair.tombstone
		kvs[i] = kv
		count := seenKeys[string(kv.Key)]
		seenKeys[string(kv.Key)] = count + 1
	}
	// fmt.Printf("first decoder loop took %s\n", time.Since(beforeFirst))
	// fmt.Printf("unmarshalling in first loop took %s\n", unmarshalling)

	// beforeSecond := time.Now()
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
	// fmt.Printf("second decoder loop took %s\n", time.Since(beforeSecond))

	return out[:i], nil
}

type tombstone struct {
	pos int
	key []byte
}

func (m *mapDecoder) doSimplified(in []value) ([]MapPair, error) {
	out := make([]MapPair, len(in))

	var tombstones []tombstone

	i := 0
	for _, raw := range in {
		if raw.tombstone {
			mp := MapPair{}
			mp.FromBytes(raw.value, true)
			tombstones = append(tombstones, tombstone{pos: i, key: mp.Key})
			continue
		}

		out[i].FromBytes(raw.value, raw.tombstone)
		i++
	}

	out = out[:i]

	if len(tombstones) > 0 {
		out = m.removeTombstonesFromResults(out, tombstones)
	}

	return out, nil
}

func (m *mapDecoder) removeTombstonesFromResults(candidates []MapPair,
	tombstones []tombstone,
) []MapPair {
	after := make([]MapPair, len(candidates))
	newPos := 0
	for origPos, candidate := range candidates {

		skip := false
		for _, tombstone := range tombstones {
			if tombstone.pos > origPos && bytes.Equal(tombstone.key, candidate.Key) {
				skip = true
			}
		}

		if skip {
			continue
		}

		after[newPos] = candidate
		newPos++
	}

	return after[:newPos]
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

// Size() returns the exact size in bytes that will be used when Bytes() is
// called
func (kv MapPair) Size() int {
	// each field uses a uint16 (2 bytes) length indicator
	return 2 + len(kv.Key) + 2 + len(kv.Value)
}

func (kv MapPair) EncodeBytes(buf []byte) error {
	if len(buf) != kv.Size() {
		return errors.Errorf("buffer has size %d, but MapPair has size %d",
			len(buf), kv.Size())
	}

	// make sure the 2 byte length indicators will never overflow:
	if len(kv.Key) >= math.MaxUint16 {
		return errors.Errorf("mapCollection key must be smaller than %d",
			math.MaxUint16)
	}
	keyLen := uint16(len(kv.Key))

	if len(kv.Value) >= math.MaxUint16 {
		return errors.Errorf("mapCollection value must be smaller than %d",
			math.MaxUint16)
	}
	valueLen := uint16(len(kv.Value))

	offset := 0
	binary.LittleEndian.PutUint16(buf[offset:offset+2], keyLen)
	offset += 2
	copy(buf[offset:], kv.Key)
	offset += len(kv.Key)

	binary.LittleEndian.PutUint16(buf[offset:offset+2], valueLen)
	offset += 2
	copy(buf[offset:], kv.Value)

	return nil
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

	lenBuf := make([]byte, 2) // can be reused for both key and value len
	binary.LittleEndian.PutUint16(lenBuf, keyLen)
	if _, err := out.Write(lenBuf); err != nil {
		return nil, errors.Wrap(err, "write map key length indicator")
	}

	if _, err := out.Write(kv.Key); err != nil {
		return nil, errors.Wrap(err, "write map key")
	}

	binary.LittleEndian.PutUint16(lenBuf, valueLen)
	if _, err := out.Write(lenBuf); err != nil {
		return nil, errors.Wrap(err, "write map value length indicator")
	}

	if _, err := out.Write(kv.Value); err != nil {
		return nil, errors.Wrap(err, "write map value")
	}

	return out.Bytes(), nil
}

func (kv *MapPair) FromBytes(in []byte, keyOnly bool) error {
	var read uint16

	// NOTE: A previous implementation was using copy statements in here to avoid
	// sharing the memory. The general idea of that is good (protect against the
	// mmaped memory being removed from a completed compaction), however this is
	// the wrong place. By the time we are in this method, we can no longer
	// control the memory safety of the "in" argument. Thus, such a copy must
	// happen at a much earlier scope when a lock is held that protects against
	// removing the segment. Such an implementation can now be found in
	// segment_collection_strategy.go as part of the *segment.getCollection
	// method. As a result all memory used here can now be considered read-only
	// and is safe to be used indefinitely.

	keyLen := binary.LittleEndian.Uint16(in[:2])
	read += 2 // uint16 -> 2 bytes

	kv.Key = in[read : read+keyLen]
	read += keyLen

	if keyOnly {
		return nil
	}

	valueLen := binary.LittleEndian.Uint16(in[read : read+2])
	read += 2

	kv.Value = in[read : read+valueLen]
	read += valueLen

	if read != uint16(len(in)) {
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

type mapEncoder struct {
	pairBuf []value
}

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
		v := make([]byte, kv.Size())
		err := kv.EncodeBytes(v)
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

// DoMultiReusable reuses a MapPair buffer that it exposes to the caller on
// this request. Warning: The caller must make sure that they no longer access
// the return value once they call this method a second time, otherwise they
// risk overwriting a previous result. The intended usage for example in a loop
// where each loop copies the results, for example using a bufio.Writer.
func (m *mapEncoder) DoMultiReusable(kvs []MapPair) ([]value, error) {
	m.resizeBuffer(len(kvs))

	for i, kv := range kvs {
		m.resizeValueAtBuffer(i, kv.Size())
		err := kv.EncodeBytes(m.pairBuf[i].value)
		if err != nil {
			return nil, err
		}

		m.pairBuf[i].tombstone = kv.Tombstone
	}

	return m.pairBuf, nil
}

func (m *mapEncoder) resizeBuffer(size int) {
	if cap(m.pairBuf) >= size {
		m.pairBuf = m.pairBuf[:size]
	} else {
		m.pairBuf = make([]value, size, int(float64(size)*1.25))
	}
}

func (m *mapEncoder) resizeValueAtBuffer(pos, size int) {
	if cap(m.pairBuf[pos].value) >= size {
		m.pairBuf[pos].value = m.pairBuf[pos].value[:size]
	} else {
		m.pairBuf[pos].value = make([]byte, size, int(float64(size)*1.25))
	}
}

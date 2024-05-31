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
	"math"

	"github.com/pkg/errors"
)

type InvertedPair struct {
	Key       []byte
	Value     []byte
	Tombstone bool
}

// Size() returns the exact size in bytes that will be used when Bytes() is
// called
func (kv InvertedPair) Size() int {
	return len(kv.Key) + len(kv.Value)
}

func (kv InvertedPair) EncodeBytes(buf []byte) error {
	if len(buf) != kv.Size() {
		return errors.Errorf("buffer has size %d, but MapPair has size %d",
			len(buf), kv.Size())
	}

	offset := 0
	copy(buf[offset:], kv.Key)
	offset += len(kv.Key)
	copy(buf[offset:], kv.Value)

	return nil
}

func (kv InvertedPair) Bytes() ([]byte, error) {
	// make sure the 2 byte length indicators will never overflow:
	if len(kv.Key) >= math.MaxUint16 {
		return nil, errors.Errorf("mapCollection key must be smaller than %d",
			math.MaxUint16)
	}

	if len(kv.Value) >= math.MaxUint16 {
		return nil, errors.Errorf("mapCollection value must be smaller than %d",
			math.MaxUint16)
	}

	out := bytes.NewBuffer(nil)

	if _, err := out.Write(kv.Key); err != nil {
		return nil, errors.Wrap(err, "write map key")
	}

	if _, err := out.Write(kv.Value); err != nil {
		return nil, errors.Wrap(err, "write map value")
	}

	return out.Bytes(), nil
}

func (kv *InvertedPair) FromBytes(in []byte, keyOnly bool, invertedKeyLen, invertedValueLen uint16) error {
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

	kv.Key = in[read : read+invertedKeyLen]
	read += invertedKeyLen

	if keyOnly {
		return nil
	}

	kv.Value = in[read : read+invertedValueLen]
	read += invertedValueLen

	if read != uint16(len(in)) {
		return errors.Errorf("inconsistent map pair: read %d out of %d bytes",
			read, len(in))
	}

	return nil
}

func (kv *InvertedPair) FromBytesReusable(in []byte, keyOnly bool, invertedKeyLen, invertedValueLen uint16) error {
	var read uint16

	if int(invertedKeyLen) > cap(kv.Key) {
		kv.Key = make([]byte, invertedKeyLen)
	} else {
		kv.Key = kv.Key[:invertedKeyLen]
	}

	copy(kv.Key, in[read:read+invertedKeyLen])
	read += invertedKeyLen

	if keyOnly {
		return nil
	}
	if int(invertedValueLen) > cap(kv.Value) {
		kv.Value = make([]byte, invertedValueLen)
	} else {
		kv.Value = kv.Value[:invertedValueLen]
	}
	copy(kv.Value, in[read:read+invertedValueLen])
	read += invertedValueLen

	if read != uint16(len(in)) {
		return errors.Errorf("inconsistent map pair: read %d out of %d bytes",
			read, len(in))
	}

	return nil
}

type invertedEncoder struct {
	pairBuf []value
}

func newInvertedEncoder() *invertedEncoder {
	return &invertedEncoder{}
}

func (m *invertedEncoder) Do(kv InvertedPair) ([]value, error) {
	v, err := kv.Bytes()
	if err != nil {
		return nil, err
	}

	out := make([]value, 1)
	out[0] = value{
		value: v,
	}

	return out, nil
}

func (m *invertedEncoder) DoMulti(kvs []InvertedPair) ([]value, error) {
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
func (m *invertedEncoder) DoMultiReusable(kvs []InvertedPair) ([]value, error) {
	m.resizeBuffer(len(kvs))

	for i, kv := range kvs {
		m.resizeValueAtBuffer(i, kv.Size())
		err := kv.EncodeBytes(m.pairBuf[i].value)
		if err != nil {
			return nil, err
		}
	}

	return m.pairBuf, nil
}

func (m *invertedEncoder) resizeBuffer(size int) {
	if cap(m.pairBuf) >= size {
		m.pairBuf = m.pairBuf[:size]
	} else {
		m.pairBuf = make([]value, size, int(float64(size)*1.25))
	}
}

func (m *invertedEncoder) resizeValueAtBuffer(pos, size int) {
	if cap(m.pairBuf[pos].value) >= size {
		m.pairBuf[pos].value = m.pairBuf[pos].value[:size]
	} else {
		m.pairBuf[pos].value = make([]byte, size, int(float64(size)*1.25))
	}
}

func (kv MapPair) BytesInverted() ([]byte, error) {
	// make sure the 2 byte length indicators will never overflow:
	if len(kv.Key) >= math.MaxUint16 {
		return nil, errors.Errorf("mapCollection key must be smaller than %d",
			math.MaxUint16)
	}

	if len(kv.Value) >= math.MaxUint16 {
		return nil, errors.Errorf("mapCollection value must be smaller than %d",
			math.MaxUint16)
	}

	out := bytes.NewBuffer(nil)

	if _, err := out.Write(kv.Key); err != nil {
		return nil, errors.Wrap(err, "write map key")
	}

	if _, err := out.Write(kv.Value); err != nil {
		return nil, errors.Wrap(err, "write map value")
	}

	return out.Bytes(), nil
}

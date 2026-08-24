//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hashtree

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
)

const DigestLength int = 16

type Digest [2]uint64

func (d *Digest) MarshalBinary() ([]byte, error) {
	var bs [DigestLength]byte

	binary.BigEndian.PutUint64(bs[:], d[0])
	binary.BigEndian.PutUint64(bs[8:], d[1])

	return bs[:], nil
}

func (d *Digest) UnmarshalBinary(bs []byte) error {
	if len(bs) != DigestLength {
		return fmt.Errorf("invalid Digest serialization")
	}

	d[0] = binary.BigEndian.Uint64(bs[:])
	d[1] = binary.BigEndian.Uint64(bs[8:])

	return nil
}

func (d *Digest) MarshalJSON() ([]byte, error) {
	b, err := d.MarshalBinary()
	if err != nil {
		return nil, err
	}

	return json.Marshal(b)
}

func (d *Digest) UnmarshalJSON(b []byte) error {
	var bs []byte

	err := json.Unmarshal(b, &bs)
	if err != nil {
		return err
	}

	return d.UnmarshalBinary(bs)
}

// SizeDigests resizes buf to hold n digests, reallocating only when capacity is insufficient.
func SizeDigests(buf []Digest, n int) []Digest {
	if cap(buf) < n {
		return make([]Digest, n)
	}
	return buf[:n]
}

// DigestsToBinary encodes digests as fixed DigestLength big-endian records —
// the shared wire format of the REST binary response and the gRPC encoding=1
// payload.
func DigestsToBinary(digests []Digest) []byte {
	out := make([]byte, 0, len(digests)*DigestLength)
	var buf [DigestLength]byte
	for _, d := range digests {
		binary.BigEndian.PutUint64(buf[:8], d[0])
		binary.BigEndian.PutUint64(buf[8:], d[1])
		out = append(out, buf[:]...)
	}
	return out
}

// DigestsFromBinary decodes a DigestsToBinary payload, rejecting any length
// that is not a whole number of records.
func DigestsFromBinary(data []byte) ([]Digest, error) {
	if len(data)%DigestLength != 0 {
		return nil, fmt.Errorf("invalid digests payload length %d: not a multiple of %d", len(data), DigestLength)
	}
	digests := make([]Digest, len(data)/DigestLength)
	for i := range digests {
		off := i * DigestLength
		digests[i][0] = binary.BigEndian.Uint64(data[off : off+8])
		digests[i][1] = binary.BigEndian.Uint64(data[off+8 : off+DigestLength])
	}
	return digests, nil
}

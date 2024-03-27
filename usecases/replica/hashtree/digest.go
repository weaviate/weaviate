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

//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hashtree

import (
	"encoding/binary"
	"fmt"

	"github.com/square/go-jose/json"
)

type Digest [2]uint64

func (d *Digest) MarshalJSON() ([]byte, error) {
	var b [16]byte

	binary.LittleEndian.PutUint64(b[:], d[0])
	binary.LittleEndian.PutUint64(b[8:], d[1])

	return json.Marshal(b)
}

func (d *Digest) UnmarshalJSON(b []byte) error {
	var bs [16]byte

	err := json.Unmarshal(b, &bs)
	if err != nil {
		return err
	}

	if len(bs) != 16 {
		return fmt.Errorf("invalid Digest serialization")
	}

	d[0] = binary.LittleEndian.Uint64(bs[:])
	d[1] = binary.LittleEndian.Uint64(bs[8:])

	return nil
}

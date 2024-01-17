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
	"io"
)

const (
	segmentedHashtreeVersion      = 1
	segmentedHashTreeHeaderLength = 1 + 8 + 4 // version segmentSize segmentCount
)

func (ht *SegmentedHashTree) Serialize(w io.Writer) (n int64, err error) {
	var hdr [segmentedHashTreeHeaderLength]byte

	hdrOff := 0

	hdr[hdrOff] = segmentedHashtreeVersion
	hdrOff++

	binary.BigEndian.PutUint64(hdr[hdrOff:], uint64(ht.segmentSize))
	hdrOff += 8

	binary.BigEndian.PutUint32(hdr[hdrOff:], uint32(len(ht.segments)))
	hdrOff += 4

	n1, err := w.Write(hdr[:])
	if err != nil {
		return int64(n1), err
	}

	n = int64(n1)

	// write segments
	for _, s := range ht.segments {
		var b [8]byte

		binary.BigEndian.PutUint64(b[:], s)

		nb, err := w.Write(b[:])
		if err != nil {
			return n + int64(nb), err
		}

		n += int64(nb)
	}

	n2, err := ht.hashtree.Serialize(w)
	if err != nil {
		return n + n2, err
	}

	n += n2

	return n, nil
}

func DeserializeSegmentedHashTree(r io.Reader) (*SegmentedHashTree, error) {
	var hdr [segmentedHashTreeHeaderLength]byte

	_, err := r.Read(hdr[:])
	if err != nil {
		return nil, err
	}

	hdrOff := 0

	if hdr[hdrOff] != segmentedHashtreeVersion {
		return nil, fmt.Errorf("unsupported version %d, expected version %d", hdr[0], segmentedHashtreeVersion)
	}
	hdrOff++

	segmentSize := binary.BigEndian.Uint64(hdr[hdrOff:])
	hdrOff += 8

	segmentCount := int(binary.BigEndian.Uint32(hdr[hdrOff:]))
	hdrOff += 4

	segments := make([]uint64, segmentCount)

	for i := 0; i < segmentCount; i++ {
		var b [8]byte

		_, err = r.Read(b[:])
		if err != nil {
			return nil, err
		}

		segments[i] = binary.BigEndian.Uint64(b[:])
	}

	hashtree, err := DeserializeCompactHashTree(r)
	if err != nil {
		return nil, err
	}

	ht := newSegmentedHashTree(segmentSize, segments, hashtree)

	return ht, nil
}

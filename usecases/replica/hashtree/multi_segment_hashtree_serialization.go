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
	multiSegmentHashtreeVersion      = 1
	multiSegmentHashTreeHeaderLength = 1 + 4 // version	segmentCount
)

func (ht *MultiSegmentHashTree) Serialize(w io.Writer) (n int64, err error) {
	var hdr [multiSegmentHashTreeHeaderLength]byte

	hdrOff := 0

	hdr[hdrOff] = multiSegmentHashtreeVersion
	hdrOff++

	binary.BigEndian.PutUint32(hdr[hdrOff:], uint32(len(ht.segments)))
	hdrOff += 4

	n1, err := w.Write(hdr[:])
	if err != nil {
		return int64(n1), err
	}

	n = int64(n1)

	// write segments
	for _, s := range ht.segments {
		segmentBs, err := s.MarshalBinary()
		if err != nil {
			return n, err
		}

		ns, err := w.Write(segmentBs)
		if err != nil {
			return n + int64(ns), err
		}

		n += int64(ns)
	}

	n2, err := ht.hashtree.Serialize(w)
	if err != nil {
		return n + n2, err
	}

	n += n2

	return n, nil
}

func DeserializeMultiSegmentHashTree(r io.Reader) (*MultiSegmentHashTree, error) {
	var hdr [multiSegmentHashTreeHeaderLength]byte

	_, err := r.Read(hdr[:])
	if err != nil {
		return nil, err
	}

	hdrOff := 0

	if hdr[hdrOff] != multiSegmentHashtreeVersion {
		return nil, fmt.Errorf("unsupported version %d, expected version %d", hdr[0], multiSegmentHashtreeVersion)
	}
	hdrOff++

	segmentCount := int(binary.BigEndian.Uint32(hdr[hdrOff:]))
	hdrOff += 4

	segments := make([]Segment, segmentCount)

	for i := 0; i < segmentCount; i++ {
		var segmentBs [SegmentLength]byte

		_, err = r.Read(segmentBs[:])
		if err != nil {
			return nil, fmt.Errorf("reading segment %d: %w", i, err)
		}

		err = segments[i].UnmarshalBinary(segmentBs[:])
		if err != nil {
			return nil, fmt.Errorf("unmarshalling segment %d: %w", i, err)
		}
	}

	hashtree, err := DeserializeCompactHashTree(r)
	if err != nil {
		return nil, err
	}

	ht := newMultiSegmentHashTree(segments, hashtree)

	return ht, nil
}

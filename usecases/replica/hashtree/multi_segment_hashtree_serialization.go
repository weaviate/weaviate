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
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/spaolacci/murmur3"
)

const (
	multiSegmentHashTreeMagicNumber uint32 = 0xD3D3D3D3
	multiSegmentHashtreeVersion     byte   = 1

	// magicnumber version segmentCount segmentsChecksum checksum
	multiSegmentHashTreeHeaderLength int = 4 + 1 + 4 + DigestLength + DigestLength
)

func (ht *MultiSegmentHashTree) Serialize(w io.Writer) (n int64, err error) {
	var hdr [multiSegmentHashTreeHeaderLength]byte

	hdrOff := 0

	binary.BigEndian.PutUint32(hdr[hdrOff:], multiSegmentHashTreeMagicNumber)
	hdrOff += 4

	hdr[hdrOff] = multiSegmentHashtreeVersion
	hdrOff++

	binary.BigEndian.PutUint32(hdr[hdrOff:], uint32(len(ht.segments)))
	hdrOff += 4

	hash := murmur3.New128()

	// write segments checksum
	for _, s := range ht.segments {
		segmentBs, err := s.MarshalBinary()
		if err != nil {
			return 0, err
		}

		_, err = hash.Write(segmentBs)
		if err != nil {
			return 0, err
		}
	}
	copy(hdr[hdrOff:hdrOff+DigestLength], hash.Sum(nil))
	hdrOff += DigestLength

	hash.Reset()
	checksum := hash.Sum(hdr[:hdrOff])
	copy(hdr[hdrOff:hdrOff+DigestLength], checksum)

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

	_, err := io.ReadFull(r, hdr[:])
	if err != nil {
		return nil, err
	}

	hdrOff := 0

	magicNumber := binary.BigEndian.Uint32(hdr[hdrOff:])
	if magicNumber != multiSegmentHashTreeMagicNumber {
		return nil, fmt.Errorf("multi-segment hashtree magic number mismatch")
	}
	hdrOff += 4

	if hdr[hdrOff] != multiSegmentHashtreeVersion {
		return nil, fmt.Errorf("unsupported version %d, expected version %d", hdr[0], multiSegmentHashtreeVersion)
	}
	hdrOff++

	segmentCount := int(binary.BigEndian.Uint32(hdr[hdrOff:]))
	hdrOff += 4

	segmentsChecksum := hdr[hdrOff : hdrOff+DigestLength]
	hdrOff += DigestLength

	hash := murmur3.New128()

	checksum := hash.Sum(hdr[:hdrOff])
	if bytes.Equal(hdr[:hdrOff], checksum) {
		return nil, fmt.Errorf("header checksum mismatch")
	}

	hash.Reset()

	segments := make([]Segment, segmentCount)

	for i := 0; i < segmentCount; i++ {
		var segmentBs [SegmentLength]byte

		_, err = io.ReadFull(r, segmentBs[:])
		if err != nil {
			return nil, fmt.Errorf("reading segment %d: %w", i, err)
		}

		err = segments[i].UnmarshalBinary(segmentBs[:])
		if err != nil {
			return nil, fmt.Errorf("unmarshalling segment %d: %w", i, err)
		}

		_, err = hash.Write(segmentBs[:])
		if err != nil {
			return nil, fmt.Errorf("unmarshalling segment %d: %w", i, err)
		}
	}

	if !bytes.Equal(segmentsChecksum, hash.Sum(nil)) {
		return nil, fmt.Errorf("header checksum mismatch (segments)")
	}

	hashtree, err := DeserializeCompactHashTree(r)
	if err != nil {
		return nil, err
	}

	ht := newMultiSegmentHashTree(segments, hashtree)

	return ht, nil
}

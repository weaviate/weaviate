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
	segmentedHashTreeMagicNumber uint32 = 0xD4D4D4D4
	segmentedHashtreeVersion     byte   = 1

	// magicnumber version segmentSize segmentCount segmentsChecksum checksum
	segmentedHashTreeHeaderLength int = 4 + 1 + 8 + 4 + DigestLength + DigestLength
)

func (ht *SegmentedHashTree) Serialize(w io.Writer) (n int64, err error) {
	var hdr [segmentedHashTreeHeaderLength]byte

	hdrOff := 0

	binary.BigEndian.PutUint32(hdr[hdrOff:], segmentedHashTreeMagicNumber)
	hdrOff += 4

	hdr[hdrOff] = segmentedHashtreeVersion
	hdrOff++

	binary.BigEndian.PutUint64(hdr[hdrOff:], uint64(ht.segmentSize))
	hdrOff += 8

	binary.BigEndian.PutUint32(hdr[hdrOff:], uint32(len(ht.segments)))
	hdrOff += 4

	hash := murmur3.New128()

	// write segments checksum
	for _, s := range ht.segments {
		var b [8]byte

		binary.BigEndian.PutUint64(b[:], s)

		_, err := hash.Write(b[:])
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

	_, err := io.ReadFull(r, hdr[:])
	if err != nil {
		return nil, err
	}

	hdrOff := 0

	magicNumber := binary.BigEndian.Uint32(hdr[hdrOff:])
	if magicNumber != segmentedHashTreeMagicNumber {
		return nil, fmt.Errorf("multi-segment hashtree magic number mismatch")
	}
	hdrOff += 4

	if hdr[hdrOff] != segmentedHashtreeVersion {
		return nil, fmt.Errorf("unsupported version %d, expected version %d", hdr[0], segmentedHashtreeVersion)
	}
	hdrOff++

	segmentSize := binary.BigEndian.Uint64(hdr[hdrOff:])
	hdrOff += 8

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

	segments := make([]uint64, segmentCount)

	for i := 0; i < segmentCount; i++ {
		var b [8]byte

		_, err = io.ReadFull(r, b[:])
		if err != nil {
			return nil, err
		}

		segments[i] = binary.BigEndian.Uint64(b[:])

		_, err = hash.Write(b[:])
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

	return newSegmentedHashTree(segmentSize, segments, hashtree)
}

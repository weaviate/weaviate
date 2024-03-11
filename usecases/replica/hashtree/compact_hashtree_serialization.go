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
	compactHashTreeMagicNumber uint32 = 0xD2D2D2D2
	compactHashtreeVersion     byte   = 1

	// magicnumber version capacity checksum
	compactHashtreeHeaderLength int = 4 + 1 + 8 + DigestLength
)

func (ht *CompactHashTree) Serialize(w io.Writer) (n int64, err error) {
	var hdr [compactHashtreeHeaderLength]byte

	hdrOff := 0

	binary.BigEndian.PutUint32(hdr[hdrOff:], compactHashTreeMagicNumber)
	hdrOff += 4

	hdr[hdrOff] = compactHashtreeVersion
	hdrOff++

	binary.BigEndian.PutUint64(hdr[hdrOff:], uint64(ht.capacity))
	hdrOff += 8

	checksum := murmur3.New128().Sum(hdr[:hdrOff])
	copy(hdr[hdrOff:hdrOff+DigestLength], checksum)

	n1, err := w.Write(hdr[:])
	if err != nil {
		return int64(n1), err
	}

	n = int64(n1)

	n2, err := ht.hashtree.Serialize(w)
	if err != nil {
		return n + n2, err
	}

	n += n2

	return n, nil
}

func DeserializeCompactHashTree(r io.Reader) (*CompactHashTree, error) {
	var hdr [compactHashtreeHeaderLength]byte

	_, err := io.ReadFull(r, hdr[:])
	if err != nil {
		return nil, err
	}

	hdrOff := 0

	magicNumber := binary.BigEndian.Uint32(hdr[hdrOff:])
	if magicNumber != compactHashTreeMagicNumber {
		return nil, fmt.Errorf("compact hashtree magic number mismatch")
	}
	hdrOff += 4

	if hdr[hdrOff] != compactHashtreeVersion {
		return nil, fmt.Errorf("unsupported version %d, expected version %d", hdr[0], compactHashtreeVersion)
	}
	hdrOff++

	capacity := binary.BigEndian.Uint64(hdr[hdrOff:])
	hdrOff += 8

	checksum := murmur3.New128().Sum(hdr[:hdrOff])
	if bytes.Equal(hdr[:hdrOff], checksum) {
		return nil, fmt.Errorf("header checksum mismatch")
	}

	hashtree, err := DeserializeHashTree(r)
	if err != nil {
		return nil, err
	}

	return newCompactHashTree(capacity, hashtree)
}

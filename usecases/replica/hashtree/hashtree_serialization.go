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
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/spaolacci/murmur3"
)

const (
	hashTreeMagicNumber uint32 = 0xD1D1D1D1
	hashTreeVersion     byte   = 1

	// magicnumber version height root checksum
	hashTreeHeaderLength int = 4 + 1 + 4 + DigestLength + DigestLength
)

// headerChecksum is the murmur3-128 of the header bytes preceding the checksum field.
func headerChecksum(hdr []byte) [DigestLength]byte {
	h1, h2 := murmur3.Sum128(hdr)
	var cs [DigestLength]byte
	binary.BigEndian.PutUint64(cs[:8], h1)
	binary.BigEndian.PutUint64(cs[8:], h2)
	return cs
}

// legacyHeaderChecksum reproduces the pre-fix stored value: hash.Sum appended,
// so the copy took the header's own leading bytes, zero-padded (murmur3-128 of
// empty input is zeros). Accepted on read so pre-fix files stay loadable.
func legacyHeaderChecksum(hdr []byte) [DigestLength]byte {
	var legacy [DigestLength]byte
	copy(legacy[:], hdr)
	return legacy
}

// validHeaderChecksum accepts the real murmur3-128 or the legacy pre-fix value.
func validHeaderChecksum(stored, hdr []byte) bool {
	expected := headerChecksum(hdr)
	if bytes.Equal(stored, expected[:]) {
		return true
	}
	legacy := legacyHeaderChecksum(hdr)
	return bytes.Equal(stored, legacy[:])
}

func (ht *HashTree) Serialize(w io.Writer) (n int64, err error) {
	ht.mux.Lock()
	defer ht.mux.Unlock()

	var hdr [hashTreeHeaderLength]byte

	hdrOff := 0

	binary.BigEndian.PutUint32(hdr[hdrOff:], hashTreeMagicNumber)
	hdrOff += 4

	hdr[hdrOff] = hashTreeVersion
	hdrOff++

	binary.BigEndian.PutUint32(hdr[hdrOff:], uint32(ht.height))
	hdrOff += 4

	root := ht.root()
	rootBs, err := root.MarshalBinary()
	if err != nil {
		return 0, err
	}
	copy(hdr[hdrOff:hdrOff+DigestLength], rootBs)
	hdrOff += DigestLength

	checksum := headerChecksum(hdr[:hdrOff])
	copy(hdr[hdrOff:hdrOff+DigestLength], checksum[:])

	n1, err := w.Write(hdr[:])
	if err != nil {
		return int64(n1), err
	}

	n = int64(n1)

	// Write leaves
	for i := ht.innerNodesCount; i < len(ht.nodes); i++ {
		nodeBs, err := ht.nodes[i].MarshalBinary()
		if err != nil {
			return n, err
		}

		ni, err := w.Write(nodeBs)
		if err != nil {
			return n + int64(ni), err
		}

		n += int64(ni)
	}

	return n, nil
}

func DeserializeHashTree(r io.Reader) (*HashTree, error) {
	var hdr [hashTreeHeaderLength]byte

	_, err := io.ReadFull(r, hdr[:])
	if err != nil {
		return nil, err
	}

	hdrOff := 0

	magicNumber := binary.BigEndian.Uint32(hdr[hdrOff:])
	if magicNumber != hashTreeMagicNumber {
		return nil, fmt.Errorf("hashtree magic number mismatch")
	}
	hdrOff += 4

	if hdr[hdrOff] != hashTreeVersion {
		return nil, fmt.Errorf("unsupported version %d, expected version %d", hdr[0], hashTreeVersion)
	}
	hdrOff++

	height := int(binary.BigEndian.Uint32(hdr[hdrOff:]))
	hdrOff += 4

	var root Digest
	root.UnmarshalBinary(hdr[hdrOff : hdrOff+DigestLength])
	hdrOff += DigestLength

	// Checked before NewHashTree so a corrupt height cannot drive a huge allocation.
	if !validHeaderChecksum(hdr[hdrOff:hdrOff+DigestLength], hdr[:hdrOff]) {
		return nil, fmt.Errorf("header checksum mismatch")
	}

	ht, err := NewHashTree(height)
	if err != nil {
		return nil, err
	}

	for i := 0; i < LeavesCount(ht.Height()); i++ {
		var leafBs [DigestLength]byte

		_, err := io.ReadFull(r, leafBs[:])
		if err != nil {
			return nil, fmt.Errorf("reading leaf %d: %w", i, err)
		}

		leafPos := ht.innerNodesCount + i
		err = ht.nodes[leafPos].UnmarshalBinary(leafBs[:])
		if err != nil {
			return nil, fmt.Errorf("unmarshalling leaf %d: %w", i, err)
		}
	}

	if root != ht.root() {
		return nil, fmt.Errorf("root digest mismatch")
	}

	return ht, nil
}

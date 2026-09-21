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

package lsmkv

import (
	"cmp"
	"encoding/binary"
	"fmt"
)

// invertedPair is one posting, or a tombstone for one doc. It holds no
// pointers, so the GC never walks the posting slices a memtable retains.
type invertedPair struct {
	// the big-endian reading of the 8 key bytes it replaces, whose numeric
	// order is their byte order
	docID       uint64
	tfBits      uint32
	propLenBits uint32
	tombstone   bool
}

// The only two record sizes an inverted writer has ever produced.
const (
	invertedRecordPostingLen   = 2 + 8 + 2 + 8
	invertedRecordTombstoneLen = 2 + 8 + 2
)

func (p invertedPair) keyCompare(other invertedPair) int {
	return cmp.Compare(p.docID, other.docID)
}

func (p invertedPair) keyEqual(other invertedPair) bool {
	return p.docID == other.docID
}

// encodeCommitLog writes the record into dst, which must hold
// invertedRecordPostingLen bytes, and returns how many it wrote.
func (p invertedPair) encodeCommitLog(dst []byte) int {
	binary.LittleEndian.PutUint16(dst[0:2], 8)
	binary.BigEndian.PutUint64(dst[2:10], p.docID)

	if p.tombstone {
		binary.LittleEndian.PutUint16(dst[10:12], 0)
		return invertedRecordTombstoneLen
	}

	binary.LittleEndian.PutUint16(dst[10:12], 8)
	binary.LittleEndian.PutUint32(dst[12:16], p.tfBits)
	binary.LittleEndian.PutUint32(dst[16:20], p.propLenBits)
	return invertedRecordPostingLen
}

// decodeCommitLog accepts only the two shapes above, so that a malformed
// record stops replay instead of reaching a segment.
func (p *invertedPair) decodeCommitLog(in []byte, tombstone bool) error {
	if len(in) < invertedRecordTombstoneLen {
		return fmt.Errorf("inverted record: want at least %d bytes, got %d",
			invertedRecordTombstoneLen, len(in))
	}

	if keyLen := binary.LittleEndian.Uint16(in[0:2]); keyLen != 8 {
		return fmt.Errorf("inverted record: key length must be 8, got %d", keyLen)
	}

	valueLen := int(binary.LittleEndian.Uint16(in[10:12]))
	if read := invertedRecordTombstoneLen + valueLen; read != len(in) {
		return fmt.Errorf("inconsistent inverted record: read %d out of %d bytes",
			read, len(in))
	}

	decoded := invertedPair{
		docID:     binary.BigEndian.Uint64(in[2:10]),
		tombstone: tombstone,
	}

	// a tombstone's value is never read: flush routes it into the bitmap
	if !tombstone {
		if valueLen != 8 {
			return fmt.Errorf("inverted record: value length must be 8, got %d", valueLen)
		}
		decoded.tfBits = binary.LittleEndian.Uint32(in[12:16])
		decoded.propLenBits = binary.LittleEndian.Uint32(in[16:20])
	}

	*p = decoded
	return nil
}

// invertedPairsToMapPairs materializes the MapPair view the merged read path
// and the map cursor consume. The result aliases a fresh arena, not the memtable.
func invertedPairsToMapPairs(in []invertedPair) []MapPair {
	out := make([]MapPair, len(in))
	arena := make([]byte, len(in)*invPayloadLen)

	for i, p := range in {
		buf := arena[i*invPayloadLen : (i+1)*invPayloadLen]
		binary.BigEndian.PutUint64(buf[0:8], p.docID)
		out[i] = MapPair{Key: buf[0:8], Tombstone: p.tombstone}

		if p.tombstone {
			continue
		}
		binary.LittleEndian.PutUint32(buf[8:12], p.tfBits)
		binary.LittleEndian.PutUint32(buf[12:16], p.propLenBits)
		out[i].Value = buf[8:16]
	}

	return out
}

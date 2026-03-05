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

package segmentindex

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

type Indexes struct {
	Keys                []Key
	SecondaryIndexCount uint16
	ScratchSpacePath    string
	ObserveWrite        prometheus.Observer
	AllocChecker        memwatch.AllocChecker

	// SizesPrecomputed indicates that the compactor already iterated over all
	// keys and pre-accumulated the index sizes below, so writeDirectly can skip
	// its own O(N) scan.
	SizesPrecomputed               bool
	PrecomputedPrimaryIndexSize    int64
	PrecomputedSecondaryIndexSizes []int64
}

func (s *Indexes) WriteTo(w io.Writer, expectedSize uint64) (int64, error) {
	if s.SecondaryIndexCount == 0 {
		// No secondary indices: write the primary index directly.
		return s.buildAndMarshalPrimary(w, s.Keys)
	}
	return s.writeDirectly(w)
}

// writeDirectly writes all indexes in a single forward pass without scratch
// files. It pre-computes the primary and secondary index sizes to determine
// each secondary index's absolute file offset before writing anything.
//
// The segment file layout (after the data section) is:
//   - offset table: SecondaryIndexCount × uint64 (absolute file offsets)
//   - primary index: balanced BST over primary keys
//   - secondary index[0], [1], …: each a balanced BST sorted by that key
func (s *Indexes) writeDirectly(w io.Writer) (written int64, err error) {
	var dataEndOffset uint64 = HeaderSize
	if len(s.Keys) > 0 {
		dataEndOffset = uint64(s.Keys[len(s.Keys)-1].ValueEnd)
	}

	var primarySize int64
	if s.SizesPrecomputed {
		primarySize = s.PrecomputedPrimaryIndexSize
	} else {
		primarySize = computePrimaryIndexSize(s.Keys)
	}

	offsetTableSize := uint64(s.SecondaryIndexCount) * 8

	// Compute absolute file offsets for each secondary index.
	offsets := make([]uint64, s.SecondaryIndexCount)
	secStart := dataEndOffset + offsetTableSize + uint64(primarySize)
	for pos := range offsets {
		offsets[pos] = secStart
		var secSize int64
		if s.SizesPrecomputed && pos < len(s.PrecomputedSecondaryIndexSizes) {
			secSize = s.PrecomputedSecondaryIndexSizes[pos]
		} else {
			secSize = computeSecondaryIndexSize(s.Keys, pos)
		}
		secStart += uint64(secSize)
	}

	// Write the offset table.
	var offsetBuf [8]byte
	for _, off := range offsets {
		binary.LittleEndian.PutUint64(offsetBuf[:], off)
		if _, err := w.Write(offsetBuf[:]); err != nil {
			return written, err
		}
		written += 8
	}

	// Write the primary index.
	n, err := s.buildAndMarshalPrimary(w, s.Keys)
	if err != nil {
		return written, err
	}
	written += n

	// Write each secondary index.
	for pos := 0; pos < int(s.SecondaryIndexCount); pos++ {
		n, err := marshalSortedSecondaryFromKeys(w, s.Keys, pos)
		if err != nil {
			return written, fmt.Errorf("write secondary index %d: %w", pos, err)
		}
		written += n
	}

	return written, nil
}

func (s *Indexes) buildAndMarshalPrimary(w io.Writer, keys []Key) (int64, error) {
	return MarshalSortedKeysFromKeys(w, keys)
}

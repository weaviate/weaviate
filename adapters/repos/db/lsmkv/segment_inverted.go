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
	"encoding/binary"
	"fmt"
	"math"
	"sync"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/gobenc"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

type segmentInvertedData struct {
	// lock to read tombstones and property lengths
	lockInvertedData sync.RWMutex

	tombstones       *sroar.Bitmap
	tombstonesLoaded bool

	propertyLengths       map[uint64]uint32
	propertyLengthsLoaded bool

	// Dense view of propertyLengths for the scoring hot path: indexed by
	// docID-propLengthsMin, avoiding a per-scored-doc map probe. Built only when
	// the docID range is dense enough to bound the extra memory (see
	// loadPropertyLengths); nil otherwise, in which case the map is used. The map
	// is always kept — compaction copies it wholesale.
	propLengthsDense []uint32
	propLengthsMin   uint64

	avgPropertyLengthsAvg   float64
	avgPropertyLengthsCount uint64
}

func (s *segment) loadTombstones() (*sroar.Bitmap, error) {
	s.invertedData.lockInvertedData.Lock()
	defer s.invertedData.lockInvertedData.Unlock()
	if s.strategy != segmentindex.StrategyInverted {
		return nil, fmt.Errorf("property only supported for inverted strategy")
	}

	if s.invertedData.tombstonesLoaded {
		return s.invertedData.tombstones, nil
	}

	buffer := make([]byte, 8)
	if err := s.copyNode(buffer, nodeOffset{s.invertedHeader.TombstoneOffset, s.invertedHeader.TombstoneOffset + 8}); err != nil {
		return nil, fmt.Errorf("copy node: %w", err)
	}
	bitmapSize := binary.LittleEndian.Uint64(buffer)

	if bitmapSize == 0 {
		s.invertedData.tombstonesLoaded = true
		return s.invertedData.tombstones, nil
	}

	buffer = make([]byte, bitmapSize)
	if err := s.copyNode(buffer, nodeOffset{s.invertedHeader.TombstoneOffset + 8, s.invertedHeader.TombstoneOffset + 8 + bitmapSize}); err != nil {
		return nil, fmt.Errorf("copy node: %w", err)
	}

	bitmap := sroar.FromBuffer(buffer)

	s.invertedData.tombstones = bitmap
	s.invertedData.tombstonesLoaded = true
	return bitmap, nil
}

func (s *segment) loadPropertyLengths() (map[uint64]uint32, error) {
	s.invertedData.lockInvertedData.Lock()
	defer s.invertedData.lockInvertedData.Unlock()
	if s.strategy != segmentindex.StrategyInverted {
		return nil, fmt.Errorf("property only supported for inverted strategy")
	}

	if s.invertedData.propertyLengthsLoaded {
		return s.invertedData.propertyLengths, nil
	}

	buffer := make([]byte, 8*3)

	if err := s.copyNode(buffer, nodeOffset{s.invertedHeader.PropertyLengthsOffset, s.invertedHeader.PropertyLengthsOffset + 8*3}); err != nil {
		return nil, fmt.Errorf("copy node: %w", err)
	}

	s.invertedData.avgPropertyLengthsAvg = math.Float64frombits(binary.LittleEndian.Uint64(buffer))
	s.invertedData.avgPropertyLengthsCount = binary.LittleEndian.Uint64(buffer[8:16])
	propertyLengthsSize := binary.LittleEndian.Uint64(buffer[16:24])

	if propertyLengthsSize == 0 {
		s.invertedData.propertyLengthsLoaded = true
		return s.invertedData.propertyLengths, nil
	}

	propertyLengthsStart := s.invertedHeader.PropertyLengthsOffset + 16 + 8
	propertyLengthsEnd := propertyLengthsStart + propertyLengthsSize

	buffer = make([]byte, propertyLengthsSize)

	if err := s.copyNode(buffer, nodeOffset{propertyLengthsStart, propertyLengthsEnd}); err != nil {
		return nil, fmt.Errorf("copy node: %w", err)
	}
	propLengths, err := gobenc.Decode(buffer)
	if err != nil {
		return s.invertedData.propertyLengths, fmt.Errorf("decode property lengths: %w", err)
	}

	if math.IsNaN(s.invertedData.avgPropertyLengthsAvg) || math.IsInf(s.invertedData.avgPropertyLengthsAvg, 0) {
		// recompute property lengths average
		var totalLength uint64
		for _, length := range propLengths {
			totalLength += uint64(length)
		}
		if s.invertedData.avgPropertyLengthsCount > 0 && len(propLengths) > 0 {
			s.invertedData.avgPropertyLengthsAvg = float64(totalLength) / float64(len(propLengths))
		} else {
			s.invertedData.avgPropertyLengthsAvg = 0
		}
	}

	s.invertedData.propertyLengthsLoaded = true
	s.invertedData.propertyLengths = propLengths
	s.buildDensePropertyLengths(propLengths)
	return s.invertedData.propertyLengths, nil
}

// buildDensePropertyLengths populates the dense docID->propLength view used by
// the scoring hot path, but only when the docID range is dense enough that the
// array stays within ~4/3 of the entry count (≈75% dense). Sparse segments keep
// using the map so a few scattered docIDs can't blow up memory. Must be called
// with lockInvertedData held.
func (s *segment) buildDensePropertyLengths(propLengths map[uint64]uint32) {
	if len(propLengths) == 0 {
		return
	}
	minID, maxID := uint64(math.MaxUint64), uint64(0)
	for id := range propLengths {
		if id < minID {
			minID = id
		}
		if id > maxID {
			maxID = id
		}
	}
	span := maxID - minID + 1
	if span > uint64(len(propLengths))/3*4 {
		return // too sparse — keep the map only
	}
	dense := make([]uint32, span)
	for id, l := range propLengths {
		dense[id-minID] = l
	}
	s.invertedData.propLengthsDense = dense
	s.invertedData.propLengthsMin = minID
}

// ReadOnlyTombstones returns segment's tombstones
// Returned bitmap must not be mutated
func (s *segment) ReadOnlyTombstones() (*sroar.Bitmap, error) {
	if s.strategy != segmentindex.StrategyInverted {
		return nil, fmt.Errorf("tombstones only supported for inverted strategy")
	}

	s.invertedData.lockInvertedData.RLock()
	if s.invertedData.tombstonesLoaded {
		defer s.invertedData.lockInvertedData.RUnlock()
		return s.invertedData.tombstones, nil
	}
	s.invertedData.lockInvertedData.RUnlock()

	return s.loadTombstones()
}

// MergeTombstones merges segment's tombstones with other tombstones
// creating new bitmap that replaces the previous one (previous one is not mutated)
// Returned bitmap must not be mutated
func (s *segment) MergeTombstones(other *sroar.Bitmap) (*sroar.Bitmap, error) {
	if s.strategy != segmentindex.StrategyInverted {
		return nil, fmt.Errorf("tombstones only supported for inverted strategy")
	}

	if _, err := s.ReadOnlyTombstones(); err != nil {
		return nil, err
	}

	s.invertedData.lockInvertedData.Lock()
	defer s.invertedData.lockInvertedData.Unlock()

	s.invertedData.tombstones = sroar.Or(s.invertedData.tombstones, other)
	return s.invertedData.tombstones, nil
}

func (s *segment) getPropertyLengths() (map[uint64]uint32, error) {
	if s.strategy != segmentindex.StrategyInverted {
		return nil, fmt.Errorf("property length only supported for inverted strategy")
	}

	s.invertedData.lockInvertedData.RLock()
	loaded := s.invertedData.propertyLengthsLoaded
	s.invertedData.lockInvertedData.RUnlock()

	if !loaded {
		return s.loadPropertyLengths()
	}

	s.invertedData.lockInvertedData.RLock()
	defer s.invertedData.lockInvertedData.RUnlock()

	return s.invertedData.propertyLengths, nil
}

func (s *segment) hasKey(key []byte) bool {
	if s.strategy != segmentindex.StrategyMapCollection && s.strategy != segmentindex.StrategyInverted {
		return false
	}

	if s.useBloomFilter && !s.bloomFilter.Test(key) {
		return false
	}

	_, err := s.index.Get(key)
	return err == nil
}

func (s *segment) getDocCount(key []byte) uint64 {
	if s.strategy != segmentindex.StrategyMapCollection && s.strategy != segmentindex.StrategyInverted {
		return 0
	}

	node, err := s.index.Get(key)
	if err != nil {
		return 0
	}

	buffer := make([]byte, 8)
	if err = s.copyNode(buffer, nodeOffset{node.Start, node.Start + 8}); err != nil {
		return 0
	}

	return binary.LittleEndian.Uint64(buffer)
}

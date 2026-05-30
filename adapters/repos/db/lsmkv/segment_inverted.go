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
	"sort"
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

	// propLengthDocIDs and propLengthValues are a compact, parallel
	// representation of the per-document property lengths. They replace the
	// former resident map[uint64]uint32, which measured ~45 B/doc (Go map
	// bucket/load-factor overhead) against a 12 B payload. The two slices cost
	// 8+4 = 12 B/doc with no map overhead, a ~3.75x reduction.
	//
	// propLengthDocIDs holds the global docIDs sorted ascending; propLengthValues
	// holds the matching lengths, aligned by index. Lookups binary-search
	// propLengthDocIDs (see propLengthAt). The structure is read-only after
	// segment open, same residency/lifecycle as the old map, so the eager-load
	// "no slow first query / no heap spike" property is preserved.
	//
	// Lengths are stored as uint32 (same width as the on-disk gob map values),
	// which is fully lossless for any tokenized BM25 field length.
	propLengthDocIDs      []uint64
	propLengthValues      []uint32
	propertyLengthsLoaded bool

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

// loadPropertyLengths eagerly reads the on-disk gob-encoded property-length map
// at segment open and builds the compact resident slices from it. The on-disk
// format is untouched (still the gob map); only the in-memory representation
// changed. The transient decoded map is dropped once the slices are built, so the
// resident cost is the two compact slices, not the map.
func (s *segment) loadPropertyLengths() error {
	s.invertedData.lockInvertedData.Lock()
	defer s.invertedData.lockInvertedData.Unlock()
	if s.strategy != segmentindex.StrategyInverted {
		return fmt.Errorf("property only supported for inverted strategy")
	}

	if s.invertedData.propertyLengthsLoaded {
		return nil
	}

	buffer := make([]byte, 8*3)

	if err := s.copyNode(buffer, nodeOffset{s.invertedHeader.PropertyLengthsOffset, s.invertedHeader.PropertyLengthsOffset + 8*3}); err != nil {
		return fmt.Errorf("copy node: %w", err)
	}

	s.invertedData.avgPropertyLengthsAvg = math.Float64frombits(binary.LittleEndian.Uint64(buffer))
	s.invertedData.avgPropertyLengthsCount = binary.LittleEndian.Uint64(buffer[8:16])
	propertyLengthsSize := binary.LittleEndian.Uint64(buffer[16:24])

	if propertyLengthsSize == 0 {
		s.invertedData.propertyLengthsLoaded = true
		return nil
	}

	propertyLengthsStart := s.invertedHeader.PropertyLengthsOffset + 16 + 8
	propertyLengthsEnd := propertyLengthsStart + propertyLengthsSize

	buffer = make([]byte, propertyLengthsSize)

	if err := s.copyNode(buffer, nodeOffset{propertyLengthsStart, propertyLengthsEnd}); err != nil {
		return fmt.Errorf("copy node: %w", err)
	}
	propLengths, err := gobenc.Decode(buffer)
	if err != nil {
		return fmt.Errorf("decode property lengths: %w", err)
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

	s.buildPropLengthSlices(propLengths)
	s.invertedData.propertyLengthsLoaded = true
	return nil
}

// buildPropLengthSlices materializes the compact sorted (docID, length) slices
// from a decoded property-length map. docIDs are sorted ascending so lookups can
// binary-search. Lengths are stored as uint32, matching the on-disk gob map width
// exactly, so any field length is represented losslessly.
// Caller must hold lockInvertedData.
func (s *segment) buildPropLengthSlices(propLengths map[uint64]uint32) {
	n := len(propLengths)
	docIDs := make([]uint64, 0, n)
	for docID := range propLengths {
		docIDs = append(docIDs, docID)
	}
	sort.Slice(docIDs, func(i, j int) bool { return docIDs[i] < docIDs[j] })

	values := make([]uint32, n)
	for i, docID := range docIDs {
		values[i] = propLengths[docID]
	}

	s.invertedData.propLengthDocIDs = docIDs
	s.invertedData.propLengthValues = values
}

// propLengthAt returns the property length for docID via binary search over the
// resident sorted slices. A missing docID returns 0, matching the zero value the
// old map[uint64]uint32 returned for an absent key (the Score path relies on this:
// a doc with no recorded length scores as length 0).
func (s *segment) propLengthAt(docID uint64) uint32 {
	s.invertedData.lockInvertedData.RLock()
	defer s.invertedData.lockInvertedData.RUnlock()
	return s.invertedData.propLengthAtLocked(docID)
}

// propLengthAtLocked is the lock-free core of propLengthAt. Caller must hold at
// least a read lock on lockInvertedData.
func (d *segmentInvertedData) propLengthAtLocked(docID uint64) uint32 {
	docIDs := d.propLengthDocIDs
	idx := sort.Search(len(docIDs), func(i int) bool { return docIDs[i] >= docID })
	if idx < len(docIDs) && docIDs[idx] == docID {
		return d.propLengthValues[idx]
	}
	return 0
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

// getPropertyLengths materializes a TRANSIENT map[uint64]uint32 from the compact
// resident slices. The map is NOT cached on the segment; it is rebuilt per call.
// This preserves the existing accessor signature for the whole-map consumers
// (compaction merge, the bucket cold paths in bucket.go, the inverted cursor)
// while keeping the resident structure compact. The hot per-document Score path
// does NOT call this; it uses propLengthAt (binary search, no allocation).
func (s *segment) getPropertyLengths() (map[uint64]uint32, error) {
	if s.strategy != segmentindex.StrategyInverted {
		return nil, fmt.Errorf("property length only supported for inverted strategy")
	}

	s.invertedData.lockInvertedData.RLock()
	loaded := s.invertedData.propertyLengthsLoaded
	s.invertedData.lockInvertedData.RUnlock()

	if !loaded {
		if err := s.loadPropertyLengths(); err != nil {
			return nil, err
		}
	}

	s.invertedData.lockInvertedData.RLock()
	defer s.invertedData.lockInvertedData.RUnlock()

	return s.invertedData.materializePropertyLengthsLocked(), nil
}

// materializePropertyLengthsLocked rebuilds a map from the compact slices. Caller
// must hold at least a read lock on lockInvertedData.
func (d *segmentInvertedData) materializePropertyLengthsLocked() map[uint64]uint32 {
	out := make(map[uint64]uint32, len(d.propLengthDocIDs))
	for i, docID := range d.propLengthDocIDs {
		out[docID] = d.propLengthValues[i]
	}
	return out
}

// snapshotPropLengthSlices returns copies of the resident (docID, length) slices
// under the read lock, so a caller (e.g. compaction) can merge two segments'
// runs without holding either segment's lock for the duration of the merge.
func (s *segment) snapshotPropLengthSlices() ([]uint64, []uint32, error) {
	if s.strategy != segmentindex.StrategyInverted {
		return nil, nil, fmt.Errorf("property length only supported for inverted strategy")
	}

	s.invertedData.lockInvertedData.RLock()
	loaded := s.invertedData.propertyLengthsLoaded
	s.invertedData.lockInvertedData.RUnlock()

	if !loaded {
		if err := s.loadPropertyLengths(); err != nil {
			return nil, nil, err
		}
	}

	s.invertedData.lockInvertedData.RLock()
	defer s.invertedData.lockInvertedData.RUnlock()

	docIDs := make([]uint64, len(s.invertedData.propLengthDocIDs))
	copy(docIDs, s.invertedData.propLengthDocIDs)
	values := make([]uint32, len(s.invertedData.propLengthValues))
	copy(values, s.invertedData.propLengthValues)
	return docIDs, values, nil
}

// mergePropertyLengthRuns merges two sorted (docID, length) runs into a single
// map via a two-pointer walk. The runs are the resident slices of two segments
// being compacted. On an overlapping docID the value from the "newer" run wins
// (newerDocIDs/newerValues) - this matches the LSM ordering where the later
// (rightmost / newer) segment shadows the older one, exactly reproducing the
// previous maps.Copy(write <- clean) semantics where the clean/c2/right segment
// overwrote the write/c1/left segment on overlap.
//
// docID gaps (sparse global docIDs), empty inputs (one or both runs nil/empty),
// and tombstoned docIDs are all handled implicitly: the merge is purely over the
// docIDs present in the length runs; tombstoning is applied separately by the
// compactor's tombstone bitmap, never by the length structure.
func mergePropertyLengthRuns(
	olderDocIDs []uint64, olderValues []uint32,
	newerDocIDs []uint64, newerValues []uint32,
) map[uint64]uint32 {
	out := make(map[uint64]uint32, len(olderDocIDs)+len(newerDocIDs))
	i, j := 0, 0
	for i < len(olderDocIDs) && j < len(newerDocIDs) {
		switch {
		case olderDocIDs[i] < newerDocIDs[j]:
			out[olderDocIDs[i]] = olderValues[i]
			i++
		case olderDocIDs[i] > newerDocIDs[j]:
			out[newerDocIDs[j]] = newerValues[j]
			j++
		default:
			// equal docID: newer run wins
			out[newerDocIDs[j]] = newerValues[j]
			i++
			j++
		}
	}
	for ; i < len(olderDocIDs); i++ {
		out[olderDocIDs[i]] = olderValues[i]
	}
	for ; j < len(newerDocIDs); j++ {
		out[newerDocIDs[j]] = newerValues[j]
	}
	return out
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

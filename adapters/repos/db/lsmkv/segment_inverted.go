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

	// Per-document property lengths in one of two representations (exactly one
	// non-nil once loaded, both nil when the segment has none): a dense array
	// indexed by docID-propLengthsDenseMin when the docID range is ≥1/3 occupied
	// (4B×span ≤ 12B×count — smaller AND O(1) to read), else docID-sorted parallel
	// arrays read via propLengthsView. Full-map consumers (compaction) reconstruct
	// a transient map via getPropertyLengths.
	propLengthsDense      []uint32
	propLengthsDenseMin   uint64
	propLengthsPairIds    []uint64
	propLengthsPairLens   []uint32
	propertyLengthsLoaded bool

	avgPropertyLengthsAvg   float64
	avgPropertyLengthsCount uint64
	// avg/count are read unlocked by combinePropertyLengths; this flag stops the
	// on-demand load from re-writing (racing) them after open.
	propertyLengthsStatsLoaded bool
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

// loadPropertyLengthsStats loads only the avg/count header, deferring the
// per-document arrays to first use. A NaN/Inf stored average can only be
// recomputed from the full data, so that case falls back to a full load.
func (s *segment) loadPropertyLengthsStats() error {
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

	if math.IsNaN(s.invertedData.avgPropertyLengthsAvg) || math.IsInf(s.invertedData.avgPropertyLengthsAvg, 0) {
		_, _, _, _, err := s.loadPropertyLengthsLocked()
		return err
	}

	s.invertedData.propertyLengthsStatsLoaded = true
	if propertyLengthsSize == 0 {
		s.invertedData.propertyLengthsLoaded = true
	}
	return nil
}

func (s *segment) loadPropertyLengths() error {
	s.invertedData.lockInvertedData.Lock()
	defer s.invertedData.lockInvertedData.Unlock()
	_, _, _, _, err := s.loadPropertyLengthsLocked()
	return err
}

// loadPropertyLengthsLocked requires the caller to hold lockInvertedData. It
// returns the loaded arrays so callers needn't re-read the struct after
// unlocking, where a freePropertyLengths could have niled them.
func (s *segment) loadPropertyLengthsLocked() ([]uint32, uint64, []uint64, []uint32, error) {
	if s.strategy != segmentindex.StrategyInverted {
		return nil, 0, nil, nil, fmt.Errorf("property only supported for inverted strategy")
	}

	if s.invertedData.propertyLengthsLoaded {
		return s.invertedData.propLengthsDense, s.invertedData.propLengthsDenseMin,
			s.invertedData.propLengthsPairIds, s.invertedData.propLengthsPairLens, nil
	}

	buffer := make([]byte, 8*3)

	if err := s.copyNode(buffer, nodeOffset{s.invertedHeader.PropertyLengthsOffset, s.invertedHeader.PropertyLengthsOffset + 8*3}); err != nil {
		return nil, 0, nil, nil, fmt.Errorf("copy node: %w", err)
	}

	// don't re-write stats already set at open: readers access them unlocked
	if !s.invertedData.propertyLengthsStatsLoaded {
		s.invertedData.avgPropertyLengthsAvg = math.Float64frombits(binary.LittleEndian.Uint64(buffer))
		s.invertedData.avgPropertyLengthsCount = binary.LittleEndian.Uint64(buffer[8:16])
		s.invertedData.propertyLengthsStatsLoaded = true
	}
	propertyLengthsSize := binary.LittleEndian.Uint64(buffer[16:24])

	if propertyLengthsSize == 0 {
		s.invertedData.propertyLengthsLoaded = true
		return nil, 0, nil, nil, nil
	}

	propertyLengthsStart := s.invertedHeader.PropertyLengthsOffset + 16 + 8
	propertyLengthsEnd := propertyLengthsStart + propertyLengthsSize

	buffer = make([]byte, propertyLengthsSize)
	if err := s.copyNode(buffer, nodeOffset{propertyLengthsStart, propertyLengthsEnd}); err != nil {
		return nil, 0, nil, nil, fmt.Errorf("copy node: %w", err)
	}
	ids, lens, err := gobenc.DecodePairs(buffer)
	if err != nil {
		return nil, 0, nil, nil, fmt.Errorf("decode property lengths: %w", err)
	}

	if math.IsNaN(s.invertedData.avgPropertyLengthsAvg) || math.IsInf(s.invertedData.avgPropertyLengthsAvg, 0) {
		var totalLength uint64
		for _, length := range lens {
			totalLength += uint64(length)
		}
		if s.invertedData.avgPropertyLengthsCount > 0 && len(lens) > 0 {
			s.invertedData.avgPropertyLengthsAvg = float64(totalLength) / float64(len(lens))
		} else {
			s.invertedData.avgPropertyLengthsAvg = 0
		}
	}

	if len(ids) > 0 {
		minID, maxID := ids[0], ids[0]
		// min/max only feed the dense gate, so stop at the first zero length — a
		// zero rules dense out anyway (see the gate below)
		hasZeroLen := lens[0] == 0
		for i := 1; i < len(ids) && !hasZeroLen; i++ {
			if ids[i] < minID {
				minID = ids[i]
			}
			if ids[i] > maxID {
				maxID = ids[i]
			}
			if lens[i] == 0 {
				hasZeroLen = true
			}
		}
		span := maxID - minID + 1
		// dense uses 0 as the "docID absent" sentinel (getPropertyLengths drops
		// zero slots), so a stored 0 would be lost — keep pairs when one exists.
		if !hasZeroLen && span != 0 && span/3 <= uint64(len(ids)) {
			// dense path: direct-indexed fill, no sort needed (unlike the pairs branch)
			dense := make([]uint32, span)
			for i, id := range ids {
				dense[id-minID] = lens[i]
			}
			s.invertedData.propLengthsDense = dense
			s.invertedData.propLengthsDenseMin = minID
		} else {
			sortPropLenPairs(ids, lens)
			s.invertedData.propLengthsPairIds = ids
			s.invertedData.propLengthsPairLens = lens
		}
	}

	s.invertedData.propertyLengthsLoaded = true
	return s.invertedData.propLengthsDense, s.invertedData.propLengthsDenseMin,
		s.invertedData.propLengthsPairIds, s.invertedData.propLengthsPairLens, nil
}

// sortPropLenPairs sorts both arrays in tandem by docID using an LSD radix sort
// over only the bytes the largest docID needs (~4 passes for realistic ID
// ranges). Entries arrive in gob map-iteration order (random), so a comparison
// sort would pay ~n log n interface-dispatched swaps — radix is linear and
// allocation-bounded to one scratch copy of each array.
func sortPropLenPairs(ids []uint64, lens []uint32) {
	n := len(ids)
	if n < 2 {
		return
	}
	var maxID uint64
	for _, id := range ids {
		if id > maxID {
			maxID = id
		}
	}

	srcIds, srcLens := ids, lens
	dstIds := make([]uint64, n)
	dstLens := make([]uint32, n)

	var counts [256]int
	for shift := uint(0); maxID>>shift > 0; shift += 8 {
		for i := range counts {
			counts[i] = 0
		}
		for _, id := range srcIds {
			counts[byte(id>>shift)]++
		}
		sum := 0
		for i, c := range counts {
			counts[i] = sum
			sum += c
		}
		for i, id := range srcIds {
			j := counts[byte(id>>shift)]
			counts[byte(id>>shift)]++
			dstIds[j] = id
			dstLens[j] = srcLens[i]
		}
		srcIds, dstIds = dstIds, srcIds
		srcLens, dstLens = dstLens, srcLens
	}

	// after an odd number of passes the sorted data sits in the scratch arrays
	if &srcIds[0] != &ids[0] {
		copy(ids, srcIds)
		copy(lens, srcLens)
	}
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

// getPropertyLengths reconstructs the full docID->propLength map. Per-query
// lookups go through propLengthsView; the full map is for compaction-frequency
// consumers only.
func (s *segment) getPropertyLengths() (map[uint64]uint32, error) {
	dense, minID, ids, lens, err := s.propLengthArrays()
	if err != nil {
		return nil, err
	}
	if dense != nil {
		// dense is only chosen when no stored length is 0, so a zero slot is
		// always an absent docID and the reconstruction reproduces the key set
		m := make(map[uint64]uint32)
		for i, l := range dense {
			if l != 0 {
				m[minID+uint64(i)] = l
			}
		}
		return m, nil
	}
	if len(ids) == 0 {
		return nil, nil
	}
	m := make(map[uint64]uint32, len(ids))
	for i, id := range ids {
		m[id] = lens[i]
	}
	return m, nil
}

// getPropertyLengthsPairs returns the property lengths as docID-sorted arrays
// (no map): the loaded slices directly for pairs segments, or the dense array
// walked into pairs (0 = absent skipped). Returned slices are read-only.
func (s *segment) getPropertyLengthsPairs() ([]uint64, []uint32, error) {
	dense, minID, ids, lens, err := s.propLengthArrays()
	if err != nil {
		return nil, nil, err
	}
	if dense != nil {
		n := 0
		for _, l := range dense {
			if l != 0 {
				n++
			}
		}
		outIDs := make([]uint64, 0, n)
		outLens := make([]uint32, 0, n)
		for i, l := range dense {
			if l != 0 {
				outIDs = append(outIDs, minID+uint64(i))
				outLens = append(outLens, l)
			}
		}
		return outIDs, outLens, nil
	}
	return ids, lens, nil
}

// mergePropLenPairs merges two docID-sorted pair arrays into one, deduping
// docIDs. On a tie the second array wins (c2 is the newer segment) — the
// precedence the compactor's prior map overwrite relied on.
func mergePropLenPairs(ids1 []uint64, lens1 []uint32, ids2 []uint64, lens2 []uint32) ([]uint64, []uint32) {
	outIDs := make([]uint64, 0, len(ids1)+len(ids2))
	outLens := make([]uint32, 0, len(ids1)+len(ids2))
	i, j := 0, 0
	for i < len(ids1) && j < len(ids2) {
		switch {
		case ids1[i] < ids2[j]:
			outIDs = append(outIDs, ids1[i])
			outLens = append(outLens, lens1[i])
			i++
		case ids1[i] > ids2[j]:
			outIDs = append(outIDs, ids2[j])
			outLens = append(outLens, lens2[j])
			j++
		default: // equal docID: second (newer) wins
			outIDs = append(outIDs, ids2[j])
			outLens = append(outLens, lens2[j])
			i++
			j++
		}
	}
	for ; i < len(ids1); i++ {
		outIDs = append(outIDs, ids1[i])
		outLens = append(outLens, lens1[i])
	}
	for ; j < len(ids2); j++ {
		outIDs = append(outIDs, ids2[j])
		outLens = append(outLens, lens2[j])
	}
	return outIDs, outLens
}

// propLengthArrays loads (if needed) and returns the per-document arrays. Both
// paths capture the arrays under the lock. The returned slices alias the cached
// arrays; a later freePropertyLengths nils the struct fields but not these
// headers, so a reader's slices stay valid.
func (s *segment) propLengthArrays() (dense []uint32, minID uint64, ids []uint64, lens []uint32, err error) {
	if s.strategy != segmentindex.StrategyInverted {
		return nil, 0, nil, nil, fmt.Errorf("property length only supported for inverted strategy")
	}

	s.invertedData.lockInvertedData.RLock()
	if s.invertedData.propertyLengthsLoaded {
		dense, minID = s.invertedData.propLengthsDense, s.invertedData.propLengthsDenseMin
		ids, lens = s.invertedData.propLengthsPairIds, s.invertedData.propLengthsPairLens
		s.invertedData.lockInvertedData.RUnlock()
		return dense, minID, ids, lens, nil
	}
	s.invertedData.lockInvertedData.RUnlock()

	s.invertedData.lockInvertedData.Lock()
	defer s.invertedData.lockInvertedData.Unlock()
	return s.loadPropertyLengthsLocked()
}

// propLengthsView is a no-IO per-docID lookup over a segment's property
// lengths: a single indexed load for dense segments, a cursor over the sorted
// pairs otherwise. A miss returns 0, matching the map semantics this replaced.
// The cursor exploits ascending docID access (scoring and posting iteration
// order): lookups gallop forward from the last hit, so monotone scans are
// amortized O(1); a backward jump restarts the search from the front.
type propLengthsView struct {
	dense []uint32
	min   uint64
	ids   []uint64
	lens  []uint32
	cur   int
}

func (v *propLengthsView) get(docID uint64) uint32 {
	if v.dense != nil {
		if idx := docID - v.min; idx < uint64(len(v.dense)) {
			return v.dense[idx]
		}
		return 0
	}

	ids := v.ids
	n := len(ids)
	c := v.cur

	if c >= n || ids[c] > docID {
		// went backward relative to the cursor (or cursor exhausted): restart
		c = 0
	}
	// short linear probe from the cursor covers the small gaps of
	// frequent-term scans
	for k := 0; k < 4 && c < n; k++ {
		if ids[c] >= docID {
			if ids[c] == docID {
				v.cur = c + 1
				return v.lens[c]
			}
			v.cur = c
			return 0
		}
		c++
	}
	if c >= n {
		v.cur = n
		return 0
	}

	lo := c - 1 // ids[lo] < docID by the loop above
	hi := n - 1
	if ids[hi] < docID {
		v.cur = n
		return 0
	}
	// invariant: ids[lo] < docID <= ids[hi].
	// bounded gallop first: frequent-term scans advance by small-to-medium gaps
	// where a couple of near-cursor probes settle the window (measured faster
	// than going straight to interpolation there)
	for step := 4; step <= 64; step *= 4 {
		p := lo + step
		if p >= hi {
			break
		}
		if ids[p] < docID {
			lo = p
		} else {
			hi = p
			break
		}
	}
	// interpolation for the rest: the pairs are a near-uniformly thinned docID
	// sequence, so the predicted position lands within a few entries of the
	// target no matter how large the jump — a pure gallop paid ~log(gap)
	// cache-missing probes there (measured +30% on rare-term queries). The
	// budget bounds adversarial distributions; binary search finishes the rest.
	for budget := 0; budget < 8 && hi-lo > 1; budget++ {
		span := ids[hi] - ids[lo]
		p := lo + 1 + int(float64(docID-ids[lo])/float64(span)*float64(hi-lo-1))
		if p <= lo {
			p = lo + 1
		} else if p >= hi {
			p = hi - 1
		}
		if ids[p] < docID {
			lo = p
		} else {
			hi = p
		}
	}
	for hi-lo > 1 {
		mid := int(uint(lo+hi) >> 1)
		if ids[mid] < docID {
			lo = mid
		} else {
			hi = mid
		}
	}
	if ids[hi] == docID {
		v.cur = hi + 1
		return v.lens[hi]
	}
	v.cur = hi
	return 0
}

func (s *segment) propLengthsView() (propLengthsView, error) {
	dense, minID, ids, lens, err := s.propLengthArrays()
	if err != nil {
		return propLengthsView{}, err
	}
	return propLengthsView{dense: dense, min: minID, ids: ids, lens: lens}, nil
}

func (s *segment) isPropertyLengthsLoaded() bool {
	if s.strategy != segmentindex.StrategyInverted {
		return false
	}

	s.invertedData.lockInvertedData.RLock()
	defer s.invertedData.lockInvertedData.RUnlock()

	return s.invertedData.propertyLengthsLoaded
}

// freePropertyLengths releases the cached per-document arrays so they reload on
// next use; the avg/count stats are kept (callers read them unlocked) and the
// stats-loaded flag stays set so a reload skips re-reading the header.
func (s *segment) freePropertyLengths() {
	if s.strategy != segmentindex.StrategyInverted {
		return
	}

	s.invertedData.lockInvertedData.Lock()
	defer s.invertedData.lockInvertedData.Unlock()

	s.invertedData.propLengthsDense = nil
	s.invertedData.propLengthsDenseMin = 0
	s.invertedData.propLengthsPairIds = nil
	s.invertedData.propLengthsPairLens = nil
	s.invertedData.propertyLengthsLoaded = false
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

// getInvertedNodeAndDocCount returns a term's index node and posting doc count
// from a single index descent, letting the caller reuse the node for term
// construction. The doc count is read from the inverted posting layout, so it
// returns false for any non-inverted strategy.
func (s *segment) getInvertedNodeAndDocCount(key []byte) (segmentindex.Node, uint64, bool) {
	if s.strategy != segmentindex.StrategyInverted {
		return segmentindex.Node{}, 0, false
	}

	if s.useBloomFilter && !s.bloomFilter.Test(key) {
		return segmentindex.Node{}, 0, false
	}

	node, err := s.index.Get(key)
	if err != nil {
		return segmentindex.Node{}, 0, false
	}

	var buf [8]byte
	if err = s.copyNode(buf[:], nodeOffset{node.Start, node.Start + 8}); err != nil {
		return segmentindex.Node{}, 0, false
	}

	return node, binary.LittleEndian.Uint64(buf[:]), true
}

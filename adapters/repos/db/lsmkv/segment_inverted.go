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

	// Per-document property lengths in one of two representations (the unused one
	// stays nil; all nil when the segment has none): a dense array indexed by
	// docID-propLengthsDenseMin when the docID range is ≥1/3 occupied (4B×span ≤
	// 12B×count — smaller AND O(1) to read), else docID-sorted parallel arrays
	// read via propLengthsView. The pairs ids are propLengthsPairIds32 (4B) when
	// the segment's max docID fits uint32, else propLengthsPairIds (8B). Full-map
	// consumers (compaction) reconstruct a transient map via getPropertyLengths.
	propLengthsDense      []uint32
	propLengthsDenseMin   uint64
	propLengthsPairIds    []uint64
	propLengthsPairIds32  []uint32
	propLengthsPairLens   []uint32
	propertyLengthsLoaded bool

	avgPropertyLengthsAvg   float64
	avgPropertyLengthsCount uint64
	// avg/count are read unlocked by combinePropertyLengths; this flag stops the
	// on-demand load from re-writing (racing) them after open.
	propertyLengthsStatsLoaded bool
}

// propLengths is the loaded per-document property lengths in exactly one
// representation (all-nil = none): dense (indexed by docID-denseMin) or
// docID-sorted pairs whose ids are ids32 (max docID fits uint32) or ids64.
type propLengths struct {
	dense    []uint32
	denseMin uint64
	ids64    []uint64
	ids32    []uint32
	lens     []uint32
}

func (d *segmentInvertedData) propLengthsSnapshot() propLengths {
	return propLengths{
		dense: d.propLengthsDense, denseMin: d.propLengthsDenseMin,
		ids64: d.propLengthsPairIds, ids32: d.propLengthsPairIds32,
		lens: d.propLengthsPairLens,
	}
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
		_, err := s.loadPropertyLengthsLocked()
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
	_, err := s.loadPropertyLengthsLocked()
	return err
}

// loadPropertyLengthsLocked requires the caller to hold lockInvertedData. It
// returns the loaded arrays so callers needn't re-read the struct after
// unlocking, where a freePropertyLengths could have niled them.
func (s *segment) loadPropertyLengthsLocked() (propLengths, error) {
	if s.strategy != segmentindex.StrategyInverted {
		return propLengths{}, fmt.Errorf("property only supported for inverted strategy")
	}

	if s.invertedData.propertyLengthsLoaded {
		return s.invertedData.propLengthsSnapshot(), nil
	}

	buffer := make([]byte, 8*3)

	if err := s.copyNode(buffer, nodeOffset{s.invertedHeader.PropertyLengthsOffset, s.invertedHeader.PropertyLengthsOffset + 8*3}); err != nil {
		return propLengths{}, fmt.Errorf("copy node: %w", err)
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
		return propLengths{}, nil
	}

	propertyLengthsStart := s.invertedHeader.PropertyLengthsOffset + 16 + 8
	propertyLengthsEnd := propertyLengthsStart + propertyLengthsSize

	buffer = make([]byte, propertyLengthsSize)
	if err := s.copyNode(buffer, nodeOffset{propertyLengthsStart, propertyLengthsEnd}); err != nil {
		return propLengths{}, fmt.Errorf("copy node: %w", err)
	}
	ids, lens, err := gobenc.DecodePairs(buffer)
	if err != nil {
		return propLengths{}, fmt.Errorf("decode property lengths: %w", err)
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
			maxID := sortPropLenPairs(ids, lens)
			// Narrow ids to uint32 (4B vs 8B) when the segment's max docID fits.
			// Runtime per-segment gate, never an assumption: docIDs aren't reused
			// on delete, so a churned shard's max can exceed uint32 with few live
			// docs. maxID is the sort's full scan, not the dense gate's early-stop.
			if maxID <= math.MaxUint32 {
				ids32 := make([]uint32, len(ids))
				for i, id := range ids {
					ids32[i] = uint32(id)
				}
				s.invertedData.propLengthsPairIds32 = ids32
			} else {
				s.invertedData.propLengthsPairIds = ids
			}
			s.invertedData.propLengthsPairLens = lens
		}
	}

	s.invertedData.propertyLengthsLoaded = true
	return s.invertedData.propLengthsSnapshot(), nil
}

// sortPropLenPairs sorts both arrays in tandem by docID using an LSD radix sort
// over only the bytes the largest docID needs (~4 passes for realistic ID
// ranges). Entries arrive in gob map-iteration order (random), so a comparison
// sort would pay ~n log n interface-dispatched swaps — radix is linear and
// allocation-bounded to one scratch copy of each array. Returns the full max
// docID (free from the radix scan) so the caller can gate the uint32-ids layout.
func sortPropLenPairs(ids []uint64, lens []uint32) uint64 {
	n := len(ids)
	if n < 2 {
		if n == 1 {
			return ids[0]
		}
		return 0
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
	return maxID
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
	pl, err := s.propLengthArrays()
	if err != nil {
		return nil, err
	}
	if pl.dense != nil {
		// dense is only chosen when no stored length is 0, so a zero slot is
		// always an absent docID and the reconstruction reproduces the key set
		m := make(map[uint64]uint32)
		for i, l := range pl.dense {
			if l != 0 {
				m[pl.denseMin+uint64(i)] = l
			}
		}
		return m, nil
	}
	if pl.ids32 != nil {
		m := make(map[uint64]uint32, len(pl.ids32))
		for i, id := range pl.ids32 {
			m[uint64(id)] = pl.lens[i]
		}
		return m, nil
	}
	if len(pl.ids64) == 0 {
		return nil, nil
	}
	m := make(map[uint64]uint32, len(pl.ids64))
	for i, id := range pl.ids64 {
		m[id] = pl.lens[i]
	}
	return m, nil
}

// getPropertyLengthsPairs returns the property lengths as docID-sorted arrays
// (no map): the loaded slices directly for uint64-pairs segments, the dense
// array walked into pairs (0 = absent skipped), or uint32-pairs widened to
// uint64. Returned slices are read-only (the uint64-pairs case aliases the
// cached slice).
func (s *segment) getPropertyLengthsPairs() ([]uint64, []uint32, error) {
	pl, err := s.propLengthArrays()
	if err != nil {
		return nil, nil, err
	}
	if pl.dense != nil {
		n := 0
		for _, l := range pl.dense {
			if l != 0 {
				n++
			}
		}
		outIDs := make([]uint64, 0, n)
		outLens := make([]uint32, 0, n)
		for i, l := range pl.dense {
			if l != 0 {
				outIDs = append(outIDs, pl.denseMin+uint64(i))
				outLens = append(outLens, l)
			}
		}
		return outIDs, outLens, nil
	}
	if pl.ids32 != nil {
		// widen for the merge/encode path; on-disk stays uint64, so the narrow
		// ids are resident-memory only and don't reach the wire format.
		ids := make([]uint64, len(pl.ids32))
		for i, id := range pl.ids32 {
			ids[i] = uint64(id)
		}
		return ids, pl.lens, nil
	}
	return pl.ids64, pl.lens, nil
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
func (s *segment) propLengthArrays() (propLengths, error) {
	if s.strategy != segmentindex.StrategyInverted {
		return propLengths{}, fmt.Errorf("property length only supported for inverted strategy")
	}

	s.invertedData.lockInvertedData.RLock()
	if s.invertedData.propertyLengthsLoaded {
		pl := s.invertedData.propLengthsSnapshot()
		s.invertedData.lockInvertedData.RUnlock()
		return pl, nil
	}
	s.invertedData.lockInvertedData.RUnlock()

	s.invertedData.lockInvertedData.Lock()
	defer s.invertedData.lockInvertedData.Unlock()
	return s.loadPropertyLengthsLocked()
}

// propLengthsView is a no-IO per-docID lookup over a segment's property
// lengths: a single indexed load for dense segments, a cursor over the sorted
// pairs (ids or ids32, whichever the segment loaded) otherwise. A miss returns
// 0, matching the map semantics this replaced.
type propLengthsView struct {
	dense []uint32
	min   uint64
	ids   []uint64
	ids32 []uint32
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
	if v.ids32 != nil {
		// every stored id fits uint32, so a query past that range is a sure miss
		if docID > math.MaxUint32 {
			return 0
		}
		val, cur := propLenPairLookup(v.ids32, v.lens, v.cur, docID)
		v.cur = cur
		return val
	}
	val, cur := propLenPairLookup(v.ids, v.lens, v.cur, docID)
	v.cur = cur
	return val
}

// propLenPairLookup is the docID-sorted-pairs cursor, shared across id widths
// via a type parameter the compiler specializes (no per-access width branch on
// the hot path). It exploits ascending docID access (scoring and posting
// iteration order): lookups gallop forward from cur, so monotone scans are
// amortized O(1); a backward jump restarts from the front. Returns the value
// (0 on miss) and the advanced cursor.
func propLenPairLookup[T uint32 | uint64](ids []T, lens []uint32, cur int, docID uint64) (uint32, int) {
	n := len(ids)
	c := cur

	if c >= n || uint64(ids[c]) > docID {
		// went backward relative to the cursor (or cursor exhausted): restart
		c = 0
	}
	// short linear probe from the cursor covers the small gaps of
	// frequent-term scans
	for k := 0; k < 4 && c < n; k++ {
		if uint64(ids[c]) >= docID {
			if uint64(ids[c]) == docID {
				return lens[c], c + 1
			}
			return 0, c
		}
		c++
	}
	if c >= n {
		return 0, n
	}

	lo := c - 1 // ids[lo] < docID by the loop above
	hi := n - 1
	if uint64(ids[hi]) < docID {
		return 0, n
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
		if uint64(ids[p]) < docID {
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
		span := uint64(ids[hi]) - uint64(ids[lo])
		p := lo + 1 + int(float64(docID-uint64(ids[lo]))/float64(span)*float64(hi-lo-1))
		if p <= lo {
			p = lo + 1
		} else if p >= hi {
			p = hi - 1
		}
		if uint64(ids[p]) < docID {
			lo = p
		} else {
			hi = p
		}
	}
	for hi-lo > 1 {
		mid := int(uint(lo+hi) >> 1)
		if uint64(ids[mid]) < docID {
			lo = mid
		} else {
			hi = mid
		}
	}
	if uint64(ids[hi]) == docID {
		return lens[hi], hi + 1
	}
	return 0, hi
}

func (s *segment) propLengthsView() (propLengthsView, error) {
	pl, err := s.propLengthArrays()
	if err != nil {
		return propLengthsView{}, err
	}
	return propLengthsView{dense: pl.dense, min: pl.denseMin, ids: pl.ids64, ids32: pl.ids32, lens: pl.lens}, nil
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
	s.invertedData.propLengthsPairIds32 = nil
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

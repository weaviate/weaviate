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

	"github.com/sirupsen/logrus"
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
	// 8+4 = 12 B/doc with no map overhead.
	//
	// propLengthDocIDs holds the global docIDs sorted ascending; propLengthValues
	// holds the matching lengths, aligned by index. Lookups binary-search
	// propLengthDocIDs (see propLengthAt). The structure is read-only after
	// segment open, same residency/lifecycle as the old map, so the eager-load
	// "no slow first query / no heap spike" property is preserved.
	//
	// Lengths are stored LOSSLESS as uint32 (the V0 score path is the read side of
	// this slice). D1 stored uint16 here, which silently clamped any >65535-token
	// doc and corrupted its BM25 score on an un-converted V0 segment (the D1-review
	// B1 trigger). The on-disk V0 gob map is already lossless uint32; only this
	// resident copy clamped. Widening to uint32 makes the V0 hot-path score read
	// lossless at no per-doc cost (CAVEAT-B). The block-impact MaxImpactPropLength
	// path still operates on a uint16 view, but it re-clamps at the boundary in
	// snapshotPropLengthSlices, not here -- the lossless slice and the clamped
	// block-impact view are kept separate (CAVEAT-A symmetry).
	propLengthDocIDs      []uint64
	propLengthValues      []uint32
	propertyLengthsLoaded bool

	avgPropertyLengthsAvg   float64
	avgPropertyLengthsCount uint64

	// V2 flat-column property-length section state. Populated once at segment
	// open (loadPropertyLengthsV2) for Version-1 segments; nil/zero for V0
	// segments. The lengths are read in place from the mmap'd segment (or via
	// bounded per-probe pread when readFromMemory is false) at score time, with
	// NO resident materialization and NO uint16 clamp (lossless uint32).
	//
	// propLengthV2Loaded marks the V2 path as initialized; propLengthV2Count is
	// n (the number of (docID,len) pairs). The two column base offsets are
	// absolute byte offsets into the segment file: docID[i] lives at
	// propLengthV2DocIDBase + i*8, len[i] at propLengthV2LenBase + i*4.
	propLengthV2Loaded    bool
	propLengthV2Count     uint64
	propLengthV2DocIDBase uint64
	propLengthV2LenBase   uint64
}

// maxPropLength is the uint16 ceiling applied ONLY where a length must be
// narrowed to a uint16 view: the block-impact MaxImpactPropLength path
// (snapshotPropLengthSlices). The resident score slice (propLengthValues) and the
// V2 column are both lossless uint32; the exact per-doc BM25 score never passes
// through this clamp (CAVEAT-A). A clamp here emits a Debug log so a future
// regression is greppable rather than silent.
const maxPropLength = math.MaxUint16

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
// binary-search. Lengths are carried LOSSLESS as uint32 -- NO uint16 clamp -- so a
// >65535-token doc scores correctly on an un-converted V0 segment (CAVEAT-B / the
// D1-review B1 fix). The narrowing-to-uint16 the block-impact path needs happens at
// the snapshotPropLengthSlices boundary, not here. Caller must hold lockInvertedData.
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

	// V2 cold-path adapter (the mandatory M2 materialize). A V2 segment's section
	// is the flat column, NOT a gob map, so loadPropertyLengths (the gob decoder)
	// would fail on it. The whole-map consumers (compaction cursor, the bucket
	// cold paths, the inverted cursor) still expect a map[uint64]uint32, so
	// materialize one LOSSLESSLY from the flat column. The lengths are carried
	// verbatim as uint32 -- no uint16 clamp -- so a >65535 doc survives this
	// adapter. This transient map is the bounded, cold-only M2 alloc the design
	// accepts; the hot per-doc score path never calls getPropertyLengths.
	if s.isPropLengthV2() {
		run, err := s.loadPropertyLengthsV2Lossless()
		if err != nil {
			return nil, err
		}
		out := make(map[uint64]uint32, len(run.docIDs))
		for i, docID := range run.docIDs {
			out[docID] = run.lengths[i]
		}
		return out, nil
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
func (s *segment) snapshotPropLengthSlices() ([]uint64, []uint16, error) {
	if s.strategy != segmentindex.StrategyInverted {
		return nil, nil, fmt.Errorf("property length only supported for inverted strategy")
	}

	// V2 segment: the resident D1 slices are never populated, so source the run
	// from the flat column. This uint16 slice feeds ONLY the node-block BM25
	// MaxImpactPropLength path (createBlocks), which has always operated on the
	// uint16-clamped representation -- so clamping here preserves that behavior
	// exactly. The LOSSLESS section output is built separately from the uint32
	// run (loadPropertyLengthRunForVersion), so the on-disk V2 length column is
	// NOT affected by this clamp; a >65535 doc still converts losslessly. A clamp
	// here emits a Debug log naming the doc so a future regression is greppable.
	if s.isPropLengthV2() {
		run, err := s.loadPropertyLengthsV2Lossless()
		if err != nil {
			return nil, nil, err
		}
		return run.docIDs, s.clampPropLengthsToUint16(run.docIDs, run.lengths), nil
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
	// The resident slice is lossless uint32; narrow to the uint16 block-impact view
	// here at the boundary (re-clamp), exactly as the V2 branch does above. The
	// lossless score read (propLengthAtLocked) keeps the uint32 value; only this
	// block-impact snapshot is clamped (CAVEAT-A).
	values := s.clampPropLengthsToUint16(docIDs, s.invertedData.propLengthValues)
	return docIDs, values, nil
}

// clampPropLengthsToUint16 narrows a lossless uint32 length slice to the uint16
// view the block-impact MaxImpactPropLength path consumes, emitting a Debug log
// (with the offending docID + raw length) for every clamped value so a future
// >65535-token regression is greppable rather than silent. docIDs is index-aligned
// with values and is used only for the log; both the V0 resident slice and the V2
// lossless run flow through here so the clamp lives in exactly one place.
func (s *segment) clampPropLengthsToUint16(docIDs []uint64, values []uint32) []uint16 {
	out := make([]uint16, len(values))
	for i, raw := range values {
		if raw > maxPropLength {
			if s.logger != nil {
				var docID uint64
				if i < len(docIDs) {
					docID = docIDs[i]
				}
				s.logger.WithFields(logrus.Fields{
					"docID":       docID,
					"rawLength":   raw,
					"clampedTo":   maxPropLength,
					"segmentPath": s.path,
				}).Debugf("clamping property length to uint16 for block-impact snapshot "+
					"(length %d exceeds %d); the lossless score path is unaffected", raw, maxPropLength)
			}
			raw = maxPropLength
		}
		out[i] = uint16(raw)
	}
	return out
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
	olderDocIDs []uint64, olderValues []uint16,
	newerDocIDs []uint64, newerValues []uint16,
) map[uint64]uint32 {
	out := make(map[uint64]uint32, len(olderDocIDs)+len(newerDocIDs))
	i, j := 0, 0
	for i < len(olderDocIDs) && j < len(newerDocIDs) {
		switch {
		case olderDocIDs[i] < newerDocIDs[j]:
			out[olderDocIDs[i]] = uint32(olderValues[i])
			i++
		case olderDocIDs[i] > newerDocIDs[j]:
			out[newerDocIDs[j]] = uint32(newerValues[j])
			j++
		default:
			// equal docID: newer run wins
			out[newerDocIDs[j]] = uint32(newerValues[j])
			i++
			j++
		}
	}
	for ; i < len(olderDocIDs); i++ {
		out[olderDocIDs[i]] = uint32(olderValues[i])
	}
	for ; j < len(newerDocIDs); j++ {
		out[newerDocIDs[j]] = uint32(newerValues[j])
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

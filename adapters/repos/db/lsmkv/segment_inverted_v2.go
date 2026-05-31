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

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

// V2 flat-column property-length section layout (HeaderInverted.Version == 1).
// The 24-byte avg/count/size prefix is retained verbatim from V0; only the body
// after it changes from a gob map[uint64]uint32 to flat SoA columns:
//
//	offset+0     : avg          float64  (8 B)   -- retained verbatim from V0
//	offset+8     : count        uint64   (8 B)   -- retained verbatim from V0
//	offset+16    : size         uint64   (8 B)   = 8 + n*12 (byte-length of body)
//	offset+24    : n            uint64   (8 B)   = number of (docID,len) pairs
//	offset+32    : docID column n*uint64         sorted ascending
//	offset+32+n*8: len column   n*uint32         aligned by index with docID
//
// docID[i] at offset+32+i*8; len[i] at offset+32+n*8+i*4. SoA (separate columns)
// so the docID column is contiguous for the gallop and the uint32 length column
// packs without padding.
//
// Why fixed-width uint64 for docIDs (not delta+varint): the score path
// (propLengthForScore) must locate a specific docID in O(log n) time via gallop
// and binary search. Variable-width varint encoding is not directly seekable --
// computing docID[mid] requires decoding every byte before it -- ruling out both
// plain delta+varint and the inverted segment's own forward-sequential posting
// codec. A fixed-width column allows O(log n) random access by index arithmetic.
// The trade: at 1B docs the raw uint64 column is ~8 GB on disk; a Lucene
// DOC-values style block-delta column (fixed-width per-block base skip index +
// delta-varint within each block) would be ~1.1 GB (~7-8x smaller). A standalone
// prototype measured block-delta B=128 at ~131 ns/lookup (monotonic, worst case)
// vs ~7 ns for flat gallop -- a 10-19x CPU overhead that narrows under real
// BlockMax-WAND short-advance patterns. Block-delta is a deliberate fast-follow,
// pending a short-advance benchmark before adoption.
const (
	propLengthV2PrefixSize = 24 // [avg f64][count u64][size u64]
	propLengthV2NField     = 8  // n uint64
	propLengthV2DocIDWidth = 8  // uint64 per docID
	propLengthV2LenWidth   = 4  // uint32 per length
)

// loadPropertyLengthsV2 initializes the V2 flat-column reader for a Version-1
// inverted segment. It reads the 24-byte avg/count/size prefix and the n field
// (exactly the prefix V0 reads, so avg/count are populated identically -- G8),
// runs the self-describing size cross-check, and caches the column base offsets
// on the segment. It does NOT decode any column into a resident structure: the
// columns are read in place at score time. Called once at open for V2 segments
// (the V0/V2 dual-path branch). Mirrors loadPropertyLengths' prefix read but
// branches on Version.
func (s *segment) loadPropertyLengthsV2() error {
	s.invertedData.lockInvertedData.Lock()
	defer s.invertedData.lockInvertedData.Unlock()

	if s.strategy != segmentindex.StrategyInverted {
		return fmt.Errorf("property only supported for inverted strategy")
	}

	if s.invertedData.propLengthV2Loaded {
		return nil
	}

	base := s.invertedHeader.PropertyLengthsOffset

	// Read the 24-byte prefix exactly as V0 does (pread-safe via copyNode).
	prefix := make([]byte, propLengthV2PrefixSize)
	if err := s.copyNode(prefix, nodeOffset{base, base + propLengthV2PrefixSize}); err != nil {
		return fmt.Errorf("read V2 property-length prefix; segment=%s: %w", s.path, err)
	}

	s.invertedData.avgPropertyLengthsAvg = math.Float64frombits(binary.LittleEndian.Uint64(prefix[0:8]))
	s.invertedData.avgPropertyLengthsCount = binary.LittleEndian.Uint64(prefix[8:16])
	size := binary.LittleEndian.Uint64(prefix[16:24])

	// FINITE path: the prefix avg is byte-identical to V0 (G8) and used as-is --
	// no recompute, no mutation. Both V2 write paths persist only a finite avg
	// (flush zeroes NaN/Inf and the count==0 case; merge's combinePropertyLengths
	// gates its inputs to finite), so a non-finite persisted avg can ONLY come
	// from on-disk corruption.
	//
	// NON-FINITE path: SELF-HEAL exactly like V0 (segment_inverted.go:164-175) --
	// recompute the avg from the persisted property lengths so the corrupt scalar
	// never flows downstream. A non-finite avg otherwise poisons the group-level
	// averagePropSum at segment_group.go:494-497 / segment_group_prepend.go:122-126
	// (uint64(avg*count) is non-finite -> garbage) and corrupts BM25 for the whole
	// bucket. The recompute is applied below once the length column is bounded;
	// it emits a greppable WARN so the corruption is still surfaced to operators.
	if size == 0 {
		// Empty/short-circuit section: V0 used size==0 for "no lengths"; the V2
		// reader treats it identically (n==0, every lookup returns 0). Match the
		// existing propertyLengthsSize==0 short-circuit in loadPropertyLengths.
		// Self-heal with n==0 here so a non-finite avg on an empty section heals
		// to 0 (the count/n guard fails) exactly as V0 does, rather than leaking.
		if err := s.recomputeV2AvgIfNonFinite(0); err != nil {
			return err
		}
		s.invertedData.propLengthV2Count = 0
		s.invertedData.propLengthV2Loaded = true
		return nil
	}

	// Read n (the pair count) from immediately after the prefix.
	nBuf := make([]byte, propLengthV2NField)
	nStart := base + propLengthV2PrefixSize
	if err := s.copyNode(nBuf, nodeOffset{nStart, nStart + propLengthV2NField}); err != nil {
		return fmt.Errorf("read V2 property-length count; segment=%s: %w", s.path, err)
	}
	n := binary.LittleEndian.Uint64(nBuf)

	// Bound n against the bytes that actually remain in the segment BEFORE
	// computing 8 + n*12, which otherwise wraps uint64 for n >= MaxUint64/12+1.
	// A forged / torn / V0-section-read-as-V2 header can carry a wrapped n whose
	// crafted `size` field matches the WRAPPED expectedSize and so slips past the
	// size cross-check below; the read path then slices the columns out of bounds
	// and PANICS on the mmap hot path. Worse, even a non-overflowing n can declare
	// more pairs than the file holds (points-past-EOF), which the size cross-check
	// alone does not catch. The remaining-bytes bound is a real on-disk quantity
	// far below MaxUint64, so requiring 8 + n*12 <= remaining closes BOTH the
	// overflow and the points-past-EOF case, and rejects through the same loud
	// torn-section error path. segmentBytes is the slice the read path indexes:
	// s.contents on mmap, s.size on pread (segmentEndPos as a defensive fallback).
	segmentBytes := s.segmentByteLen()
	if nStart > segmentBytes || segmentBytes-nStart < propLengthV2NField {
		return s.tornV2Section(size, n,
			"property-length n-field starts past the segment end (n-field at %d, segment end %d)",
			nStart, segmentBytes)
	}
	maxPairs := (segmentBytes - nStart - propLengthV2NField) /
		(propLengthV2DocIDWidth + propLengthV2LenWidth)
	if n > maxPairs {
		return s.tornV2Section(size, n,
			"property-length pair count %d exceeds the %d pairs that fit in the bytes "+
				"remaining before the segment end %d (overflow or points past EOF)",
			n, maxPairs, segmentBytes)
	}

	// G4 / defense-in-depth self-describing cross-check: the existing `size`
	// field must equal exactly 8 + n*12 (the n-field + the two columns). A
	// header whose declared size disagrees with its declared n means a torn or
	// mis-decoded section -- reject loudly rather than read past the section end.
	// n is already bounded above, so this multiplication cannot overflow.
	expectedSize := propLengthV2NField + n*(propLengthV2DocIDWidth+propLengthV2LenWidth)
	if size != expectedSize {
		return s.tornV2Section(size, n,
			"declared size %d != 8 + n*12 = %d (n=%d)", size, expectedSize, n)
	}

	// Cache the absolute column base offsets. docID column starts right after n;
	// len column starts right after the docID column.
	docIDBase := nStart + propLengthV2NField
	lenBase := docIDBase + n*propLengthV2DocIDWidth

	s.invertedData.propLengthV2Count = n
	s.invertedData.propLengthV2DocIDBase = docIDBase
	s.invertedData.propLengthV2LenBase = lenBase

	// Self-heal a non-finite persisted avg from the now-bounded length column,
	// matching V0 (segment_inverted.go:164-175). The bases above are set so
	// readLenV2 can sum the column; the finite path no-ops here.
	if err := s.recomputeV2AvgIfNonFinite(n); err != nil {
		return err
	}

	s.invertedData.propLengthV2Loaded = true
	return nil
}

// recomputeV2AvgIfNonFinite self-heals a non-finite persisted avg the same way
// the V0 loader does (segment_inverted.go:164-175): when the persisted
// avgPropertyLengthsAvg is NaN or +/-Inf, it recomputes the average from the
// persisted property lengths -- here, the V2 uint32 length column read over
// [0,n) via readLenV2 -- using the SAME formula and guards as V0:
//
//	avg = sum(lengths) / n   when avgPropertyLengthsCount > 0 && n > 0
//	avg = 0                  otherwise (no count or no lengths)
//
// n is the V2 analog of len(propLengths) in V0 (the number of (docID,len)
// pairs). The recomputed FINITE value is written back into avgPropertyLengthsAvg
// -- the same field V0 heals -- so the downstream uint64(avg*count) in
// segment_group.go / segment_group_prepend.go stays finite and BM25 scoring is
// not poisoned. A greppable WARN is emitted naming the segment, the corrupt
// persisted value, and the recomputed value.
//
// The FINITE path never enters the branch: no recompute, no WARN, no field
// write -- byte-identical to V0. Caller must hold lockInvertedData and must have
// set propLengthV2LenBase before calling with n > 0 (the size==0 site calls with
// n == 0, so the summation loop never runs and the base is not dereferenced).
func (s *segment) recomputeV2AvgIfNonFinite(n uint64) error {
	persisted := s.invertedData.avgPropertyLengthsAvg
	if !math.IsNaN(persisted) && !math.IsInf(persisted, 0) {
		return nil
	}

	var totalLength uint64
	for i := uint64(0); i < n; i++ {
		length, err := s.readLenV2(i, nil)
		if err != nil {
			return fmt.Errorf("recompute V2 avg: read length column at idx %d; segment=%s: %w", i, s.path, err)
		}
		totalLength += uint64(length)
	}

	var recomputed float64
	if s.invertedData.avgPropertyLengthsCount > 0 && n > 0 {
		recomputed = float64(totalLength) / float64(n)
	} else {
		recomputed = 0
	}
	s.invertedData.avgPropertyLengthsAvg = recomputed

	if s.logger != nil {
		s.logger.WithField("segmentPath", s.path).
			WithField("persistedAvg", persisted).
			WithField("recomputedAvg", recomputed).
			Warnf("V2 inverted property-length section has a non-finite persisted avg %v; "+
				"likely on-disk corruption (both write paths persist only finite avgs); "+
				"recomputed avg as %v from the persisted length column (self-heal, matches V0); segment=%s",
				persisted, recomputed, s.path)
	}
	return nil
}

// segmentByteLen returns the authoritative byte length the V2 read path will
// index, used by loadPropertyLengthsV2 to bound the declared pair count against
// the bytes that physically exist. mmap reads index s.contents directly, so its
// length is the true upper bound; pread reads are bounded by s.size (the
// SectionReader length in bufferedReaderAt). segmentEndPos is the production
// segment-end fallback when neither is set (e.g. constructed-in-test segments
// that happen to set only one). Picking the slice the read path actually indexes
// guarantees: any section that passes the bound cannot index out of range.
func (s *segment) segmentByteLen() uint64 {
	if s.readFromMemory {
		if l := uint64(len(s.contents)); l > 0 {
			return l
		}
	} else if s.size > 0 {
		return uint64(s.size)
	}
	return s.segmentEndPos
}

// tornV2Section emits the loud WARN (operator greps it to find the offending
// segment) and returns the single torn-section error all V2 format-layer
// rejections funnel through: the overflow / points-past-EOF bound and the
// size cross-check share this path so a torn or mis-decoded section is rejected
// identically wherever the inconsistency is first detected. The returned error
// always carries the "torn V2 property-length section" prefix the callers and
// tests match on.
func (s *segment) tornV2Section(size, n uint64, format string, args ...any) error {
	detail := fmt.Sprintf(format, args...)
	if s.logger != nil {
		s.logger.WithField("segmentPath", s.path).
			WithField("declaredSize", size).
			WithField("n", n).
			Warnf("V2 inverted property-length section is torn or mis-decoded: %s", detail)
	}
	return fmt.Errorf("torn V2 property-length section: %s; segment=%s", detail, s.path)
}

// propLengthAtV2 returns the lossless uint32 property length for docID from the
// V2 flat columns, galloping over the sorted docID column from startIdx (the
// per-query-per-term last-found index; pass 0 for a fresh/stateless lookup). It
// returns the index it landed on so a monotonic caller can thread it into the
// next lookup (the T3 query cursor). A missing docID returns (0, the bracket
// index) -- zero matches the old map's zero-for-absent semantics.
//
// Both the mmap (zero-copy slice from s.contents) and pread (bounded per-probe
// copyNode) cases are handled by readDocIDV2 / readLenV2, which mirror
// segment_blockmax.go:30-37 EXACTLY. A naive s.contents[off:] without the
// readFromMemory branch panics on a pread-mode segment -- the #1 constraint.
//
// scratch is an optional 8-byte buffer reused across all readDocIDV2/readLenV2
// calls within this gallop. When non-nil, the pread branch writes into
// scratch[0:8] (docID) or scratch[0:4] (len) instead of make([]byte,N),
// eliminating per-probe heap allocation. The buffer is never aliased: each
// probe decodes immediately via binary.LittleEndian before the next probe
// starts. Pass nil to use the fallback make path (compaction, self-heal).
func (s *segment) propLengthAtV2(docID, startIdx uint64, scratch *[8]byte) (uint32, uint64, error) {
	d := s.invertedData
	n := d.propLengthV2Count
	if n == 0 {
		return 0, 0, nil
	}
	if startIdx >= n {
		startIdx = n - 1
	}

	// Gallop: expand a window [lo, hi] that brackets docID, starting from the
	// previous lookup index. The docID column is sorted ascending and queries
	// arrive monotonically (BlockMax-WAND), so the bracket is found in
	// O(log delta) probes spatially local to the previous one.
	probeVal, err := s.readDocIDV2(startIdx, scratch)
	if err != nil {
		return 0, 0, err
	}

	var lo, hi uint64
	switch {
	case probeVal == docID:
		// Hit at the start index (the common monotonic case: same or adjacent).
		length, err := s.readLenV2(startIdx, scratch)
		return length, startIdx, err
	case probeVal < docID:
		// Gallop forward.
		lo = startIdx
		step := uint64(1)
		hi = startIdx
		for {
			next := startIdx + step
			if next >= n-1 {
				hi = n - 1
				break
			}
			v, err := s.readDocIDV2(next, scratch)
			if err != nil {
				return 0, 0, err
			}
			if v >= docID {
				hi = next
				break
			}
			lo = next
			step *= 2
		}
	default: // probeVal > docID
		// Gallop backward.
		hi = startIdx
		step := uint64(1)
		lo = 0
		for {
			if startIdx < step {
				lo = 0
				break
			}
			prev := startIdx - step
			v, err := s.readDocIDV2(prev, scratch)
			if err != nil {
				return 0, 0, err
			}
			if v <= docID {
				lo = prev
				break
			}
			hi = prev
			step *= 2
		}
	}

	// Binary-search the bracketed [lo, hi] window for docID.
	for lo <= hi {
		mid := lo + (hi-lo)/2
		v, err := s.readDocIDV2(mid, scratch)
		if err != nil {
			return 0, 0, err
		}
		switch {
		case v == docID:
			length, err := s.readLenV2(mid, scratch)
			return length, mid, err
		case v < docID:
			lo = mid + 1
		default:
			if mid == 0 {
				// avoid uint64 underflow; docID is below the column minimum.
				return 0, 0, nil
			}
			hi = mid - 1
		}
	}

	// Absent docID. Return 0 (zero-for-absent) and the bracket index so a
	// monotonic caller continues from a sensible position.
	return 0, lo, nil
}

// readDocIDV2 reads docID[idx] from the flat docID column.
//
// mmap path (readFromMemory): zero-copy slice from s.contents, 0 alloc.
//
// pread path, scratch != nil (hot score path via SegmentBlockMax): direct
// contentFile.ReadAt into scratch[0:8], 0 alloc. No buffered reader, no
// metric wrapper, no pool; just a single pread(2) syscall. Safe because the
// iterator calling us is single-goroutine (the gallopIdx/scratch pair is
// per-iterator and never shared), and the 8-byte value is decoded immediately
// by binary.LittleEndian before the next probe starts.
//
// pread path, scratch == nil (compaction / self-heal callers): allocates a
// transient []byte via copyNode. These callers run at load/compact time, not
// on the hot score path.
func (s *segment) readDocIDV2(idx uint64, scratch *[8]byte) (uint64, error) {
	off := s.invertedData.propLengthV2DocIDBase + idx*propLengthV2DocIDWidth
	if s.readFromMemory {
		return binary.LittleEndian.Uint64(s.contents[off : off+propLengthV2DocIDWidth]), nil
	}
	if scratch != nil {
		if _, err := s.contentFile.ReadAt(scratch[:], int64(off)); err != nil {
			return 0, fmt.Errorf("read V2 docID column at idx %d; segment=%s: %w", idx, s.path, err)
		}
		return binary.LittleEndian.Uint64(scratch[:]), nil
	}
	buf := make([]byte, propLengthV2DocIDWidth)
	if err := s.copyNode(buf, nodeOffset{off, off + propLengthV2DocIDWidth}); err != nil {
		return 0, fmt.Errorf("read V2 docID column at idx %d; segment=%s: %w", idx, s.path, err)
	}
	return binary.LittleEndian.Uint64(buf), nil
}

// readLenV2 reads len[idx] from the flat length column as a lossless uint32 (NO
// uint16 clamp -- the V2 column is the lossless fix for the D1 clamp).
//
// mmap path (readFromMemory): zero-copy from s.contents, 0 alloc.
//
// pread path, scratch != nil (hot score path via SegmentBlockMax): direct
// contentFile.ReadAt into scratch[0:4], 0 alloc. Same zero-copy rationale as
// readDocIDV2: single-goroutine iterator, value decoded immediately.
//
// pread path, scratch == nil (compaction / self-heal callers): allocates via
// copyNode.
func (s *segment) readLenV2(idx uint64, scratch *[8]byte) (uint32, error) {
	off := s.invertedData.propLengthV2LenBase + idx*propLengthV2LenWidth
	if s.readFromMemory {
		return binary.LittleEndian.Uint32(s.contents[off : off+propLengthV2LenWidth]), nil
	}
	if scratch != nil {
		if _, err := s.contentFile.ReadAt(scratch[:propLengthV2LenWidth], int64(off)); err != nil {
			return 0, fmt.Errorf("read V2 length column at idx %d; segment=%s: %w", idx, s.path, err)
		}
		return binary.LittleEndian.Uint32(scratch[:propLengthV2LenWidth]), nil
	}
	buf := make([]byte, propLengthV2LenWidth)
	if err := s.copyNode(buf, nodeOffset{off, off + propLengthV2LenWidth}); err != nil {
		return 0, fmt.Errorf("read V2 length column at idx %d; segment=%s: %w", idx, s.path, err)
	}
	return binary.LittleEndian.Uint32(buf), nil
}

// isPropLengthV2 reports whether this segment's property-length section is the
// V2 flat-column format. The dual-path branch is decided ONCE per segment at
// open from the inverted header Version, not per-doc.
func (s *segment) isPropLengthV2() bool {
	return s.invertedHeader != nil &&
		s.invertedHeader.Version == segmentindex.SegmentInvertedVersionV2
}

// propLengthForScore returns the LOSSLESS per-doc property length used in the exact
// BM25 score, dispatching once on the segment's format:
//
//   - V2 (flat column): gallop over the immutable mmap'd column from *gallopIdx
//     (the per-query-per-term monotonic cursor), with NO lockInvertedData RLock --
//     the V2 columns are immutable bytes, so the hot path is lock-free (D1-review
//     M1). *gallopIdx is advanced to the landed index so the next monotonic lookup
//     starts spatially local. Pass a pointer to a stable per-iterator cursor; pass
//     a pointer to a zero local for a stateless one-shot lookup.
//   - V0 (resident slices): binary-search the lossless uint32 resident slice under
//     the read lock (propLengthAt). gallopIdx is untouched on this path.
//
// CAVEAT-A: the exact per-doc score reads ONLY through this lossless path, NEVER
// through the uint16-clamped block-impact snapshot. CAVEAT-B: both branches are
// lossless uint32, so a >65535-token doc scores correctly on V0 and V2 alike.
//
// On a V2 read error (a torn/mis-decoded column surfacing mid-query) the length
// falls back to 0 -- the same value an absent docID yields -- and a WARN is emitted
// so the corruption is greppable; 0 keeps the query answering rather than panicking
// the scorer on a single bad segment.
func (s *segment) propLengthForScore(docID uint64, gallopIdx *uint64) uint32 {
	if !s.isPropLengthV2() {
		return s.propLengthAt(docID)
	}
	var start uint64
	if gallopIdx != nil {
		start = *gallopIdx
	}
	length, idx, err := s.propLengthAtV2(docID, start, nil)
	if err != nil {
		if s.logger != nil {
			s.logger.WithField("segmentPath", s.path).
				WithField("docID", docID).
				Warnf("V2 property-length read failed mid-score, scoring length as 0: %v", err)
		}
		return 0
	}
	if gallopIdx != nil {
		*gallopIdx = idx
	}
	return length
}

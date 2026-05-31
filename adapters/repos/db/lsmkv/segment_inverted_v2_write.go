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
	"io"
	"math"
	"sort"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/gobenc"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

// encodePropertyLengthsV2 serializes a V2 flat-column property-length section:
// the 24-byte [avg][count][size] prefix followed by [n][sorted docID column]
// [uint32 length column], producing bytes the T1 reader (loadPropertyLengthsV2)
// reads back verbatim. docIDs MUST be sorted ascending and aligned by index with
// lengths (the merge and the flush both produce sorted runs, so no extra sort is
// done here -- a caller that passes an unsorted run is a bug the reader's gallop
// would silently mis-resolve, so callers assert sortedness, see writePropertyLengthsV2).
//
// size is written as 8 + n*12 (the n-field + the two columns), exactly the
// self-describing length the T1 reader cross-checks (size == 8 + n*12). Lengths
// are written losslessly as uint32 -- NO uint16 clamp. The clamp lived only in
// D1's resident-slice path (buildPropLengthSlices); the V2 column is the lossless
// fix and MUST never route a length through that path. n==0 emits only the
// prefix with size==0 (matching V0's size==0 short-circuit and the T1 reader's
// n==0 path).
func encodePropertyLengthsV2(avg float64, count uint64, docIDs []uint64, lengths []uint32) []byte {
	n := uint64(len(docIDs))

	if n == 0 {
		buf := make([]byte, propLengthV2PrefixSize)
		binary.LittleEndian.PutUint64(buf[0:8], math.Float64bits(avg))
		binary.LittleEndian.PutUint64(buf[8:16], count)
		binary.LittleEndian.PutUint64(buf[16:24], 0) // size==0
		return buf
	}

	size := uint64(propLengthV2NField) + n*(propLengthV2DocIDWidth+propLengthV2LenWidth)

	// prefix(24) + n(8) + docID column(n*8) + len column(n*4)
	total := propLengthV2PrefixSize + propLengthV2NField +
		int(n)*propLengthV2DocIDWidth + int(n)*propLengthV2LenWidth
	buf := make([]byte, total)

	binary.LittleEndian.PutUint64(buf[0:8], math.Float64bits(avg))
	binary.LittleEndian.PutUint64(buf[8:16], count)
	binary.LittleEndian.PutUint64(buf[16:24], size)

	off := propLengthV2PrefixSize
	binary.LittleEndian.PutUint64(buf[off:off+propLengthV2NField], n)
	off += propLengthV2NField

	docIDBase := off
	lenBase := docIDBase + int(n)*propLengthV2DocIDWidth
	for i := range docIDs {
		binary.LittleEndian.PutUint64(
			buf[docIDBase+i*propLengthV2DocIDWidth:], docIDs[i],
		)
		binary.LittleEndian.PutUint32(
			buf[lenBase+i*propLengthV2LenWidth:], lengths[i],
		)
	}

	return buf
}

// propLengthRun is a sorted, parallel (docID, length) run carrying LOSSLESS
// uint32 lengths. It is the per-input shape the convert-on-compaction merge
// consumes and the merged shape it produces -- streamed straight into the V2
// flat columns with no intermediate Go map. Unlike snapshotPropLengthSlices
// (which returns uint16 post-clamp), the lengths here are never clamped, so a
// >65535 doc survives a V0->V2 convert verbatim (S8 / the D1 B1 fix).
type propLengthRun struct {
	docIDs  []uint64 // sorted ascending
	lengths []uint32 // aligned by index; lossless
}

// loadPropertyLengthsV0Lossless reads a V0 (gob map) property-length section
// LOSSLESSLY into a sorted uint32 run, for use as a convert-on-compaction input.
// It deliberately bypasses the D1 resident-slice path (snapshotPropLengthSlices /
// buildPropLengthSlices), which clamps to uint16 -- reading a >65535 doc through
// that path would RE-CLAMP it and silently carry D1's B1 corruption into the V2
// output. Here the gob is decoded straight to uint32 and the docIDs sorted, so
// every length is carried verbatim.
//
// It reads the same 24-byte [avg][count][size] prefix the V0 loader reads (so
// avg/count are available to combinePropertyLengths) and the gob body after it,
// pread-safe via copyNode.
func (s *segment) loadPropertyLengthsV0Lossless() (propLengthRun, error) {
	base := s.invertedHeader.PropertyLengthsOffset

	prefix := make([]byte, propLengthV2PrefixSize)
	if err := s.copyNode(prefix, nodeOffset{base, base + propLengthV2PrefixSize}); err != nil {
		return propLengthRun{}, fmt.Errorf("read V0 property-length prefix; segment=%s: %w", s.path, err)
	}
	size := binary.LittleEndian.Uint64(prefix[16:24])
	if size == 0 {
		return propLengthRun{}, nil
	}

	bodyStart := base + propLengthV2PrefixSize
	bodyEnd := bodyStart + size
	body := make([]byte, size)
	if err := s.copyNode(body, nodeOffset{bodyStart, bodyEnd}); err != nil {
		return propLengthRun{}, fmt.Errorf("read V0 property-length gob body; segment=%s: %w", s.path, err)
	}

	m, err := gobenc.Decode(body)
	if err != nil {
		return propLengthRun{}, fmt.Errorf("decode V0 property lengths; segment=%s: %w", s.path, err)
	}

	return mapToSortedRun(m), nil
}

// mapToSortedRun turns a decoded V0 gob map into a sorted uint32 run. Lengths are
// carried verbatim (uint32) -- no clamp.
func mapToSortedRun(m map[uint64]uint32) propLengthRun {
	docIDs := make([]uint64, 0, len(m))
	for docID := range m {
		docIDs = append(docIDs, docID)
	}
	sort.Slice(docIDs, func(i, j int) bool { return docIDs[i] < docIDs[j] })
	lengths := make([]uint32, len(docIDs))
	for i, docID := range docIDs {
		lengths[i] = m[docID]
	}
	return propLengthRun{docIDs: docIDs, lengths: lengths}
}

// propLengthRunToMap is the inverse of mapToSortedRun: it materializes a lossless
// uint32 run back into the map[uint64]uint32 the V0 gob encoder consumes. Lengths
// are carried verbatim (no clamp), so a >65535 doc round-trips through the V0->V0
// gob output exactly. The clamped uint16 path lives only in clampPropLengthsToUint16
// (block-impact MaxImpactPropLength) and never reaches the persisted exact section.
func propLengthRunToMap(run propLengthRun) map[uint64]uint32 {
	out := make(map[uint64]uint32, len(run.docIDs))
	for i, docID := range run.docIDs {
		out[docID] = run.lengths[i]
	}
	return out
}

// loadPropertyLengthsV2Lossless reads a V2 (flat-column) property-length section
// into a sorted uint32 run, for use as a convert-on-compaction input. It runs the
// T1 reader's load+self-describing checks (loadPropertyLengthsV2) and then reads
// every (docID, length) pair through the same readDocIDV2/readLenV2 helpers the
// score path uses (so mmap and pread inputs are both handled, and a torn V2 input
// is rejected loudly rather than mis-merged). The lengths are already lossless
// uint32 on disk; they are carried verbatim.
func (s *segment) loadPropertyLengthsV2Lossless() (propLengthRun, error) {
	if err := s.loadPropertyLengthsV2(); err != nil {
		return propLengthRun{}, err
	}
	n := s.invertedData.propLengthV2Count
	if n == 0 {
		return propLengthRun{}, nil
	}
	docIDs := make([]uint64, n)
	lengths := make([]uint32, n)
	for i := uint64(0); i < n; i++ {
		d, err := s.readDocIDV2(i, nil)
		if err != nil {
			return propLengthRun{}, err
		}
		l, err := s.readLenV2(i, nil)
		if err != nil {
			return propLengthRun{}, err
		}
		docIDs[i] = d
		lengths[i] = l
	}
	return propLengthRun{docIDs: docIDs, lengths: lengths}, nil
}

// loadPropertyLengthRunForVersion reads a compaction input's property-length
// section per that input's OWN HeaderInverted.Version: V0 -> lossless gob,
// V2 -> flat column. This is the G6 dispatch: the existing
// len(DataFields)==len(DataFields) compactor guard does NOT catch a V0/V2 mix
// (both inputs have DataFields=[DocIds,Tfs]), so reading every input as a single
// assumed version would decode one of them as garbage and write that garbage
// LOSSLESSLY into the V2 output -- a permanent B1-class corruption. Dispatching
// on the per-input Version is the only thing that prevents it.
func (s *segment) loadPropertyLengthRunForVersion() (propLengthRun, error) {
	if s.isPropLengthV2() {
		return s.loadPropertyLengthsV2Lossless()
	}
	return s.loadPropertyLengthsV0Lossless()
}

// mergePropertyLengthRunsV2 two-pointer-merges two sorted uint32 runs into one
// sorted uint32 run, NEWER-wins on a docID collision (newer == the right/c2
// segment), reproducing compactor_inverted.go's NEWER-wins semantics
// (compactor_inverted.go:133-146) exactly. It streams directly into output
// slices with no intermediate Go map (the allocation-lighter convert path,
// subsystem-impact §7 #1). The output docIDs stay sorted ascending, so the V2
// encoder needs no extra sort pass.
func mergePropertyLengthRunsV2(older, newer propLengthRun) propLengthRun {
	out := propLengthRun{
		docIDs:  make([]uint64, 0, len(older.docIDs)+len(newer.docIDs)),
		lengths: make([]uint32, 0, len(older.lengths)+len(newer.lengths)),
	}
	i, j := 0, 0
	for i < len(older.docIDs) && j < len(newer.docIDs) {
		switch {
		case older.docIDs[i] < newer.docIDs[j]:
			out.docIDs = append(out.docIDs, older.docIDs[i])
			out.lengths = append(out.lengths, older.lengths[i])
			i++
		case older.docIDs[i] > newer.docIDs[j]:
			out.docIDs = append(out.docIDs, newer.docIDs[j])
			out.lengths = append(out.lengths, newer.lengths[j])
			j++
		default:
			// equal docID: newer (right/c2) run wins
			out.docIDs = append(out.docIDs, newer.docIDs[j])
			out.lengths = append(out.lengths, newer.lengths[j])
			i++
			j++
		}
	}
	for ; i < len(older.docIDs); i++ {
		out.docIDs = append(out.docIDs, older.docIDs[i])
		out.lengths = append(out.lengths, older.lengths[i])
	}
	for ; j < len(newer.docIDs); j++ {
		out.docIDs = append(out.docIDs, newer.docIDs[j])
		out.lengths = append(out.lengths, newer.lengths[j])
	}
	return out
}

// writePropertyLengthsV2 writes the V2 flat-column section to w via the
// encodePropertyLengthsV2 layout and returns the number of bytes written. It
// asserts the run is sorted ascending (a defensive check: an unsorted run would
// pass the T1 size cross-check but the gallop would silently mis-resolve docIDs,
// so a sortedness bug must fail loudly here at write time rather than corrupt
// scores at read time).
func writePropertyLengthsV2(w io.Writer, avg float64, count uint64, run propLengthRun) (int, error) {
	if !propLengthRunSorted(run) {
		return 0, fmt.Errorf("writePropertyLengthsV2: docID run is not sorted ascending (internal invariant violated)")
	}
	encoded := encodePropertyLengthsV2(avg, count, run.docIDs, run.lengths)
	if _, err := w.Write(encoded); err != nil {
		return 0, err
	}
	return len(encoded), nil
}

// propLengthRunSorted reports whether the run's docIDs are strictly ascending
// (no duplicates -- a merge output must have deduped collisions).
func propLengthRunSorted(run propLengthRun) bool {
	for i := 1; i < len(run.docIDs); i++ {
		if run.docIDs[i] <= run.docIDs[i-1] {
			return false
		}
	}
	return true
}

// compile-time guard: keep the V2 version sentinel referenced so the writer file
// stays coupled to the segmentindex constant the reader and header guard use.
var _ = segmentindex.SegmentInvertedVersionV2

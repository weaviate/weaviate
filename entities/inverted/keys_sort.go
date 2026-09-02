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

package inverted

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sort"
)

// Ordering for [SortedKeys], reached only through the builders' Build methods.
// dispatchFixedWidth and sortVariableWidth each document the shape they cover.
//
// The branch is picked from key width and shared prefix, never from the
// caller's type: the lexicographic encoders already make byte order equal
// value order — it is why LexicographicallySortableFloat64 flips the sign bit
// — so int, number, date, boolean, uuid and text keys all sort correctly by
// bytes. A shared prefix leaves fewer discriminating bytes, so it can move a
// batch to a narrower branch: 14-byte ids need two words, the same ids under a
// 6-byte shared prefix need one.

// sortScratch gathers one sort's working buffers, so what a sort costs in
// memory can be read in one place. Made per sort, not pooled (see
// [VarKeyBuilder.Build]); every buffer here dies with the sort except bytes
// and offs, which the variable-width arm hands back as the finished list.
type sortScratch struct {
	// keys and keysAlt are always the same length — the radix alternates
	// between them — so they come from one allocation sliced in two rather
	// than two of half the size.
	keysBacking   []uint64
	keys, keysAlt []uint64
	hi, hiAlt     []uint64
	idx, idxAlt   []uint32
	bytes         []byte
	offs          []uint32
	// The key-sized buffer americanFlagSort swaps through is deliberately not
	// a field here. It reaches sort.Interface, so handing out a pointer into
	// this struct would move the whole struct to the heap on every path —
	// including the packed arms, which never ask for one.
}

// ensureKeys backs the packed keys. The radix alternates between two halves of
// one allocation, so they are sliced from a single backing array.
func (s *sortScratch) ensureKeys(n int) {
	s.keysBacking = make([]uint64, 2*n)
	s.keys = s.keysBacking[:n]
	s.keysAlt = s.keysBacking[n : 2*n]
}

// ensureIdx backs the permutation alone, for the comparison branch of the
// variable-width arm, which sorts indices and must not pay for the packed
// keys or the second index array.
func (s *sortScratch) ensureIdx(n int) []uint32 {
	s.idx = make([]uint32, n)
	return s.idx
}

// ensureIndexed backs a radix that carries indices alongside the packed keys,
// because the key it packed is not the whole key and cannot be written back on
// its own.
func (s *sortScratch) ensureIndexed(n int) {
	s.ensureKeys(n)
	s.ensureIdx(n)
	s.idxAlt = make([]uint32, n)
}

// ensureWide adds the second key word, which only the 128-bit arm reads. It is
// separate because the variable-width arm needs everything else here and none
// of this: asking for it too would allocate 1.6MB on a 100,000-key filter and
// never touch it, and that is per concurrent query.
func (s *sortScratch) ensureWide(n int) {
	s.ensureIndexed(n)
	s.hi = make([]uint64, n)
	s.hiAlt = make([]uint64, n)
}

func (s *sortScratch) ensureBytes(n int) []byte {
	s.bytes = make([]byte, n)
	return s.bytes
}

// ensureOffs backs the rebuilt offset array. Variable-width keys are permuted,
// so new offsets cannot be written over the old ones: the loop still has to
// read offs[e] for indices it has not reached yet.
func (s *sortScratch) ensureOffs(n int) []uint32 {
	s.offs = make([]uint32, n)
	return s.offs
}

// radixCutoff is the batch size at which the packed arms hand over to a radix
// pass. Set by the stack array those arms sort in — [radixCutoff]uint64 is 512
// bytes, the two-word arm holds twice that — rather than by the measured
// crossover.
//
// The handover is early: BenchmarkFixedArmsSmall still has the packed branch
// ahead of the radix pass at n=63, on both widths, and being early costs under
// a microsecond. Moving the boundary out would mean a larger stack array or an
// allocation on a path whose whole point is having neither.
//
// Deliberately not tuned per machine: the crossover moves with the branch
// predictor and memory bandwidth, but a low gate stays cheap where a high one
// would not — the comparison sort this replaced runs 8.4ms at n=65536 where
// the radix pass runs 0.67ms.
const radixCutoff = 64

// wideRadixCutoff is where the two-word arm takes over, much later than
// radixCutoff because that arm pays for a second radix pass, an index array
// and a permutation buffer.
//
// Between the two constants a two-word batch is sorted by comparison instead
// — the band the packed arm's radixCutoff-sized stack array can't reach, where
// BenchmarkFixedArmsLarge measures the radix pass behind. Uuid filters of a
// few dozen to a few hundred ids sit in it.
//
// Rounded rather than measured to an exact crossover, which moves between runs
// and machines — repeated benchmarks put it anywhere from 160 to 200. Being off
// by a few dozen keys costs little: the two branches are within about 5% of
// each other through that band, and only pull apart well past it.
const wideRadixCutoff = 192

// ErrInternal reports that a list could not be built because this package, or
// its caller, is wrong — not because a filter value was. A builder's other
// rejections and this one reach the API through the same return, so without a
// sentinel a broken sort would look like a malformed query; a searcher can
// tell them apart and log an internal fault as one.
//
// Returned rather than panicked: the gRPC search path installs no recovery
// interceptor, so a panic here ends the process, and would do so again on
// every replay of the query.
var ErrInternal = errors.New("inverted: internal fault")

// varRadixCutoff is where the variable-width branch hands over, later than
// radixCutoff because that branch also pays for a permutation, a prefix scan
// over offsets, and collision repair for keys the packed word can't separate.
//
// BenchmarkVariableArms: at n=64 the comparison sort is still 28% ahead and
// allocates a third as much; the two meet near here.
//
// A separate constant rather than radixCutoff itself, since that one is
// bounded by the packed branches' stack arrays, not by a measured crossover —
// tying them would move this value with no benchmark behind the change.
const varRadixCutoff = 128

// repairRunCutoff is the run size below which the collision repair stops
// re-packing and compares instead. It shares a value with radixCutoff and
// nothing else: a run is a stretch of an existing permutation rather than a
// batch, so what makes a radix pass worth its histogram there is its own
// question, unmeasured so far.
const repairRunCutoff = radixCutoff

// keyRange is a stretch of keys plus the byte depth at which they are still
// equal. Both radix traversals that carry their own work stack use it — one
// over records in a slab, one over indices into a permutation.
type keyRange struct{ off, n, d int }

// sortKeys orders a variable-width key list — slab plus n+1 ascending offsets —
// and reports the width every key shares, or 0 if they differ.
//
// The returned slab and offsets need not be the ones passed in: differing-
// length keys are rebuilt into fresh arrays, which the caller adopts instead
// of being copied back where they started.
//
// Uniform width is a property of the data, not the builder, so it's detected
// with one scan of the offsets rather than tracked on the append path.
func sortKeys(slab []byte, offs []uint32) ([]byte, []uint32, int) {
	n := len(offs) - 1
	w := uniformWidthOf(offs)
	if n < 2 {
		return slab, offs, w
	}
	// One scratch for whichever arm runs, so the uniform-width path does not
	// build a second set of buffers it has no use for.
	var sc sortScratch
	if w > 0 {
		dispatchFixedWidth(slab, w, &sc)
		return slab, offs, w
	}
	slab, offs = sortVariableWidth(slab, offs, n, &sc)
	return slab, offs, 0
}

// uniformWidthOf reports the width every key shares, or 0 if they differ.
// offs[i] is cumulative, so uniform width w means offs[i] == i*w.
func uniformWidthOf(offs []uint32) int {
	if len(offs) < 2 {
		return 0
	}
	w := offs[1] - offs[0]
	if w == 0 {
		return 0
	}
	for i := 1; i < len(offs); i++ {
		if offs[i] != uint32(i)*w {
			return 0
		}
	}
	return int(w)
}

// sortFixedWidth orders a slab whose keys are all w bytes.
func sortFixedWidth(slab []byte, w int) {
	var sc sortScratch
	dispatchFixedWidth(slab, w, &sc)
}

// dispatchFixedWidth is the shared body: sortKeys reaches it with a scratch it is
// already holding for the variable-width arm.
func dispatchFixedWidth(slab []byte, w int, sc *sortScratch) {
	// w == 0 would divide by zero below; both entry points already refuse a
	// non-positive width, so this is belt and suspenders.
	if w <= 0 {
		return
	}
	n := len(slab) / w
	if n < 2 {
		return
	}
	if w == 1 {
		countingSort1(slab)
		return
	}
	// The prefix scan runs before the batch-size gate because it decides how a
	// key is represented, not just whether a radix pass amortises: a key that
	// fits a word packs into one whatever the batch size, and small batches are
	// where the fixed cost of not packing — a sort.Interface allocation and an
	// indirect call per comparison and swap — hurts most.
	lcp := commonPrefixFixed(slab, w, n)
	d := w - lcp
	small := n < radixCutoff
	switch {
	case d <= 8 && small:
		packedSmall(slab, w, lcp, n)
	case d <= 8:
		packedRadix(slab, w, lcp, n, sc)
	case d <= 16 && small:
		widePackedSmall(slab, w, lcp, n)
	case d <= 16 && n < wideRadixCutoff:
		sortSlabComparison(slab, w, n, make([]byte, w))
	case d <= 16:
		widePackedRadix(slab, w, lcp, n, sc)
	case small:
		sortSlabComparison(slab, w, n, make([]byte, w))
	default:
		americanFlagSort(slab, w, lcp, make([]byte, w))
	}
}

// packedSmall orders a batch below the radix cutoff whose keys fit one word
// after the shared prefix — every int, number and date filter, and any uuid or
// text batch whose keys share enough of a prefix.
//
// The packed keys live in a stack array rather than the comparison fallback's
// two allocations, and compare as words rather than through sort.Interface. As
// in packedRadix, the packed value IS the key, so the slab is written back
// from it directly.
func packedSmall(slab []byte, w, lcp, n int) {
	var buf [radixCutoff]uint64
	keys := buf[:n]
	d := w - lcp
	shiftUp := uint(8 * (8 - d))
	for i := range keys {
		keys[i] = loadBE(slab[i*w+lcp:], d) << shiftUp
	}
	slices.Sort(keys)
	for i, v := range keys {
		storeBE(slab[i*w+lcp:], v>>shiftUp, d)
	}
}

// widePackedSmall is packedSmall for the keys that need two words — uuids, and
// anything else up to 16 discriminating bytes. Comparing high word first
// matches the ordering widePackedRadix's two stable radix passes produce.
func widePackedSmall(slab []byte, w, lcp, n int) {
	var buf [radixCutoff][2]uint64
	keys := buf[:n]
	lowBytes := w - lcp - 8
	lowShift := uint(8 * (8 - lowBytes))
	for i := range keys {
		keys[i][0] = binary.BigEndian.Uint64(slab[i*w+lcp:])
		keys[i][1] = loadBE(slab[i*w+lcp+8:], lowBytes) << lowShift
	}
	slices.SortFunc(keys, func(a, b [2]uint64) int {
		if c := cmp.Compare(a[0], b[0]); c != 0 {
			return c
		}
		return cmp.Compare(a[1], b[1])
	})
	for i, v := range keys {
		binary.BigEndian.PutUint64(slab[i*w+lcp:], v[0])
		storeBE(slab[i*w+lcp+8:], v[1]>>lowShift, lowBytes)
	}
}

func commonPrefixFixed(slab []byte, w, n int) int {
	lcp := w
	first := slab[:w]
	for i := 1; i < n && lcp > 0; i++ {
		k := slab[i*w : i*w+w]
		j := 0
		for j < lcp && k[j] == first[j] {
			j++
		}
		lcp = j
	}
	return lcp
}

// packedRadix applies when the bytes after the shared prefix fit in a word.
// The packed value then holds the whole key, so the sorted slab is written
// back from it — no index array, and no second slab to permute into.
func packedRadix(slab []byte, w, lcp, n int, sc *sortScratch) {
	sc.ensureKeys(n)
	keys := sc.keys
	d := w - lcp
	shiftUp := uint(8 * (8 - d))
	if d == 8 && lcp == 0 {
		// Every int, number and date key. Worth its own arm: loadBE walks the
		// key a byte at a time to stay correct at any width, and that loop
		// costs more than the radix it feeds.
		for i := 0; i < n; i++ {
			keys[i] = binary.BigEndian.Uint64(slab[i*w:])
		}
	} else {
		for i := 0; i < n; i++ {
			keys[i] = loadBE(slab[i*w+lcp:], d) << shiftUp
		}
	}
	radixU64(keys, sc.keysAlt)

	// Only the discriminating bytes are written back. The prefix is shared by
	// every key by definition, so whatever key a slot ends up holding, the
	// bytes already sitting in front of it are the ones it wants.
	for i, v := range keys {
		storeBE(slab[i*w+lcp:], v>>shiftUp, d)
	}
}

// widePackedRadix handles uuid-shaped keys: too wide for one word, narrow enough for
// two. The low word is sorted first so the high word's stable pass preserves
// its ordering, which is what makes the pair a correct 128-bit sort.
func widePackedRadix(slab []byte, w, lcp, n int, sc *sortScratch) {
	sc.ensureWide(n)
	hi, lo, idx := sc.hi, sc.keys, sc.idx
	d := w - lcp
	lowBytes := d - 8
	lowShift := uint(8 * (8 - lowBytes))
	for i := 0; i < n; i++ {
		hi[i] = binary.BigEndian.Uint64(slab[i*w+lcp:])
		lo[i] = loadBE(slab[i*w+lcp+8:], lowBytes) << lowShift
		idx[i] = uint32(i)
	}
	radixU64Keyed(lo, sc.keysAlt, idx, sc.idxAlt)
	for i, e := range idx {
		sc.hiAlt[i] = hi[e]
	}
	copy(hi, sc.hiAlt)
	radixU64Keyed(hi, sc.hiAlt, idx, sc.idxAlt)

	permuteFixed(slab, w, idx, make([]byte, w))
}

// permuteFixed rearranges equal-width records in place — record i ends up
// holding what idx[i] pointed at — by following each cycle of the permutation,
// which needs room for one key rather than a second copy of the slab.
//
// Chosen for memory, not speed: it saves widePackedRadix's second slab (16 of
// 56 bytes per key) at the cost of a dependent load chain in place of a
// gather's overlapping reads. BenchmarkPermute, on uuid-shaped keys: the cycle
// trails a scratch-slab gather by a third to a half up to n=2048, ties near
// n=4096, and is 2.4x slower at n=65536 — a few percent at the sizes a uuid
// filter actually reaches.
//
// idx must be a permutation of 0..n-1: a repeat or an out-of-range index
// leaves a cycle that never returns to its start, hanging the goroutine
// forever — uncancellable, unlike a wrong answer. idx is left as the identity
// when done, which is how each cycle marks what it moved; nothing reads it
// after.
func permuteFixed(slab []byte, w int, idx []uint32, tmp []byte) {
	for i := range idx {
		if int(idx[i]) == i {
			continue
		}
		copy(tmp, slab[i*w:i*w+w])
		j := i
		for int(idx[j]) != i {
			src := int(idx[j])
			copy(slab[j*w:j*w+w], slab[src*w:src*w+w])
			idx[j] = uint32(j)
			j = src
		}
		copy(slab[j*w:j*w+w], tmp)
		idx[j] = uint32(j)
	}
}

// americanFlagSort orders equal-width keys with an in-place most-significant-
// digit radix: it buckets on the leading byte first. Once a byte is bucketed
// elements never leave their bucket, so the permutation can be cycled within
// the slab and needs no second copy of it — only the caller's swap buffer and
// the ranges still to visit. d is where to start, letting a caller that knows
// the shared prefix skip the levels that bucket every key together.
//
// The traversal must stay iterative. Recursion carries three 256-entry arrays
// per frame, about 6KB, at a depth equal to the key width — which comes from
// a filter value, so a wide key exhausts the goroutine stack. That is fatal:
// recover cannot catch it and the process dies.
//
// Pending ranges are bounded by the key count rather than the key width: each
// is a disjoint subset holding at least two keys, so at most n/2 can wait.
func americanFlagSort(slab []byte, w, d int, swap []byte) {
	// 64 keeps the initial allocation off the heap without reaching for the n/2
	// bound: a wide 65,536-key batch peaks around 330 pending ranges in practice.
	pending := make([]keyRange, 1, 64)
	pending[0] = keyRange{off: 0, n: len(slab) / w, d: d}
	var b bucketing
	for len(pending) > 0 {
		s := pending[len(pending)-1]
		pending = pending[:len(pending)-1]

		// Advancing over bytes every key agrees on is a loop, not a call: this
		// is the depth that must not become one frame per byte of key width.
		for s.n >= 2 && s.d < w {
			if s.n < msdInsertionCutoff {
				insertionSortFixed(slab[s.off*w:(s.off+s.n)*w], w, s.d, swap)
				break
			}
			if !b.partition(slab, w, s, swap) {
				s.d++
				continue
			}
			pending = b.appendSubRanges(pending, s)
			break
		}
	}
}

// bucketing holds one level's histogram and cursors.
//
// count is shared: partition builds it and appendSubRanges reads it. head and
// tail are partition's own, hoisted here only to keep 4KB off a per-range
// frame — which is what recursion would have cost per level.
type bucketing struct {
	count      [256]int
	head, tail [256]int
}

// partition groups the range by the byte at depth s.d, moving records within
// the slab, and reports whether the byte separated anything. A byte every key
// shares leaves the range untouched for the caller to retry one byte deeper.
func (b *bucketing) partition(slab []byte, w int, s keyRange, swap []byte) bool {
	clear(b.count[:])
	for i := s.off; i < s.off+s.n; i++ {
		b.count[slab[i*w+s.d]]++
	}
	if b.count[slab[s.off*w+s.d]] == s.n {
		return false
	}

	sum := s.off
	for v := 0; v < 256; v++ {
		b.head[v] = sum
		sum += b.count[v]
		b.tail[v] = sum
	}
	// Records are cycled into place rather than copied out and back: once a
	// byte is bucketed nothing leaves its bucket, so every record can be swapped
	// directly to where it belongs.
	for v := 0; v < 256; v++ {
		for b.head[v] < b.tail[v] {
			rec := slab[b.head[v]*w : b.head[v]*w+w]
			target := rec[s.d]
			if target == byte(v) {
				b.head[v]++
				continue
			}
			dst := slab[b.head[target]*w : b.head[target]*w+w]
			copy(swap, rec)
			copy(rec, dst)
			copy(dst, swap)
			b.head[target]++
		}
	}
	return true
}

// appendSubRanges queues every bucket still holding more than one key, to be
// separated one byte deeper.
func (b *bucketing) appendSubRanges(pending []keyRange, s keyRange) []keyRange {
	start := s.off
	for v := 0; v < 256; v++ {
		if b.count[v] > 1 {
			pending = append(pending, keyRange{off: start, n: b.count[v], d: s.d + 1})
		}
		start += b.count[v]
	}
	return pending
}

// msdInsertionCutoff is where bucketing a range costs more than comparing it:
// a pass builds a 256-entry histogram whatever the range holds.
const msdInsertionCutoff = 24

// insertionSortFixed orders a short range of equal-width keys, comparing from
// depth d. swap holds one key while two exchange places: a key here is wide by
// construction — this is only reached below americanFlagSort, which handles the
// keys too wide to pack — so exchanging them a byte at a time costs several
// times what one buffered copy does.
func insertionSortFixed(slab []byte, w, d int, swap []byte) {
	n := len(slab) / w
	for i := 1; i < n; i++ {
		for j := i; j > 0; j-- {
			a := slab[j*w : j*w+w]
			b := slab[(j-1)*w : (j-1)*w+w]
			if bytes.Compare(a[d:], b[d:]) >= 0 {
				break
			}
			copy(swap, a)
			copy(a, b)
			copy(b, swap)
		}
	}
}

// dedupFixed collapses runs of equal keys in a sorted fixed-width slab and
// reports how many distinct keys remain.
//
// The pass also re-validates the sort for free: it already compares each key
// to its predecessor, so checking the sign catches an out-of-order key that no
// consumer could otherwise detect. Returned as an error rather than panicked,
// since a panic here would kill the process on every replay of the query.
//
// TestDedupRejectsAnInversion drives this by hand; a correct sort never
// produces one.
func dedupFixed(slab []byte, w, n int) (int, error) {
	if n < 2 {
		return n, nil
	}
	out := 1
	for i := 1; i < n; i++ {
		cur := slab[i*w : i*w+w]
		switch c := bytes.Compare(slab[(out-1)*w:out*w], cur); {
		case c == 0:
			continue
		case c > 0:
			return 0, fmt.Errorf("%w: key %d of %d sorts before its predecessor "+
				"(%x after %x) in the %d-byte fixed-width branch",
				ErrInternal, i, n, headOf(cur), headOf(slab[(out-1)*w:out*w]), w)
		}
		if out != i {
			copy(slab[out*w:], cur)
		}
		out++
	}
	return out, nil
}

// headOf trims a key for an error message. The batch is gone by the time anyone
// reads the log, so the two keys that disagreed are the only evidence of which
// encoding broke.
func headOf(key []byte) []byte {
	const most = 16
	if len(key) > most {
		return key[:most]
	}
	return key
}

// dedupVariable is dedupFixed for keys of differing lengths, packing the
// survivors back down the slab and rewriting their offsets as it goes.
func dedupVariable(slab []byte, offs []uint32, n int) (int, error) {
	if n < 2 {
		return n, nil
	}
	out := 1
	pos := offs[1]
	for i := 1; i < n; i++ {
		cur := slab[offs[i]:offs[i+1]]
		prev := slab[offs[out-1]:pos]
		switch c := bytes.Compare(prev, cur); {
		case c == 0:
			continue
		case c > 0:
			return 0, fmt.Errorf("%w: key %d of %d sorts before its predecessor "+
				"(%x after %x) in the variable-width branch",
				ErrInternal, i, n, headOf(cur), headOf(prev))
		}
		// The write cursor trails the read position, so this cannot overwrite a
		// key not yet read; skipped while nothing has been dropped yet.
		if pos != offs[i] {
			copy(slab[pos:], cur)
		}
		offs[out] = pos
		pos += uint32(len(cur))
		offs[out+1] = pos
		out++
	}
	return out, nil
}

// countingSort1 is the boolean path: one pass to count, one to write back.
func countingSort1(slab []byte) {
	var counts [256]int
	for _, b := range slab {
		counts[b]++
	}
	pos := 0
	for v, c := range counts {
		for ; c > 0; c-- {
			slab[pos] = byte(v)
			pos++
		}
	}
}

// sortVariableWidth is the general case: keys of differing lengths, so a key's
// position isn't its rank and both slab and offsets must be rebuilt. The
// rebuilt arrays are returned for the caller to adopt, rather than copied back
// over the originals.
//
// The rebuild is unavoidable either way, so only the ordering is worth gating
// — see varRadixCutoff.
func sortVariableWidth(slab []byte, offs []uint32, n int, sc *sortScratch) ([]byte, []uint32) {
	small := n < varRadixCutoff
	var idx []uint32
	if small {
		idx = sc.ensureIdx(n)
	} else {
		sc.ensureIndexed(n)
		idx = sc.idx
	}
	for i := 0; i < n; i++ {
		idx[i] = uint32(i)
	}
	if small {
		sortRunByBytes(slab, offs, idx, 0)
	} else {
		lcp := commonPrefixVariable(slab, offs, n)
		keys := sc.keys
		for i := 0; i < n; i++ {
			keys[i] = packSuffix(slab[offs[i]:offs[i+1]], lcp)
		}
		radixU64Keyed(keys, sc.keysAlt, idx, sc.idxAlt)
		repairCollisions(slab, offs, n, lcp, sc)
	}

	out := sc.ensureBytes(len(slab))
	newOffs := sc.ensureOffs(n + 1)
	pos := uint32(0)
	for i, e := range idx {
		k := slab[offs[e]:offs[e+1]]
		copy(out[pos:], k)
		newOffs[i] = pos
		pos += uint32(len(k))
	}
	newOffs[n] = pos
	return out, newOffs
}

// repairCollisions orders the keys the packed word could not separate.
//
// A key with more than 8 discriminating bytes packs to the same word as its
// neighbours; since radix is stable, such a run comes out in input order,
// which is silently wrong unless repaired here — re-packed 8 bytes deeper and
// radixed again, since a comparison sort would re-walk their shared prefix on
// every call.
//
// Stays iterative for the reason given on [americanFlagSort], advancing 8
// bytes per level instead of one.
//
// Terminates on the run's shortest key, not on whether a level separates
// anything: packSuffix pads an ended key with zero exactly as it pads a
// still-running one, so past that depth no level can tell them apart. Gating
// on the longest key instead would let one long value hold the whole batch in
// the loop, since FIELD tokenization keeps NUL bytes verbatim and so cannot be
// used to detect where a shorter key ended.
func repairCollisions(slab []byte, offs []uint32, n, lcp int, sc *sortScratch) {
	keys, idx := sc.keys, sc.idx
	pending := appendTiedRuns(make([]keyRange, 0, 64), keys, 0, n, lcp+8)

	for len(pending) > 0 {
		r := pending[len(pending)-1]
		pending = pending[:len(pending)-1]

		run := idx[r.off : r.off+r.n]
		if r.n < repairRunCutoff || r.d >= shortestKeyIn(offs, run) {
			sortRunByBytes(slab, offs, run, lcp)
			continue
		}
		for i := r.off; i < r.off+r.n; i++ {
			keys[i] = packSuffix(slab[offs[idx[i]]:offs[idx[i]+1]], r.d)
		}
		radixU64Keyed(keys[r.off:r.off+r.n], sc.keysAlt[r.off:r.off+r.n],
			run, sc.idxAlt[r.off:r.off+r.n])
		pending = appendTiedRuns(pending, keys, r.off, r.n, r.d+8)
	}
}

// appendTiedRuns records every stretch of two or more keys the last pass left
// tied, to be separated at depth d.
func appendTiedRuns(dst []keyRange, keys []uint64, off, n, d int) []keyRange {
	for i := off; i < off+n; {
		j := i + 1
		for j < off+n && keys[j] == keys[i] {
			j++
		}
		if j-i > 1 {
			dst = append(dst, keyRange{off: i, n: j - i, d: d})
		}
		i = j
	}
	return dst
}

// shortestKeyIn is the length of the run's shortest key, which is the depth
// past which the packing stops telling its keys apart. See repairCollisions.
func shortestKeyIn(offs []uint32, run []uint32) int {
	shortest := int(offs[run[0]+1] - offs[run[0]])
	for _, e := range run[1:] {
		if l := int(offs[e+1] - offs[e]); l < shortest {
			shortest = l
		}
	}
	return shortest
}

// sortRunByBytes is the terminal comparison, starting at the batch's shared
// prefix (lcp) rather than the run's own depth: packSuffix zero-pads short
// keys, so "ab" and "ab\x00" pack alike without agreeing on 8 real bytes, and
// comparing only past that point would report them equal.
func sortRunByBytes(slab []byte, offs []uint32, run []uint32, lcp int) {
	slices.SortFunc(run, func(a, b uint32) int {
		return bytes.Compare(slab[offs[a]+uint32(lcp):offs[a+1]],
			slab[offs[b]+uint32(lcp):offs[b+1]])
	})
}

func commonPrefixVariable(slab []byte, offs []uint32, n int) int {
	first := slab[offs[0]:offs[1]]
	lcp := len(first)
	for i := 1; i < n && lcp > 0; i++ {
		k := slab[offs[i]:offs[i+1]]
		if len(k) < lcp {
			lcp = len(k)
		}
		j := 0
		for j < lcp && k[j] == first[j] {
			j++
		}
		lcp = j
	}
	return lcp
}

// radixU64 is a least-significant-digit radix — it passes over the trailing
// byte first — and skips passes whose byte every key agrees on. Which passes
// those are comes from the OR and the AND of every key, folded in one
// arithmetic pass with no memory traffic: a zero byte in (or ^ and) is a byte
// all keys agree on. Narrow value ranges — small ints, dates in one era — make
// that common rather than exceptional.
//
// Kept separate from radixU64Keyed rather than carrying a nil index: the
// scatter loop below is the hot one, and a branch or a second indexed store in
// it costs more than the duplicated body.
func radixU64(a, scratch []uint64) {
	// Guards element-0 indexing below; callers are gated well above this today,
	// but the gate is a tunable constant.
	if len(a) < 2 {
		return
	}
	varying := varyingBytes(a)
	var counts [256]int
	src, dst := a, scratch
	for p := 0; p < 8; p++ {
		shift := uint(8 * p)
		if (varying>>shift)&0xff == 0 {
			continue
		}
		clear(counts[:])
		for _, v := range src {
			counts[(v>>shift)&0xff]++
		}
		sum := 0
		for i := range counts {
			counts[i], sum = sum, sum+counts[i]
		}
		for _, v := range src {
			b := (v >> shift) & 0xff
			dst[counts[b]] = v
			counts[b]++
		}
		src, dst = dst, src
	}
	if &src[0] != &a[0] {
		copy(a, src)
	}
}

// radixU64Keyed sorts keys while carrying idx alongside, stably.
func radixU64Keyed(keys, keyScratch []uint64, idx, idxScratch []uint32) {
	if len(keys) < 2 {
		return
	}
	varying := varyingBytes(keys)
	var counts [256]int
	ks, kd, is, id := keys, keyScratch, idx, idxScratch
	for p := 0; p < 8; p++ {
		shift := uint(8 * p)
		if (varying>>shift)&0xff == 0 {
			continue
		}
		clear(counts[:])
		for _, v := range ks {
			counts[(v>>shift)&0xff]++
		}
		sum := 0
		for i := range counts {
			counts[i], sum = sum, sum+counts[i]
		}
		for i, v := range ks {
			b := (v >> shift) & 0xff
			kd[counts[b]], id[counts[b]] = v, is[i]
			counts[b]++
		}
		ks, kd, is, id = kd, ks, id, is
	}
	if &ks[0] != &keys[0] {
		copy(keys, ks)
		copy(idx, is)
	}
}

// varyingBytes returns a mask whose byte p is non-zero exactly when the keys
// disagree somewhere in byte p.
func varyingBytes(a []uint64) uint64 {
	var or uint64
	and := ^uint64(0)
	for _, v := range a {
		or |= v
		and &= v
	}
	return or ^ and
}

// packSuffix folds up to 8 bytes after the shared prefix into a word,
// big-endian so integer order matches byte order. Short keys pad with zero,
// which is correct because a prefix sorts before any extension of it.
func packSuffix(k []byte, lcp int) uint64 {
	if lcp < len(k) {
		k = k[lcp:]
	} else {
		k = nil
	}
	var v uint64
	for i := 0; i < 8; i++ {
		v <<= 8
		if i < len(k) {
			v |= uint64(k[i])
		}
	}
	return v
}

func loadBE(b []byte, w int) uint64 {
	var v uint64
	for i := 0; i < w; i++ {
		v = v<<8 | uint64(b[i])
	}
	return v
}

func storeBE(b []byte, v uint64, w int) {
	for i := w - 1; i >= 0; i-- {
		b[i] = byte(v)
		v >>= 8
	}
}

// fixedWidthKeys sorts a slab of equal-width keys in place through
// sort.Interface, which costs a call per comparison and per swap. It is the
// fallback for keys too wide to pack into words — see dispatchFixedWidth.
type fixedWidthKeys struct {
	slab    []byte
	w, n    int
	scratch []byte
}

func (f *fixedWidthKeys) Len() int { return f.n }

func (f *fixedWidthKeys) Less(i, j int) bool {
	return bytes.Compare(f.at(i), f.at(j)) < 0
}

func (f *fixedWidthKeys) Swap(i, j int) {
	a, b := f.at(i), f.at(j)
	copy(f.scratch, a)
	copy(a, b)
	copy(b, f.scratch)
}

func (f *fixedWidthKeys) at(i int) []byte { return f.slab[i*f.w : (i+1)*f.w] }

// sortSlabComparison is the small-batch fallback: pdqsort through
// sort.Interface, which costs a call per comparison and per swap. The swap
// buffer comes from the caller's scratch rather than a fresh allocation, since
// every call site is already holding one.
func sortSlabComparison(slab []byte, w, n int, swap []byte) {
	sort.Sort(&fixedWidthKeys{slab: slab, w: w, n: n, scratch: swap})
}

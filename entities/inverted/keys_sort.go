//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package inverted

import (
	"bytes"
	"encoding/binary"
	"sort"
)

// Ordering for [SortedKeys], reached only through the builders' Sort methods.
//
// The method is chosen from the keys' width and their shared prefix, never
// from the caller's type. That is sound because the lexicographic encoders
// exist to make byte order equal value order — it is why
// LexicographicallySortableFloat64 flips the sign bit — so int, number, date,
// boolean, uuid and text keys are all correctly ordered by their bytes, and
// none of them needs a type-aware comparison.
//
// Writing d for the discriminating bytes, a key's width minus the prefix every
// key shares:
//
//	w == 1        counting sort; no comparisons at all
//	n < 256       comparison sort; a radix pass cannot amortise below that
//	d <= 8        pack into a word, radix, write straight back. The packed
//	              value IS the key, so nothing has to be permuted alongside it
//	d <= 16       radix the low word then the high word, carrying indices;
//	              stability across the two makes it a correct 128-bit sort
//	wider         in-place MSD radix, which allocates nothing
//	varying width pack, radix carrying indices, then rebuild slab and offsets
//
// Keys with no shared prefix simply have more discriminating bytes: an 8-byte
// int with nothing in common still takes the first arm, a 14-byte id with
// nothing in common takes the third.

// sortScratch holds the working buffers. Every buffer the sort needs comes
// from here — one that did not would keep allocating unnoticed if these were
// ever pooled, while its neighbours were reused.
//
// Buffers grow to the batch and are never shrunk, so a scratch is worth
// reusing across the properties of one query but not beyond it.
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
	// prefixBuf holds the shared prefix inline. Fixed keys are at most 16
	// bytes and text prefixes are short in practice, so this covers the cases
	// that reach here without an allocation; prefix falls back to the heap
	// only for a prefix longer than the buffer.
	prefixBuf [64]byte
	prefix    []byte
}

func (s *sortScratch) ensureU64(n int) {
	if cap(s.keysBacking) < 2*n {
		s.keysBacking = make([]uint64, 2*n)
	}
	s.keys = s.keysBacking[:n]
	s.keysAlt = s.keysBacking[n : 2*n]
}

func (s *sortScratch) ensureWide(n int) {
	s.ensureU64(n)
	if cap(s.hi) < n {
		s.hi = make([]uint64, n)
		s.hiAlt = make([]uint64, n)
		s.idx = make([]uint32, n)
		s.idxAlt = make([]uint32, n)
	}
	s.hi, s.hiAlt = s.hi[:n], s.hiAlt[:n]
	s.idx, s.idxAlt = s.idx[:n], s.idxAlt[:n]
}

func (s *sortScratch) ensureBytes(n int) []byte {
	if cap(s.bytes) < n {
		s.bytes = make([]byte, n)
	}
	s.bytes = s.bytes[:n]
	return s.bytes
}

// ensureOffs backs the rebuilt offset array. Variable-width keys are permuted,
// so new offsets cannot be written over the old ones: the loop still has to
// read offs[e] for indices it has not reached yet.
func (s *sortScratch) ensureOffs(n int) []uint32 {
	if cap(s.offs) < n {
		s.offs = make([]uint32, n)
	}
	s.offs = s.offs[:n]
	return s.offs
}

// ensurePrefix backs the saved shared prefix, which cannot alias the slab: the
// write-back overwrites slab[0:lcp] on its first key.
func (s *sortScratch) ensurePrefix(n int) []byte {
	if n <= len(s.prefixBuf) {
		return s.prefixBuf[:n]
	}
	if cap(s.prefix) < n {
		s.prefix = make([]byte, n)
	}
	s.prefix = s.prefix[:n]
	return s.prefix
}

// radixCutoff is the batch size below which a radix pass cannot amortise its
// fixed cost — the 256-entry histogram, the scratch buffers, and the prefix
// scan — and the comparison sort wins.
//
// Measured per arm on a 100k-key corpus: the one-word arms (int, date, float,
// and text whose keys fit a word after the shared prefix) cross at n=64, and
// the two-word uuid arm at n=128, since it pays a second pass and an index
// array. 64 for all of them costs uuid about 1.3us on a 64-key filter and
// keeps this a single number.
//
// It is deliberately not tuned per machine. The crossover does move — a weaker
// branch predictor favours radix, less memory bandwidth or a slower allocator
// favours the comparison sort — but the penalties are wildly asymmetric. Being
// under the crossover costs sub-microseconds (0.49us at n=16, 0.28us at n=32);
// being over it costs 10us at n=256 and 7.4ms at n=65536. A gate that is too
// low is nearly free, so it is set low rather than precisely.
const radixCutoff = 64

// sortKeys orders a variable-width key list — slab plus n+1 ascending offsets.
//
// Uniform width is a property of the data, not of the builder, and the text
// producers that reach here do append keys of one length in practice. One scan
// of the offsets says so, which costs a fraction of the sort it redirects and
// adds nothing to the append path — where testing lengths per key would push
// AppendString past Go's inlining budget.
func sortKeys(slab []byte, offs []uint32) {
	n := len(offs) - 1
	if n < 2 {
		return
	}
	// One scratch for whichever arm runs. It is owned here rather than by the
	// caller because nothing outside this file has a reason to supply one, and
	// threaded from here rather than made per arm because the uniform-width
	// path would otherwise build a second set of buffers it does not need.
	var sc sortScratch
	if w := uniformWidthOf(offs); w > 0 {
		sortFixedInto(slab, w, &sc)
		return
	}
	sortVariableWidth(slab, offs, n, &sc)
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
	sortFixedInto(slab, w, &sc)
}

// sortFixedInto is the shared body: sortKeys reaches it with a scratch it is
// already holding for the variable-width arm.
func sortFixedInto(slab []byte, w int, sc *sortScratch) {
	n := len(slab) / w
	if w <= 0 || n < 2 {
		return
	}
	if w == 1 {
		countingSort1(slab)
		return
	}
	if n < radixCutoff {
		sortSlabComparison(slab, w, n)
		return
	}
	lcp := commonPrefixFixed(slab, w, n)
	switch d := w - lcp; {
	case d <= 8:
		packedFixed(slab, w, lcp, n, sc)
	case d <= 16:
		wideFixed(slab, w, lcp, n, sc)
	default:
		sortFixedInPlace(slab, w, lcp, sc.ensurePrefix(w))
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

// packedFixed applies when the bytes after the shared prefix fit in a word.
// The packed value then holds the whole key, so the sorted slab is written
// back from it — no index array, and no second slab to permute into.
func packedFixed(slab []byte, w, lcp, n int, sc *sortScratch) {
	sc.ensureU64(n)
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

	prefix := sc.ensurePrefix(lcp)
	copy(prefix, slab[:lcp])
	for i, v := range keys {
		copy(slab[i*w:], prefix)
		storeBE(slab[i*w+lcp:], v>>shiftUp, d)
	}
}

// wideFixed handles uuid-shaped keys: too wide for one word, narrow enough for
// two. The low word is sorted first so the high word's stable pass preserves
// its ordering, which is what makes the pair a correct 128-bit sort.
func wideFixed(slab []byte, w, lcp, n int, sc *sortScratch) {
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

	out := sc.ensureBytes(len(slab))
	for i, e := range idx {
		copy(out[i*w:], slab[int(e)*w:int(e)*w+w])
	}
	copy(slab, out)
}

// sortFixedInPlace orders equal-width keys with an in-place MSD radix
// (American Flag Sort), allocating nothing beyond the caller's swap buffer.
//
// LSD needs a second buffer because it alternates between two on every pass.
// MSD does not: once a byte is bucketed, elements never leave their bucket, so
// the permutation can be done by cycling records within the slab. The cost is
// random-access swaps where LSD scatters sequentially, which is why the packed
// arms are preferred where a key fits in one or two words.
//
// d is where to start; callers that know the shared prefix pass it and skip
// the levels that would bucket every key together.
func sortFixedInPlace(slab []byte, w, d int, swap []byte) {
	n := len(slab) / w
	if n < 2 || d >= w {
		return
	}
	if n < 24 {
		insertionSortFixed(slab, w, d)
		return
	}

	var count [256]int
	for i := 0; i < n; i++ {
		count[slab[i*w+d]]++
	}
	if count[slab[d]] == n {
		sortFixedInPlace(slab, w, d+1, swap) // every key agrees here
		return
	}

	var head, tail [256]int
	sum := 0
	for b := 0; b < 256; b++ {
		head[b] = sum
		sum += count[b]
		tail[b] = sum
	}
	for b := 0; b < 256; b++ {
		for head[b] < tail[b] {
			rec := slab[head[b]*w : head[b]*w+w]
			target := rec[d]
			if target == byte(b) {
				head[b]++
				continue
			}
			dst := slab[head[target]*w : head[target]*w+w]
			copy(swap, rec)
			copy(rec, dst)
			copy(dst, swap)
			head[target]++
		}
	}

	start := 0
	for b := 0; b < 256; b++ {
		if count[b] > 1 {
			sortFixedInPlace(slab[start*w:(start+count[b])*w], w, d+1, swap)
		}
		start += count[b]
	}
}

func insertionSortFixed(slab []byte, w, d int) {
	n := len(slab) / w
	for i := 1; i < n; i++ {
		for j := i; j > 0; j-- {
			a := slab[j*w : j*w+w]
			b := slab[(j-1)*w : (j-1)*w+w]
			if bytes.Compare(a[d:], b[d:]) >= 0 {
				break
			}
			for k := 0; k < w; k++ {
				a[k], b[k] = b[k], a[k]
			}
		}
	}
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
// position is not its rank and both slab and offsets have to be rebuilt.
func sortVariableWidth(slab []byte, offs []uint32, n int, sc *sortScratch) {
	lcp := commonPrefixVariable(slab, offs, n)
	sc.ensureWide(n)
	keys, idx := sc.keys, sc.idx
	for i := 0; i < n; i++ {
		keys[i] = packSuffix(slab[offs[i]:offs[i+1]], lcp)
		idx[i] = uint32(i)
	}
	radixU64Keyed(keys, sc.keysAlt, idx, sc.idxAlt)

	// A key wider than the packed word collides with its neighbours, and radix
	// is stable, so such a run arrives in INPUT order — which is not sorted.
	// Without this pass the result is silently wrong for any keys agreeing on
	// their first 8 discriminating bytes.
	for i := 0; i < n; {
		j := i + 1
		for j < n && keys[j] == keys[i] {
			j++
		}
		if j-i > 1 {
			run := idx[i:j]
			sort.Slice(run, func(a, b int) bool {
				x := slab[offs[run[a]]:offs[run[a]+1]]
				y := slab[offs[run[b]]:offs[run[b]+1]]
				return bytes.Compare(x, y) < 0
			})
		}
		i = j
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
	copy(slab, out)
	copy(offs, newOffs)
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

// radixU64 is an LSD radix that skips passes whose byte every key agrees on.
//
// Finding those passes is the subtle part. Counting inside each pass means a
// skippable pass has already read the whole slice to discover it is skippable,
// and narrow value ranges — small ints, dates in one era — are the common case
// rather than an edge case. Computing all eight histograms up front fixes that
// but costs more than it saves on wide data: eight scattered increments per
// element over a 16KB histogram lose to one increment into a 2KB array that
// stays hot in L1, and the reads it avoids were L2 hits anyway.
//
// So the passes are found with neither. OR and AND of every key, folded in one
// arithmetic pass with no memory traffic, give the bits that differ somewhere:
// a zero byte in (or ^ and) is a byte every key agrees on.
func radixU64(a, scratch []uint64) {
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

// sortSlabComparison is the small-batch fallback: pdqsort through
// sort.Interface, which costs a call per comparison and per swap.
func sortSlabComparison(slab []byte, w, n int) {
	sort.Sort(&fixedWidthKeys{slab: slab, w: w, n: n, scratch: make([]byte, w)})
}

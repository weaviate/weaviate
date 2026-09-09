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
	"fmt"
	"iter"
	"math"
)

// SortedKeys is an ascending, immutable list of encoded index keys, held as
// one slab plus per-key offsets — or a shared width, when keys are all one
// size — rather than a [][]byte, whose 24-byte headers would outweigh the
// 8-to-16-byte keys they describe.
//
// Only a builder's Build and [SortedKeys.Sub] return one, so the order and the
// absence of duplicates need no re-checking; dropping duplicates means a list
// can be shorter than what was appended.
//
// The zero value is a valid empty list.
type SortedKeys struct {
	slab []byte
	// offs has one entry per key plus a terminator, and is nil when the keys
	// share a width.
	offs []uint32
	// w is the shared key width, read only when offs is nil.
	w int
}

// Len returns the number of keys.
//
// Derived from the backing arrays rather than stored, so it can't drift out of
// sync with them — callers route on this count.
func (k SortedKeys) Len() int {
	if k.offs != nil {
		// A builder never leaves offs without a terminator; treat that shape
		// as empty rather than reporting a count that could misroute callers.
		if len(k.offs) == 0 {
			return 0
		}
		return len(k.offs) - 1
	}
	if k.w <= 0 {
		return 0
	}
	return len(k.slab) / k.w
}

// errUnbuiltKeys is built once rather than formatted per panic: formatting is a
// call, and a call is what At cannot afford — see below.
var errUnbuiltKeys = fmt.Errorf("%w: keys were not made by a builder", ErrInternal)

// At returns key i, aliasing the slab; callers must not modify it. The
// result's capacity ends at the key itself, so append reallocates rather than
// overwriting the next key.
//
// Out-of-range panics via the compiler's generated bounds check rather than an
// explicit one: naming the index and count here costs At its inlining — 271
// against an 80 budget, versus 53 without — and At is called per key by
// readers that need keys out of order. [SortedKeys.All] reads the slab
// directly and pays neither cost.
//
// One case the generated check misses: a zero width, where i*0 legally slices
// to [0:0:0] against any slab. Refused explicitly in the body, or an unbuilt
// SortedKeys would answer every index with an empty key instead of panicking.
//
// One case neither checks: an index near maxint can wrap i*k.w into a legal
// range and return the wrong key. No caller holds one — Len bounds every
// derived index — and guarding it slows [BenchmarkIterate]'s width arm, so
// this is documented rather than enforced.
func (k SortedKeys) At(i int) []byte {
	if k.offs == nil {
		if k.w <= 0 {
			panic(errUnbuiltKeys)
		}
		return k.slab[i*k.w : (i+1)*k.w : (i+1)*k.w]
	}
	return k.slab[k.offs[i]:k.offs[i+1]:k.offs[i+1]]
}

// All iterates the keys in order, yielding each key's position and bytes,
// aliasing the slab.
//
// One func literal covers both layouts: separate ones would keep the compiler
// from devirtualizing the yield, heap-allocating the caller's loop body.
//
// The layout is branched on once, outside the loop, and keys are sliced
// directly rather than via At, which would add a branch and a redundant range
// check per key. BenchmarkIterate: 38us vs 159us over 100,000 keys.
func (k SortedKeys) All() iter.Seq2[int, []byte] {
	return func(yield func(int, []byte) bool) {
		if k.offs == nil {
			if k.w <= 0 {
				return
			}
			for i := 0; (i+1)*k.w <= len(k.slab); i++ {
				if !yield(i, k.slab[i*k.w:(i+1)*k.w:(i+1)*k.w]) {
					return
				}
			}
			return
		}
		for i := 0; i+1 < len(k.offs); i++ {
			if !yield(i, k.slab[k.offs[i]:k.offs[i+1]:k.offs[i+1]]) {
				return
			}
		}
	}
}

// Sub returns the keys in [from, to) as a list of their own, aliasing this
// one's storage rather than copying it. 0 <= from <= to <= Len().
//
// The check exists because the slice expressions would otherwise panic with a
// bare bounds error — and because a zero-width layout slices to an empty list
// at any index, where they would not panic at all.
func (k SortedKeys) Sub(from, to int) SortedKeys {
	if from < 0 || to < from || to > k.Len() {
		panic(fmt.Errorf("%w: range [%d,%d) is not within a list of %d keys",
			ErrInternal, from, to, k.Len()))
	}
	// Len treats offs without a terminator as empty, so route that shape to the
	// width arm, which yields an empty list.
	if len(k.offs) == 0 {
		return SortedKeys{slab: k.slab[from*k.w : to*k.w : to*k.w], w: k.w}
	}
	return SortedKeys{slab: k.slab, offs: k.offs[from : to+1 : to+1]}
}

// FirstAtOrAfter returns the first position in [from, to) whose key is at or
// past target, or to if there is none. It searches a sub-range rather than the
// whole list, which is what a caller walking two sorted runs past each other
// needs.
//
// from is a hint, not a bound the answer has to clear: a target already at or
// before keys[from] answers from.
//
// to is required, not a hint: 0 <= from <= to <= Len(), unchecked. Trimming a
// range the caller got wrong would answer as if they had got it right.
func (k SortedKeys) FirstAtOrAfter(from, to int, target []byte) int {
	lo, hi := from, from
	for step := 1; hi < to && bytes.Compare(k.At(hi), target) < 0; step *= 2 {
		lo = hi + 1
		hi = lo + step
	}
	if hi > to {
		hi = to
	}
	// Everything before lo is before target, and hi is either past the window or
	// at or past target, so the answer is in [lo, hi].
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if bytes.Compare(k.At(mid), target) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// isAscending reports whether the keys are ordered. Off the query path — it
// exists so tests can pin the invariant each producer promises.
func (k SortedKeys) isAscending() bool {
	for i := 1; i < k.Len(); i++ {
		if bytes.Compare(k.At(i-1), k.At(i)) > 0 {
			return false
		}
	}
	return true
}

// VarKeyBuilder fills a [SortedKeys] whose keys vary in length, appended in
// any order; [VarKeyBuilder.Build] sorts them and rebuilds the slab to match.
//
// The layout isn't chosen by watching appended lengths here — that per-key
// test would push AppendString past Go's inlining budget.
type VarKeyBuilder struct {
	slab []byte
	offs []uint32
	// n duplicates len(offs)-1, so the two disagree exactly when the
	// constructor was skipped; Build refuses that.
	n int
}

// NewVarKeyBuilder sizes both arrays up front so neither grows, since a
// producer knows its key count and total byte length before it starts. It
// panics on negative arguments.
//
// totalBytes is the batch total, not a per-key width — the reverse of
// [NewFixedKeyBuilder]'s argument, and the compiler can't catch the swap.
func NewVarKeyBuilder(numKeys, totalBytes int) *VarKeyBuilder {
	// A negative count would size offs to numKeys+1 == 0 and hand back a
	// builder that quietly grows instead of staying pre-sized.
	if numKeys < 0 || totalBytes < 0 {
		panic(fmt.Errorf("%w: key count %d and total bytes %d must not be negative",
			ErrInternal, numKeys, totalBytes))
	}
	return &VarKeyBuilder{
		slab: make([]byte, 0, totalBytes),
		offs: append(make([]uint32, 0, numKeys+1), 0),
	}
}

// AppendString copies a key onto the slab without materializing a []byte for
// the conversion.
func (b *VarKeyBuilder) AppendString(key string) {
	b.slab = append(b.slab, key...)
	b.offs = append(b.offs, uint32(len(b.slab)))
	b.n++
}

// Build orders the keys, drops duplicates, and returns the finished list,
// consuming the builder. Ordering happens here rather than in a separate Sort
// call, so the invariant [SortedKeys] promises can't be skipped.
//
// The result can be shorter than the number of appends — duplicates are
// dropped (see [dedupFixed], [dedupVariable]) — so a caller needing its own
// count must not substitute this one. Keys that share a width come back
// without offsets; the sort already scans for that, so it costs nothing extra.
//
// Scratch isn't pooled: measured no faster at any batch size, and pooling
// would hold a large batch's buffers for the process's lifetime.
func (b *VarKeyBuilder) Build() (SortedKeys, error) {
	// offs carries a leading zero, so a filled builder holds one more offset
	// than keys; a mismatch means the constructor was skipped (reachable via
	// an exported zero-value composite literal).
	if len(b.offs) != b.n+1 {
		return SortedKeys{}, fmt.Errorf("%w: VarKeyBuilder holds %d keys against %d "+
			"offsets; NewVarKeyBuilder was not used", ErrInternal, b.n, len(b.offs))
	}
	// Checked once here rather than per key: offsets are uint32 and
	// AppendString can't afford a bound per append, but len(slab) is still an
	// honest int after the fact. Not an [ErrInternal] — a batch this large is
	// what the caller asked for — and kept despite being unreachable for the
	// current production caller, since the builder is exported.
	if uint64(len(b.slab)) > math.MaxUint32 {
		return SortedKeys{}, fmt.Errorf("inverted: batch holds %d bytes of keys, "+
			"above the uint32 offset ceiling of %d", len(b.slab), uint32(math.MaxUint32))
	}
	slab, offs, w := sortKeys(b.slab, b.offs)
	if w > 0 {
		n, err := dedupFixed(slab, w, b.n)
		if err != nil {
			return SortedKeys{}, err
		}
		// Capped at the last surviving key so an index past Len cannot reach the
		// deduped tail, and copied down when that tail is most of the array.
		return SortedKeys{slab: shrinkKeys(slab, n*w), w: w}, nil
	}
	n, err := dedupVariable(slab, offs, b.n)
	if err != nil {
		return SortedKeys{}, err
	}
	return SortedKeys{
		slab: shrinkKeys(slab, int(offs[n])),
		offs: shrinkOffs(offs, n+1),
	}, nil
}

// FixedKeyBuilder fills a [SortedKeys] whose keys are all one width. It records
// no offsets, since key i is at i*w, and the type is what keeps that true:
// nothing can append a key of another length to it.
type FixedKeyBuilder struct {
	slab []byte
	w    int
}

// NewFixedKeyBuilder sizes the slab for numKeys keys of keyWidth bytes each,
// and panics on invalid arguments.
//
// keyWidth is one key's width, not the batch total — the reverse of
// [NewVarKeyBuilder]'s argument, and the compiler can't catch the swap. A
// width above the widest key any family encodes is rejected here, before it
// silently produces wrong-width keys that would sort and dedup normally.
func NewFixedKeyBuilder(numKeys, keyWidth int) *FixedKeyBuilder {
	if keyWidth <= 0 || keyWidth > maxKeyWidth {
		panic(fmt.Errorf("%w: key width %d is not in 1..%d; NewFixedKeyBuilder "+
			"takes the width of one key, not the batch total",
			ErrInternal, keyWidth, maxKeyWidth))
	}
	if numKeys < 0 {
		panic(fmt.Errorf("%w: key count %d is negative", ErrInternal, numKeys))
	}
	return &FixedKeyBuilder{slab: make([]byte, 0, numKeys*keyWidth), w: keyWidth}
}

// maxKeyWidth is the widest key any fixed-width family encodes — a uuid, at
// 16. It also lets AppendBuf extend the slab from zeroKey without sizing a
// buffer.
const maxKeyWidth = len(zeroKey)

// AppendBuf appends a key and returns its zeroed bytes for the encoder to
// write into directly. The slice is capped at its own end, so writing through
// it cannot reach the next key.
//
// Valid only until the next append or Build: a buffer held across a slab
// growth would alias the stale array, and one held across Build would land on
// whatever key the sort moved into that slot.
func (b *FixedKeyBuilder) AppendBuf() []byte {
	// A zero width would hand back an empty buffer and let the encoder die
	// several frames away with an unhelpful index-out-of-range. Panics rather
	// than returning an error since none can be returned through here.
	if b.w <= 0 {
		panic(fmt.Errorf("%w: FixedKeyBuilder has a key width of %d; "+
			"NewFixedKeyBuilder was not used", ErrInternal, b.w))
	}
	start := len(b.slab)
	b.slab = append(b.slab, zeroKey[:b.w]...)
	return b.slab[start : start+b.w : start+b.w]
}

// zeroKey backs AppendBuf's extension, and its length is the width bound every
// builder is checked against. A family wider than this has to grow the array,
// not just the bound — AppendBuf extends the slab from it and sizes nothing.
var zeroKey [16]byte

// shrinkFloor is the array size below which reclaiming a deduped tail is not
// worth a copy. Small batches are the common ones and would pay the copy for
// bytes nobody misses.
const shrinkFloor = 4 << 10

// shrinkKeys ends the slab at the last surviving key, and copies it onto its
// own array when what is left behind is most of it.
//
// Both results are capped at that key: an aliased one so an index past it
// cannot reach the dropped tail, a copied one because append rounds up to a
// size class and At bounds against capacity, which would otherwise answer an
// index past the last key with a zero one instead of panicking.
//
// Dedup can leave a handful of keys inside an allocation sized for the whole
// batch — a boolean filter over 100,000 values keeps two — and the list is
// held for the query's lifetime, keeping the dead tail alive with it. Capping
// alone hides that rather than releasing it, so the copy decision is made from
// the pre-cap array size.
//
// The ratio lets a batch that dropped nothing skip the copy, at a worst case
// of three quarters wasted: dedupping 100,000 keys to 26,000 holds 800KB for
// 208KB of live keys.
func shrinkKeys(slab []byte, end int) []byte {
	if cap(slab) < shrinkFloor || cap(slab) <= 4*end {
		return slab[:end:end]
	}
	out := append([]byte(nil), slab[:end]...)
	return out[:end:end]
}

// shrinkOffs is shrinkKeys for the offsets, which are sized to the pre-dedup key
// count and go dead in the same proportion. Its floor is the same number of
// bytes, not of entries, so the two decide independently: 4095 offsets are worth
// reclaiming where 4095 bytes are not.
func shrinkOffs(offs []uint32, end int) []uint32 {
	if 4*cap(offs) < shrinkFloor || cap(offs) <= 4*end {
		return offs[:end:end]
	}
	out := append([]uint32(nil), offs[:end]...)
	return out[:end:end]
}

// Build orders the keys in place, drops duplicates, and returns the finished
// list, consuming the builder — see [VarKeyBuilder.Build] for why.
//
// At one width, key i sits at offset i*w, so ordering moves only the slab and
// there are no offsets to reorder alongside it — and what lets these keys be
// radix sorted rather than compared.
func (b *FixedKeyBuilder) Build() (SortedKeys, error) {
	// A width is a constant of the family being encoded, so a non-positive one
	// is a builder that skipped its constructor. Refused rather than returned as
	// an empty list, which reads downstream as a filter that matched nothing.
	if b.w <= 0 {
		return SortedKeys{}, fmt.Errorf("%w: FixedKeyBuilder has a key width of %d; "+
			"NewFixedKeyBuilder was not used", ErrInternal, b.w)
	}
	sortFixedWidth(b.slab, b.w)
	n, err := dedupFixed(b.slab, b.w, len(b.slab)/b.w)
	if err != nil {
		return SortedKeys{}, err
	}
	// Capped at the last surviving key so an index past Len cannot reach the
	// deduped tail, and copied down when that tail is most of the array.
	return SortedKeys{slab: shrinkKeys(b.slab, n*b.w), w: b.w}, nil
}

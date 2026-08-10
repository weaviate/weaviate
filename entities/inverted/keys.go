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

// SortedKeys is an ascending, immutable list of encoded index keys, held as one
// slab plus per-key offsets rather than as a [][]byte whose 24-byte headers
// would outweigh the 8-to-16-byte keys they describe. Keys that share a width
// carry the width instead of offsets, since key i then starts at i*w.
//
// Only a builder's Build returns one, so the order cannot be lost downstream
// and no consumer re-checks it. Build verifies it instead, while dropping
// duplicates — so a list holds distinct keys, and can be shorter than the
// values a filter named.
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
// Derived rather than stored. Each layout already carries the count — one
// offset per key plus a terminator, or a slab that is exactly n keys wide — and
// a stored count would be a third place for them to disagree. Routing reads
// this, so a count out of step with the arrays would send a filter down the
// wrong path rather than merely iterate wrong.
func (k SortedKeys) Len() int {
	if k.offs != nil {
		// An offsets array with no terminator is not a shape the builders
		// produce. Reporting -1 for it would pass every "no keys" guard, which
		// all test for zero, and then fail the "has keys" test that routes to
		// the batched fold — so the leaf would fall through to the per-value
		// path with no value set, and answer from the wrong bucket.
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

// At returns key i, aliasing the slab. Callers must not modify it. Its capacity
// stops at its own end, so appending to it reallocates rather than writing over
// the next key.
//
// Use [SortedKeys.All] to walk the list. The range check is what makes At too
// large to inline — its cost is 271 against a budget of 80, and 45 without the
// two panic paths — so a loop driven by At pays a call per key, which
// BenchmarkIterate measures at about four times the cost of iterating. That is
// the price of the check, and All is how the query path avoids paying it.
//
// The range is checked here rather than left to the two layouts, which disagree
// about it: the offsets arm would panic on its own index, the width arm reads
// slab[0:0:0] for every index when the width is zero, and a zero value has no
// width at all. One check makes all three refuse the same way.
func (k SortedKeys) At(i int) []byte {
	if k.offs == nil {
		// Multiplied rather than dividing to derive the count, which would be a
		// division per key. An index large enough to overflow the product still
		// panics, on the slice expression rather than here, so the cost is the
		// message and not safety.
		if i < 0 || k.w <= 0 || (i+1)*k.w > len(k.slab) {
			panic(outOfRange(i, k.Len()))
		}
		return k.slab[i*k.w : (i+1)*k.w : (i+1)*k.w]
	}
	if i < 0 || i+1 >= len(k.offs) {
		panic(outOfRange(i, k.Len()))
	}
	return k.slab[k.offs[i]:k.offs[i+1]:k.offs[i+1]]
}

// outOfRange builds the value At panics with. It carries [ErrInternal] like the
// returned faults do: if the recovery interceptor whose absence sends Build's
// errors back through the return ever arrives, a recovered panic should be
// classifiable the same way rather than arriving as an opaque string.
func outOfRange(i, n int) error {
	return fmt.Errorf("%w: key %d requested from a list of %d", ErrInternal, i, n)
}

// All iterates the keys in order, yielding each key's position and bytes, which
// alias the slab.
//
// One func literal covers both layouts. With one per layout the compiler cannot
// tell which iterator a caller received, so it cannot devirtualize the yield and
// heap-allocates the caller's loop body instead.
//
// The layout is branched on once, outside the loop, and the keys are sliced
// here rather than through At, which would pay both the branch and a range
// check per key to re-prove what the loop condition already guarantees.
// BenchmarkIterate measures the two: 38us against 159us over 100,000 keys.
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

// VarKeyBuilder fills a [SortedKeys] whose keys vary in length. Keys may be
// appended in any order; because a key's offset then depends on the lengths
// before it, ordering them means sorting a permutation and rebuilding the slab
// in that order, both of which happen in [VarKeyBuilder.Build].
//
// The builder a producer picks is what chooses the layout. Deciding it here
// instead — watching the lengths appended — costs a per-key test that pushes
// AppendString past Go's inlining budget.
type VarKeyBuilder struct {
	slab []byte
	offs []uint32
	// n counts the appends, duplicating len(offs)-1 so the two disagree exactly
	// when the constructor was skipped. Build refuses that.
	n int
}

// NewVarKeyBuilder sizes both arrays up front so neither grows: a producer knows
// its key count and total byte length before it starts. It panics on negative
// arguments.
//
// totalBytes is the batch's total, not a per-key width — the opposite of the
// neighbouring [NewFixedKeyBuilder], which the arguments cannot distinguish.
func NewVarKeyBuilder(numKeys, totalBytes int) *VarKeyBuilder {
	// Refused here rather than left to make: a negative count sizes offs to
	// numKeys+1 == 0 and hands back a builder that works while quietly growing,
	// which is the one promise this constructor exists to make.
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
// consuming the builder.
//
// Ordering here, rather than in a Sort the producer has to remember to call,
// leaves no way to skip the invariant [SortedKeys] is named for. Dropping
// duplicates is then a linear pass over the same comparisons — see [dedupFixed]
// and [dedupVariable] — so the list can be shorter than the appends that filled
// it, and a caller needing its own count must not take this one for it.
//
// Keys that share a width come back in the layout carrying no offsets. The sort
// scans them to choose its method regardless, so the layout costs nothing to
// learn and saves four bytes per key for the query's lifetime.
//
// Scratch is sized to the batch. Its rebuilt slab and offsets become the
// returned list; the rest dies with the sort. Pooling measured no faster at any
// batch size and would hold a large batch's buffers for the process's lifetime.
func (b *VarKeyBuilder) Build() (SortedKeys, error) {
	// offs carries a leading zero, so a filled builder holds one more offset
	// than keys. Without it every key reads a position over and the last is
	// lost, narrowing a filter result with nothing to report it. Reachable from
	// outside the package: an empty composite literal needs no exported field.
	if len(b.offs) != b.n+1 {
		return SortedKeys{}, fmt.Errorf("%w: VarKeyBuilder holds %d keys against %d "+
			"offsets; NewVarKeyBuilder was not used", ErrInternal, b.n, len(b.offs))
	}
	// Offsets are uint32 and AppendString cannot afford a bound per key, but
	// len(slab) is still an honest int here, so the ceiling is checked once
	// after the fact. Past it the offsets have wrapped and stopped ascending,
	// which reads as wrong keys rather than as a failure.
	// Deliberately not an [ErrInternal]: a batch this large is what the filter
	// asked for, not this package being wrong, and an operator reading it as an
	// internal fault would look in the wrong place. Not covered by any test —
	// reaching it needs 4GiB of keys in one filter — and unreachable for the
	// only production caller, which bounds the same total through
	// [tokenizer.AnalyzedBatch.SingleTokenBytes] before sizing this builder —
	// AnalyzeBatch bounds the token count, which is a different quantity. It is
	// kept because the builder is exported and the next producer may not.
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

// NewFixedKeyBuilder sizes the slab for numKeys keys of keyWidth bytes each. It
// panics on arguments it cannot build from.
//
// keyWidth is the width of one key, not the batch's total — the neighbouring
// [NewVarKeyBuilder] takes a total, and the two are otherwise identical in
// shape. Passing a total here is accepted by the compiler and produces keys of
// the wrong width that sort and dedup normally, so it is refused at the point
// the mistake is made: a width past the widest key any family encodes is
// rejected here rather than diagnosed inside an encoder writing into a buffer
// of the wrong size.
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

// maxKeyWidth is the widest key any fixed-width family encodes — a uuid, at 16.
// Bounding the width here is what makes a batch total passed as a width fail at
// the call rather than silently produce keys of the wrong shape, and it is what
// lets AppendBuf extend the slab from zeroKey without ever sizing a buffer.
const maxKeyWidth = len(zeroKey)

// AppendBuf appends a key and returns its bytes, zeroed, for the encoder to
// write into — cheaper than encoding into a temporary and copying it in. The
// returned slice is capped at its own end, so writing through it cannot reach
// the next key.
//
// It is valid until the next append or Build, whichever comes first. The slab
// is sized for the key count the constructor was given, so it does not grow in
// practice; a buffer held across a growth would alias the old array and lose
// what was written through it, and one held across Build would land on whatever
// key the sort moved into that slot.
func (b *FixedKeyBuilder) AppendBuf() []byte {
	// A zero width would hand back an empty buffer and leave the encoder to
	// die writing into it, several frames from the mistake and with nothing
	// but an index-out-of-range to go on. Build's guard cannot reach that: the
	// encoder runs first. Panics rather than returning because there is no
	// error to return through, and the width came from a constructor argument.
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
// An aliased result is capped at that key so an index past it cannot reach the
// dropped tail. A copied one may carry spare capacity, which append rounds up
// to a size class — but those bytes are a fresh allocation, not the tail.
//
// Dedup can leave a handful of keys inside an allocation sized for the whole
// batch — a boolean filter over 100,000 values keeps two — and the finished
// list is held for as long as the query runs, so the dead tail is held with it.
// Capping alone does not release it, and hides it: the returned slice reports
// the small capacity while the whole array stays reachable. So the decision is
// made from the array the keys came in, before it is capped.
//
// The ratio keeps a batch that dropped nothing from copying at all, and leaves
// a worst case of three quarters wasted: a batch dedupping 100,000 keys to
// 26,000 holds 800KB for 208KB of keys rather than pay the copy.
func shrinkKeys(slab []byte, end int) []byte {
	if cap(slab) < shrinkFloor || cap(slab) <= 4*end {
		return slab[:end:end]
	}
	return append([]byte(nil), slab[:end]...)
}

// shrinkOffs is shrinkKeys for the offsets, which are sized to the pre-dedup key
// count and go dead in the same proportion. Its floor is the same number of
// bytes, not of entries, so the two decide independently: 4095 offsets are worth
// reclaiming where 4095 bytes are not.
func shrinkOffs(offs []uint32, end int) []uint32 {
	if 4*cap(offs) < shrinkFloor || cap(offs) <= 4*end {
		return offs[:end:end]
	}
	return append([]uint32(nil), offs[:end]...)
}

// Build orders the keys in place, drops duplicates, and returns the finished
// list, consuming the builder. Both happen here for the reasons given on
// [VarKeyBuilder.Build].
//
// At one width key i sits at offset i*w, so ordering moves only the slab and
// there are no offsets to reorder alongside it. That is also what lets these
// keys be radix sorted rather than compared.
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

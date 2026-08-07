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
	"iter"
)

// Keys is an immutable list of encoded index keys held as one slab plus per-key
// offsets rather than as a [][]byte.
//
// A []byte header costs 24 bytes to describe a key that is typically 8 to 16
// bytes, so at batch sizes the descriptors outweigh what they point at: a
// 100,000-key filter spends 2.4MB on headers against 1.2MB of keys. Offsets
// cost 4 bytes each, and because they ascend with the slab, walking the keys
// reads it front to back instead of chasing pointers around it.
//
// Keys of one width — every family but text — need no offsets at all, since key
// i starts at i*w. Those lists carry the width instead and hold the slab alone,
// which is the whole 4-bytes-per-key index gone rather than shrunk. Reading is
// one type either way: the layout costs a branch in At, which measures cheaper
// than the offset loads it replaces, where two types would cost an indirect call
// per corpus row.
//
// The zero value is a valid empty list, which is what a leaf that is not a
// batched Contains carries.
type Keys struct {
	slab []byte
	// offs has one entry per key plus a terminator, and is nil when the keys
	// share a width.
	offs []uint32
	// w is the shared key width, read only when offs is nil.
	w int
	// n is the key count, which neither offs nor w alone gives in both layouts.
	n int
}

func (k Keys) Len() int { return k.n }

// At returns key i, aliasing the slab. Callers must not modify it. Its capacity
// stops at its own end, so appending to it reallocates instead of writing over
// the next key.
func (k Keys) At(i int) []byte {
	if k.offs == nil {
		return k.slab[i*k.w : (i+1)*k.w : (i+1)*k.w]
	}
	return k.slab[k.offs[i]:k.offs[i+1]:k.offs[i+1]]
}

// All iterates the keys in order, yielding each key's position and bytes. The
// bytes alias the slab.
//
// One func literal, not one per layout: with two the compiler cannot tell which
// iterator a caller received, so it cannot devirtualize the yield call and
// heap-allocates the caller's loop body — several allocations per query on the
// batched Contains path, enough to matter where a query is microseconds. One
// literal keeps everything statically known and stack-allocated, at the cost of
// the layout branch moving inside the loop, which is predicted and far cheaper.
func (k Keys) All() iter.Seq2[int, []byte] {
	return func(yield func(int, []byte) bool) {
		for i := 0; i < k.n; i++ {
			if !yield(i, k.At(i)) {
				return
			}
		}
	}
}

// SizeBytes is the memory the list holds, for allocation accounting.
func (k Keys) SizeBytes() int { return cap(k.slab) + 4*cap(k.offs) }

// KeyBuilder fills a [Keys] whose keys vary in length — the text path, and
// nothing else today.
//
// Which builder a producer picks is how the layout is chosen. Deciding it here
// instead — watching the lengths appended — costs a per-key test that pushes
// AppendString past Go's inlining budget, and a call per key measured several
// percent of a large query.
type KeyBuilder struct {
	slab []byte
	offs []uint32
}

// NewKeyBuilder sizes both arrays up front: the producer knows its key count and
// total byte length before it starts, so neither array grows.
func NewKeyBuilder(numKeys, totalBytes int) *KeyBuilder {
	return &KeyBuilder{
		slab: make([]byte, 0, totalBytes),
		offs: append(make([]uint32, 0, numKeys+1), 0),
	}
}

// AppendString copies a key onto the slab without materializing a []byte for
// the conversion — the text path's tokens are strings.
func (b *KeyBuilder) AppendString(key string) {
	b.slab = append(b.slab, key...)
	b.offs = append(b.offs, uint32(len(b.slab)))
}

func (b *KeyBuilder) Build() Keys {
	return Keys{slab: b.slab, offs: b.offs, n: len(b.offs) - 1}
}

// FixedKeyBuilder fills a [Keys] whose keys are all one width — int, number,
// date, bool and uuid, whose encoders write a key of known size each.
// It records no offsets, since key i is at i*w, and needs no per-key
// bookkeeping to know that: the type says so, so nothing can append a key of
// another length to it.
type FixedKeyBuilder struct {
	slab []byte
	w    int
}

// NewFixedKeyBuilder sizes the slab for numKeys keys of width bytes each.
func NewFixedKeyBuilder(numKeys, width int) *FixedKeyBuilder {
	return &FixedKeyBuilder{slab: make([]byte, 0, numKeys*width), w: width}
}

// AppendBuf appends a key and returns its bytes, zeroed, for the encoder to
// write into — cheaper than encoding into a temporary and copying it in. The
// returned slice is capped at its own end, so writing through it cannot reach
// the next key.
func (b *FixedKeyBuilder) AppendBuf() []byte {
	start := len(b.slab)
	if b.w <= len(zeroKey) {
		b.slab = append(b.slab, zeroKey[:b.w]...)
	} else {
		b.slab = append(b.slab, make([]byte, b.w)...)
	}
	return b.slab[start : start+b.w : start+b.w]
}

// zeroKey backs AppendBuf's extension. Sized to the widest fixed key there is, a
// 16-byte uuid.
var zeroKey [16]byte

func (b *FixedKeyBuilder) Build() Keys {
	return Keys{slab: b.slab, w: b.w, n: len(b.slab) / b.w}
}

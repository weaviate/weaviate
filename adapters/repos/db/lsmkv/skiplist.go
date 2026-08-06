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
	"bytes"
	"encoding/binary"
	"sync/atomic"
)

// skipList is a generic single-writer, lock-free-read ordered index: []byte keys,
// each accumulating an append-only log of V that an adapter (e.g. skipListMap)
// reduces. insert() must be externally serialized; get()/forEach() take no lock.
//
// Lock-free reads are safe because a skip list never rewires existing nodes (no
// rotations, unlike a red-black tree) and each value log is append-only and
// atomically published: a reader that Loads a pointer/count sees every write the
// writer made before that Store (Go atomics are release/acquire), so it observes
// a consistent prefix, never a torn node.
const (
	skipListMaxHeight = 16
	// A value log is a chain of chunks whose backing arrays grow geometrically
	// (firstValueChunkSize -> ... -> maxValueChunkSize), each sized once and never
	// resized. BM25 vocabularies are ~99% single-posting, so a small first chunk
	// keeps the common case near the red-black tree's footprint — a fixed 16-slot
	// first chunk allocated a full ~1KB size class on the first insert of every key.
	// Later chunks are larger to amortize allocation and pointer-chasing for hot terms.
	firstValueChunkSize = 2
	maxValueChunkSize   = 16
	// A node carries its forward pointers, its value log and that log's first
	// chunk inline, so adding a key costs one allocation instead of five. With
	// p=1/4 towers above this height are <1% of nodes and fall back to a heap
	// slice for the overflow levels.
	skipListInlineHeight = 4
)

// valueChunk is a single-producer append block: the writer fills entries[n] then
// stores n+1 to publish it, so a reader that loads n sees entries[:n] complete.
// entries is sized once at construction and never resized, so its backing array
// never moves — that immutability is what keeps entries[:n] safe for a lock-free reader.
type valueChunk[V any] struct {
	entries []V
	n       atomic.Int32
	next    atomic.Pointer[valueChunk[V]]
}

func newValueChunk[V any](capacity int, first V) *valueChunk[V] {
	c := &valueChunk[V]{entries: make([]V, capacity)}
	c.entries[0] = first
	c.n.Store(1)
	return c
}

type valueLog[V any] struct {
	head  *valueChunk[V] // immutable after creation
	tail  *valueChunk[V] // writer-only
	count atomic.Int32   // total entries; lets a reader pre-size snapshot() to one alloc
}

// init points the log at storage the caller owns (a node's inline first chunk),
// so no part of an empty log is separately allocated.
func (vl *valueLog[V]) init(first *valueChunk[V], entries []V) {
	first.entries = entries
	vl.head = first
	vl.tail = first
}

// append adds v to the log and returns the number of value slots newly allocated
// (0 unless the chunk was full and a new one had to be created). writer-only.
func (vl *valueLog[V]) append(v V) int {
	t := vl.tail
	n := t.n.Load()
	if int(n) < len(t.entries) {
		t.entries[n] = v
		t.n.Store(n + 1) // publish the entry...
		vl.count.Add(1)  // ...then bump count, so count never exceeds published entries
		return 0
	}
	nextCap := len(t.entries) * 2
	if nextCap > maxValueChunkSize {
		nextCap = maxValueChunkSize
	}
	c := newValueChunk(nextCap, v)
	t.next.Store(c) // publish the new chunk (its first entry is already published)
	vl.tail = c
	vl.count.Add(1)
	return nextCap
}

// snapshot returns a consistent prefix of the log, pre-sized from count so the
// common (quiescent) read is a single right-sized allocation. Lock-free.
func (vl *valueLog[V]) snapshot() []V {
	out := make([]V, 0, int(vl.count.Load()))
	for c := vl.head; c != nil; c = c.next.Load() {
		n := int(c.n.Load())
		out = append(out, c.entries[:n]...)
		if n < len(c.entries) {
			// The chunk was not full at the moment n was loaded. The writer may
			// have filled it and linked a successor since; following next would
			// splice newer entries after a gap of unseen ones, so stop here —
			// what was gathered is a consistent prefix.
			break
		}
	}
	return out
}

// keyPrefix packs the first 8 bytes of a key, zero-padded, into an integer that
// orders the same way the bytes do. Equal prefixes are ties, not matches, so a
// caller must fall back to comparing the full keys.
func keyPrefix(key []byte) uint64 {
	var b [8]byte
	copy(b[:], key)
	return binary.BigEndian.Uint64(b[:])
}

// A node is allocated once and never moved or copied: vlog.head points into
// first, and first.entries aliases inline, so relocating it would dangle both.
//
// pre/next/nextInline are the descent's entire working set for a node and are
// laid out first so they share one cache line: a search resolves most steps
// without touching key, which lives in a separate allocation.
type skipListNode[V any] struct {
	pre        uint64
	next       []atomic.Pointer[skipListNode[V]] // len == height of this node
	nextInline [skipListInlineHeight]atomic.Pointer[skipListNode[V]]
	key        []byte
	vlog       valueLog[V]
	first      valueChunk[V]
	inline     [firstValueChunkSize]V
}

func newSkipListNode[V any](key []byte, height int) *skipListNode[V] {
	n := &skipListNode[V]{key: key, pre: keyPrefix(key)}
	n.vlog.init(&n.first, n.inline[:])
	if height <= skipListInlineHeight {
		n.next = n.nextInline[:height]
	} else {
		n.next = make([]atomic.Pointer[skipListNode[V]], height)
	}
	return n
}

// before reports whether n sorts strictly before key, comparing the inline
// prefix first and only dereferencing n.key when prefixes tie.
func (n *skipListNode[V]) before(pre uint64, key []byte) bool {
	if n.pre != pre {
		return n.pre < pre
	}
	return bytes.Compare(n.key, key) < 0
}

type skipList[V any] struct {
	head   *skipListNode[V] // sentinel; next has skipListMaxHeight slots
	height int              // highest level in use; writer-only
	rng    uint64           // xorshift state; writer-only
}

func newSkipList[V any]() *skipList[V] {
	return &skipList[V]{
		head:   newSkipListNode[V](nil, skipListMaxHeight),
		height: 1,
		rng:    0x9e3779b97f4a7c15,
	}
}

// writer-only
func (s *skipList[V]) randomHeight() int {
	x := s.rng
	x ^= x << 13
	x ^= x >> 7
	x ^= x << 17
	s.rng = x
	// p=1/4: shorter towers than p=1/2 mean fewer forward pointers per node and
	// fewer levels to descend, at the same O(log n) expected search cost.
	h := 1
	for h < skipListMaxHeight && x&3 == 0 {
		h++
		x >>= 2
	}
	return h
}

// insert adds v under key and returns the number of value slots newly allocated,
// so the caller can account the value-log backing growth. writer-only.
func (s *skipList[V]) insert(key []byte, v V) int {
	pre := keyPrefix(key)
	var preds [skipListMaxHeight]*skipListNode[V]
	x := s.head
	for lvl := s.height - 1; lvl >= 0; lvl-- {
		for {
			nxt := x.next[lvl].Load()
			if nxt == nil || !nxt.before(pre, key) {
				break
			}
			x = nxt
		}
		preds[lvl] = x
	}

	if nxt := x.next[0].Load(); nxt != nil && bytes.Equal(nxt.key, key) {
		return nxt.vlog.append(v) // existing key: no topology change
	}

	h := s.randomHeight()
	n := newSkipListNode[V](key, h)
	n.vlog.append(v)
	if h > s.height {
		for lvl := s.height; lvl < h; lvl++ {
			preds[lvl] = s.head
		}
		s.height = h
	}
	// Set the new node's forward pointers first, then publish it into each
	// predecessor bottom-up so a reader always finds it at level 0.
	for lvl := 0; lvl < h; lvl++ {
		n.next[lvl].Store(preds[lvl].next[lvl].Load())
	}
	for lvl := 0; lvl < h; lvl++ {
		preds[lvl].next[lvl].Store(n)
	}
	return firstValueChunkSize // the new node's first value chunk
}

// insertMany adds all of vs under key in one descent and returns the number of
// value slots newly allocated. Unlike repeated insert calls, an empty vs still
// materializes the key (with an empty value log), matching the red-black trees,
// which create a node even when handed no values. writer-only.
func (s *skipList[V]) insertMany(key []byte, vs []V) int {
	pre := keyPrefix(key)
	var preds [skipListMaxHeight]*skipListNode[V]
	x := s.head
	for lvl := s.height - 1; lvl >= 0; lvl-- {
		for {
			nxt := x.next[lvl].Load()
			if nxt == nil || !nxt.before(pre, key) {
				break
			}
			x = nxt
		}
		preds[lvl] = x
	}

	if nxt := x.next[0].Load(); nxt != nil && bytes.Equal(nxt.key, key) {
		slots := 0
		for _, v := range vs {
			slots += nxt.vlog.append(v)
		}
		return slots
	}

	h := s.randomHeight()
	n := newSkipListNode[V](key, h)
	slots := firstValueChunkSize
	// the node is not yet linked, so these appends are invisible until publication
	for _, v := range vs {
		slots += n.vlog.append(v)
	}
	if h > s.height {
		for lvl := s.height; lvl < h; lvl++ {
			preds[lvl] = s.head
		}
		s.height = h
	}
	for lvl := 0; lvl < h; lvl++ {
		n.next[lvl].Store(preds[lvl].next[lvl].Load())
	}
	for lvl := 0; lvl < h; lvl++ {
		preds[lvl].next[lvl].Store(n)
	}
	return slots
}

// get is lock-free. It descends from the max height (unused upper levels are
// nil) rather than the writer-only height field, which a reader must not touch.
func (s *skipList[V]) get(key []byte) ([]V, bool) {
	pre := keyPrefix(key)
	x := s.head
	for lvl := skipListMaxHeight - 1; lvl >= 0; lvl-- {
		for {
			nxt := x.next[lvl].Load()
			if nxt == nil || !nxt.before(pre, key) {
				break
			}
			x = nxt
		}
	}
	if nxt := x.next[0].Load(); nxt != nil && bytes.Equal(nxt.key, key) {
		return nxt.vlog.snapshot(), true
	}
	return nil, false
}

// forEach visits keys in ascending order, lock-free. On a live index it is a
// point-in-time view: keys inserted mid-walk may be skipped.
func (s *skipList[V]) forEach(fn func(key []byte, values []V)) {
	for x := s.head.next[0].Load(); x != nil; x = x.next[0].Load() {
		fn(x.key, x.vlog.snapshot())
	}
}

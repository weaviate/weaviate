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
	valueChunkSize    = 16
)

// valueChunk is a single-producer append block: the writer fills entries[n] then
// stores n+1 to publish it, so a reader that loads n sees entries[:n] complete.
type valueChunk[V any] struct {
	entries [valueChunkSize]V
	n       atomic.Int32
	next    atomic.Pointer[valueChunk[V]]
}

type valueLog[V any] struct {
	head *valueChunk[V] // immutable after creation
	tail *valueChunk[V] // writer-only
}

func newValueLog[V any](first V) *valueLog[V] {
	c := &valueChunk[V]{}
	c.entries[0] = first
	c.n.Store(1)
	return &valueLog[V]{head: c, tail: c}
}

// writer-only
func (vl *valueLog[V]) append(v V) {
	t := vl.tail
	n := t.n.Load()
	if int(n) < valueChunkSize {
		t.entries[n] = v
		t.n.Store(n + 1) // publish
		return
	}
	c := &valueChunk[V]{}
	c.entries[0] = v
	c.n.Store(1)
	t.next.Store(c) // publish new chunk
	vl.tail = c
}

// snapshot returns a consistent prefix of the log. Lock-free.
func (vl *valueLog[V]) snapshot() []V {
	var out []V
	for c := vl.head; c != nil; c = c.next.Load() {
		n := int(c.n.Load())
		out = append(out, c.entries[:n]...)
	}
	return out
}

type skipListNode[V any] struct {
	key  []byte
	vlog *valueLog[V]
	next []atomic.Pointer[skipListNode[V]] // len == height of this node
}

type skipList[V any] struct {
	head   *skipListNode[V] // sentinel; next has skipListMaxHeight slots
	height int              // highest level in use; writer-only
	rng    uint64           // xorshift state; writer-only
}

func newSkipList[V any]() *skipList[V] {
	return &skipList[V]{
		head:   &skipListNode[V]{next: make([]atomic.Pointer[skipListNode[V]], skipListMaxHeight)},
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
	h := 1
	for h < skipListMaxHeight && x&1 == 1 {
		h++
		x >>= 1
	}
	return h
}

// writer-only
func (s *skipList[V]) insert(key []byte, v V) {
	var preds [skipListMaxHeight]*skipListNode[V]
	x := s.head
	for lvl := s.height - 1; lvl >= 0; lvl-- {
		for {
			nxt := x.next[lvl].Load()
			if nxt == nil || bytes.Compare(nxt.key, key) >= 0 {
				break
			}
			x = nxt
		}
		preds[lvl] = x
	}

	if nxt := x.next[0].Load(); nxt != nil && bytes.Equal(nxt.key, key) {
		nxt.vlog.append(v) // existing key: no topology change
		return
	}

	h := s.randomHeight()
	n := &skipListNode[V]{
		key:  key,
		vlog: newValueLog(v),
		next: make([]atomic.Pointer[skipListNode[V]], h),
	}
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
}

// get is lock-free. It descends from the max height (unused upper levels are
// nil) rather than the writer-only height field, which a reader must not touch.
func (s *skipList[V]) get(key []byte) ([]V, bool) {
	x := s.head
	for lvl := skipListMaxHeight - 1; lvl >= 0; lvl-- {
		for {
			nxt := x.next[lvl].Load()
			if nxt == nil || bytes.Compare(nxt.key, key) >= 0 {
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

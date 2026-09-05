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

	"github.com/weaviate/weaviate/entities/lsmkv"
)

// replaceIndex is the memtable's ordered key -> (value, secondaryKeys,
// tombstone) index for StrategyReplace, implemented by both the red-black tree
// (locked reads) and the replace skip list (lock-free reads).
type replaceIndex interface {
	// returns net additions of insert in bytes, previous secondary keys, and
	// whether the key was already live (present and not tombstoned)
	insert(key, value []byte, secondaryKeys [][]byte) (int, [][]byte, bool)
	// returns previous secondary keys and whether the key was already tombstoned
	setTombstone(key, value []byte, secondaryKeys [][]byte) ([][]byte, bool)
	get(key []byte) ([]byte, error)
	getNode(key []byte) (*binarySearchNode, error)
	exists(key []byte) error
	countStats() *countStats
	flattenInOrder() []*binarySearchNode
}

func newReplaceIndex(lockFree bool) replaceIndex {
	if lockFree {
		return newSkipListReplace()
	}
	return &binarySearchTree{}
}

// replaceEntry is one key's current state. It is immutable once stored in a
// node: a put or tombstone swaps the whole pointer, so a reader that loads it
// never sees a torn (value, secondaryKeys, tombstone) combination. Unlike the
// value-log skip list, replaced entries become garbage immediately — repeated
// updates of one key don't accumulate old versions until flush.
type replaceEntry struct {
	value         []byte
	secondaryKeys [][]byte
	tombstone     bool
}

type skipListReplaceNode struct {
	key  []byte
	val  atomic.Pointer[replaceEntry]
	next []atomic.Pointer[skipListReplaceNode] // len == height of this node
}

// skipListReplace is a single-writer skip list with lock-free reads for
// StrategyReplace. The topology rules are the same as skipList's: nodes are
// never rewired once linked, and publication order (payload before links,
// links bottom-up) guarantees a reader never observes a node without its entry.
type skipListReplace struct {
	head   *skipListReplaceNode // sentinel; next has skipListMaxHeight slots
	height int                  // highest level in use; writer-only
	rng    uint64               // xorshift state; writer-only
}

func newSkipListReplace() *skipListReplace {
	return &skipListReplace{
		head:   &skipListReplaceNode{next: make([]atomic.Pointer[skipListReplaceNode], skipListMaxHeight)},
		height: 1,
		rng:    0x9e3779b97f4a7c15,
	}
}

// writer-only
func (s *skipListReplace) randomHeight() int {
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

// upsert stores e as key's current entry and returns the entry it replaced
// (nil if the key is new). writer-only.
func (s *skipListReplace) upsert(key []byte, e *replaceEntry) *replaceEntry {
	var preds [skipListMaxHeight]*skipListReplaceNode
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
		prev := nxt.val.Load()
		nxt.val.Store(e)
		return prev
	}

	h := s.randomHeight()
	n := &skipListReplaceNode{
		key:  key,
		next: make([]atomic.Pointer[skipListReplaceNode], h),
	}
	n.val.Store(e)
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
	return nil
}

func (s *skipListReplace) insert(key, value []byte, secondaryKeys [][]byte) (int, [][]byte, bool) {
	prev := s.upsert(key, &replaceEntry{value: value, secondaryKeys: secondaryKeys})
	if prev == nil {
		return len(key) + len(value), nil, false
	}
	netAdditions := len(prev.value) - len(value)
	if netAdditions < 0 {
		netAdditions = -netAdditions
	}
	return netAdditions, prev.secondaryKeys, !prev.tombstone
}

func (s *skipListReplace) setTombstone(key, value []byte, secondaryKeys [][]byte) ([][]byte, bool) {
	prev := s.upsert(key, &replaceEntry{value: value, secondaryKeys: secondaryKeys, tombstone: true})
	if prev == nil {
		return nil, false
	}
	return prev.secondaryKeys, prev.tombstone
}

// findNode is lock-free. Like skipList.get, it descends from the max height
// rather than the writer-only height field.
func (s *skipListReplace) findNode(key []byte) *skipListReplaceNode {
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
		return nxt
	}
	return nil
}

func (s *skipListReplace) get(key []byte) ([]byte, error) {
	n := s.findNode(key)
	if n == nil {
		return nil, lsmkv.NotFound
	}
	e := n.val.Load()
	if e.tombstone {
		return nil, errorFromTombstonedValue(e.value)
	}
	return e.value, nil
}

// getNode returns a node snapshot built from the key's current entry, shaped
// like the red-black tree's node so cursor code can consume either index.
func (s *skipListReplace) getNode(key []byte) (*binarySearchNode, error) {
	n := s.findNode(key)
	if n == nil {
		return nil, lsmkv.NotFound
	}
	e := n.val.Load()
	if e.tombstone {
		return nil, errorFromTombstonedValue(e.value)
	}
	return &binarySearchNode{
		key:           n.key,
		value:         e.value,
		secondaryKeys: e.secondaryKeys,
	}, nil
}

func (s *skipListReplace) exists(key []byte) error {
	_, err := s.getNode(key)
	return err
}

func (s *skipListReplace) countStats() *countStats {
	stats := &countStats{}
	for x := s.head.next[0].Load(); x != nil; x = x.next[0].Load() {
		if x.val.Load().tombstone {
			stats.tombstonedKeys = append(stats.tombstonedKeys, x.key)
		} else {
			stats.upsertKeys = append(stats.upsertKeys, x.key)
		}
	}
	return stats
}

// flattenInOrder returns key-ascending node snapshots. On a live index it is a
// point-in-time view: keys inserted mid-walk may be skipped.
func (s *skipListReplace) flattenInOrder() []*binarySearchNode {
	var out []*binarySearchNode
	for x := s.head.next[0].Load(); x != nil; x = x.next[0].Load() {
		e := x.val.Load()
		var skeys [][]byte
		if ln := len(e.secondaryKeys); ln > 0 {
			skeys = make([][]byte, ln)
			copy(skeys, e.secondaryKeys)
		}
		out = append(out, &binarySearchNode{
			key:           x.key,
			value:         e.value,
			secondaryKeys: skeys,
			tombstone:     e.tombstone,
		})
	}
	return out
}

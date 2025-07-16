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

package common

import (
	"math/rand/v2"
	"sync/atomic"
)

type node[T any] struct {
	value T
	next  *node[T]
}

// Stack is a lock-free stack implementation using atomic operations.
// It allows concurrent Push and Pop operations without locks.
type Stack[T any] struct {
	head atomic.Pointer[node[T]]
}

func (s *Stack[T]) Push(value T) {
	newNode := &node[T]{value: value}
	for {
		oldHead := s.head.Load()
		newNode.next = oldHead
		if s.head.CompareAndSwap(oldHead, newNode) {
			return
		}
	}
}

func (s *Stack[T]) Pop() (value T, ok bool) {
	for {
		oldHead := s.head.Load()
		if oldHead == nil {
			var zero T
			return zero, false
		}
		newHead := oldHead.next
		if s.head.CompareAndSwap(oldHead, newHead) {
			return oldHead.value, true
		}
	}
}

func (s *Stack[T]) IsEmpty() bool {
	oldHead := s.head.Load()
	return oldHead == nil
}

// must be a power of two for the randomness to work correctly
const numShards = 8

// Pool is a sharded, lock-free freelist.
type Pool[T any] struct {
	shards [numShards]Stack[T]
}

func (p *Pool[T]) getShard() *Stack[T] {
	shard := rand.Uint64() & (numShards - 1)
	return &p.shards[shard]
}

// Put adds an object to a randomly selected shard.
func (p *Pool[T]) Put(value T) {
	p.getShard().Push(value)
}

// Get retrieves an object from a randomly selected shard.
// It tries multiple shards if needed.
func (p *Pool[T]) Get() (T, bool) {
	// Start from a random shard
	start := p.getShard()
	if v, ok := start.Pop(); ok {
		return v, true
	}

	// Try other shards sequentially
	for i := range p.shards {
		if &p.shards[i] == start {
			continue
		}
		if v, ok := p.shards[i].Pop(); ok {
			return v, true
		}
	}
	var zero T
	return zero, false
}

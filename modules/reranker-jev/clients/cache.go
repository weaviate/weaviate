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

package clients

import (
	"container/list"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"sync"
)

// judgmentKey is a hash, so that an entry has the same size whatever the
// length of the document and the query.
type judgmentKey [sha256.Size]byte

// newJudgmentKey hashes the parts with their lengths, so that two different
// lists of parts cannot produce the same input.
func newJudgmentKey(parts ...string) judgmentKey {
	hash := sha256.New()
	var length [8]byte
	for _, part := range parts {
		binary.LittleEndian.PutUint64(length[:], uint64(len(part)))
		hash.Write(length[:])
		io.WriteString(hash, part)
	}
	var key judgmentKey
	hash.Sum(key[:0])
	return key
}

type judgment struct {
	key         judgmentKey
	probability float64
}

// judgmentCache is a least-recently-used cache of Jev answers. Jev can answer
// the same question about the same document with a different probability, so
// without the cache a result could pass the threshold in one request and fail
// it in the next. Entries never expire: a changed document has another key.
// The key also covers the endpoint, the API key and the batch size, see
// judgeRequest.cacheKey.
type judgmentCache struct {
	lock     sync.Mutex
	capacity int
	order    *list.List
	entries  map[judgmentKey]*list.Element
}

func newJudgmentCache(capacity int) *judgmentCache {
	return &judgmentCache{
		capacity: capacity,
		order:    list.New(),
		entries:  make(map[judgmentKey]*list.Element),
	}
}

func (c *judgmentCache) get(key judgmentKey) (float64, bool) {
	c.lock.Lock()
	defer c.lock.Unlock()

	element, ok := c.entries[key]
	if !ok {
		return 0, false
	}
	c.order.MoveToFront(element)
	return element.Value.(*judgment).probability, true
}

// putIfAbsent stores the probability unless the key already has one, and
// returns the stored one. Queries that judged the same document at the same
// time then all use the first answer.
func (c *judgmentCache) putIfAbsent(key judgmentKey, probability float64) float64 {
	c.lock.Lock()
	defer c.lock.Unlock()

	if element, ok := c.entries[key]; ok {
		c.order.MoveToFront(element)
		return element.Value.(*judgment).probability
	}
	c.insert(key, probability)
	return probability
}

// insert adds a key that is not in the cache. The caller holds the lock.
func (c *judgmentCache) insert(key judgmentKey, probability float64) {
	if c.order.Len() >= c.capacity {
		oldest := c.order.Back()
		c.order.Remove(oldest)
		delete(c.entries, oldest.Value.(*judgment).key)
	}
	c.entries[key] = c.order.PushFront(&judgment{key: key, probability: probability})
}

// put stores the probability and replaces the one the key had.
func (c *judgmentCache) put(key judgmentKey, probability float64) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if element, ok := c.entries[key]; ok {
		element.Value.(*judgment).probability = probability
		c.order.MoveToFront(element)
		return
	}
	c.insert(key, probability)
}

// remove drops the key. A missing key is not an error.
func (c *judgmentCache) remove(key judgmentKey) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if element, ok := c.entries[key]; ok {
		c.order.Remove(element)
		delete(c.entries, key)
	}
}

func (c *judgmentCache) len() int {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.order.Len()
}

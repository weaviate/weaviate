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

package queue

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/pkg/errors"
)

// Retention keeps every chunk from its mark on, processed or not, until it is
// released: a replacement index replays them while the current consumer
// carries on. Processed chunks are kept as a backup keeps them, behind a
// .processed tombstone, so a restart deletes them: a replacement is never
// resumed after one.
type Retention struct {
	q *DiskQueue

	m sync.Mutex
	// mark is the first chunk to replay, empty until the queue holds one
	mark string
	// gap is the first loss of a record the replay needs
	gap error
}

// Retain starts keeping the queue's chunks, from the oldest one not processed
// yet. A queue holds one retention at a time.
func (q *DiskQueue) Retain() (*Retention, error) {
	q.rmLock.Lock()
	defer q.rmLock.Unlock()

	if q.r == nil {
		return nil, errors.New("queue not initialized")
	}
	if q.retention.Load() != nil {
		return nil, errors.New("queue already retained")
	}

	r := Retention{q: q}
	// under the writer's lock, so a chunk created next sees the retention
	q.m.Lock()
	defer q.m.Unlock()
	r.mark = q.r.firstUnprocessed()
	if r.mark == "" && q.w.f != nil {
		r.mark = q.w.f.Name()
	}
	q.retention.Store(&r)
	return &r, nil
}

// Mark is the path of the first chunk to replay. Empty: nothing was queued
// since Retain.
func (r *Retention) Mark() string {
	r.m.Lock()
	defer r.m.Unlock()
	return r.mark
}

// Gap is the first loss of a record from the mark on, or nil. A replay with
// a gap misses that record.
func (r *Retention) Gap() error {
	r.m.Lock()
	defer r.m.Unlock()
	return r.gap
}

// Release stops keeping chunks and deletes the processed ones, unless a
// backup still keeps them. Releasing twice is a no-op.
func (r *Retention) Release() error {
	q := r.q
	q.rmLock.Lock()
	defer q.rmLock.Unlock()

	if q.retention.Load() != r {
		return nil
	}
	q.retention.Store(nil)
	if q.maintenanceMode.Load() {
		return nil
	}
	return q.cleanupProcessedChunks()
}

// chunkCreated gives a retention without a mark its first chunk.
func (q *DiskQueue) chunkCreated(path string) {
	r := q.retention.Load()
	if r == nil {
		return
	}
	r.m.Lock()
	defer r.m.Unlock()
	if r.mark == "" {
		r.mark = path
	}
}

// recordLoss tells a retention that records were lost: those of the chunk
// at path, or with no path a record that never reached a chunk. A chunk
// before the mark is no loss to the replay.
func (q *DiskQueue) recordLoss(path string, cause error) {
	r := q.retention.Load()
	if r == nil {
		return
	}
	r.m.Lock()
	defer r.m.Unlock()
	if r.gap != nil {
		return
	}
	if path == "" {
		r.gap = fmt.Errorf("a record was not queued: %w", cause)
		return
	}
	if r.mark == "" || !chunkAtOrAfter(path, r.mark) {
		return
	}
	r.gap = fmt.Errorf("records of chunk %s were lost: %w", filepath.Base(path), cause)
}

// chunkAtOrAfter reports whether the chunk at path comes at or after the
// one at mark in replay order. A name that does not parse counts as after.
func chunkAtOrAfter(path, mark string) bool {
	seq, ok := chunkSeq(path)
	markSeq, markOK := chunkSeq(mark)
	if !ok || !markOK {
		return true
	}
	return seq >= markSeq
}

// chunkSeq is the number a chunk file is named after.
func chunkSeq(path string) (int64, bool) {
	name := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(path), "chunk-"), ".bin")
	seq, err := strconv.ParseInt(name, 10, 64)
	return seq, err == nil
}

// firstUnprocessed is the path of the oldest chunk whose records are not
// processed yet, or empty.
func (r *chunkReader) firstUnprocessed() string {
	r.m.Lock()
	defer r.m.Unlock()
	for _, ref := range r.chunkList {
		if !ref.processed {
			return ref.path
		}
	}
	return ""
}

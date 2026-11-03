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
	// mark is the first chunk to replay
	mark string
}

// Retain starts keeping the queue's chunks, from the oldest one not processed
// yet. On a queue with none, the mark is the name a chunk created now would
// get: chunks replay in name order, and names follow the clock. A queue
// holds one retention at a time.
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
	// under the writer's lock, so no chunk is created in between
	q.m.Lock()
	defer q.m.Unlock()
	r.mark = q.r.firstUnprocessed()
	if r.mark == "" && q.w.f != nil {
		r.mark = q.w.f.Name()
	}
	if r.mark == "" {
		r.mark = filepath.Join(q.dir, fmt.Sprintf(chunkFileFmt, chunkTimeNow()))
	}
	q.retention.Store(&r)
	return &r, nil
}

// Mark is the path of the first chunk to replay: every chunk named at or
// after it. The file may not exist yet.
func (r *Retention) Mark() string {
	r.m.Lock()
	defer r.m.Unlock()
	return r.mark
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

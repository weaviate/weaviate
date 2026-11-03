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

	"github.com/pkg/errors"
)

// Retain keeps every chunk from the returned mark on, processed or not,
// until ReleaseRetention: a replacement index replays them while the current
// consumer carries on. Processed chunks are kept as a backup keeps them,
// behind a .processed tombstone, so a restart deletes them: a replacement is
// never resumed after one.
//
// The mark is the path of the first chunk to replay, and every chunk named
// at or after it replays: the oldest chunk not processed yet, or on a queue
// with none the name a chunk created now would get, since names follow the
// clock. The file may not exist yet. A queue is retained once at a time.
func (q *DiskQueue) Retain() (mark string, err error) {
	q.rmLock.Lock()
	defer q.rmLock.Unlock()

	if q.r == nil {
		return "", errors.New("queue not initialized")
	}
	if q.retained.Load() {
		return "", errors.New("queue already retained")
	}

	// under the writer's lock, so no chunk is created in between
	q.m.Lock()
	defer q.m.Unlock()
	mark = q.r.firstUnprocessed()
	if mark == "" && q.w.f != nil {
		mark = q.w.f.Name()
	}
	if mark == "" {
		mark = filepath.Join(q.dir, fmt.Sprintf(chunkFileFmt, chunkTimeNow()))
	}
	q.retained.Store(true)
	return mark, nil
}

// ReleaseRetention stops keeping chunks and deletes the processed ones,
// unless a backup still keeps them. Releasing a queue not retained is a
// no-op.
func (q *DiskQueue) ReleaseRetention() error {
	q.rmLock.Lock()
	defer q.rmLock.Unlock()

	if !q.retained.Swap(false) || q.maintenanceMode.Load() {
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

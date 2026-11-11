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

package geopoints

import (
	"context"
	"sync"

	"github.com/weaviate/sroar"
)

// Memtable is the in-memory layer of a bucket: points in arrival order and the
// doc IDs removed since the last flush. A query scans it. The lock guards reads
// against writes; the lsmkv memtable serializes the writers.
type Memtable struct {
	lock         sync.RWMutex
	points       []Point
	deleted      *sroar.Bitmap
	deletedCount int // GetCardinality would recompute it on every write
}

func NewMemtable() *Memtable {
	return &Memtable{deleted: sroar.NewBitmap()}
}

// Add records a point. An earlier removal of the same doc ID stands: doc IDs
// are never reused, so such an add is a stale write for a point that is gone.
func (m *Memtable) Add(docID uint64, lat, lon float32) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.points = append(m.points, Point{DocID: docID, Lat: lat, Lon: lon})
}

func (m *Memtable) Remove(docID uint64) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.deleted.Set(docID) {
		m.deletedCount++
	}
}

// Size drives the flush decision and must be positive once anything was
// written, or the write-ahead log is discarded as empty.
func (m *Memtable) Size() int {
	m.lock.RLock()
	defer m.lock.RUnlock()

	return len(m.points)*16 + m.deletedCount*8
}

func (m *Memtable) WithinRange(ctx context.Context, c *Circle, out, deleted *sroar.Bitmap) error {
	m.lock.RLock()
	defer m.lock.RUnlock()

	deleted.Or(m.deleted)
	var matches []uint64 // in arrival order, which is nearly doc ID order
	for i := range m.points {
		if i%4096 == 4095 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		p := &m.points[i]
		if c.Contains(p.Lat, p.Lon) {
			matches = append(matches, p.DocID)
		}
	}
	addSorted(out, matches)
	return nil
}

// Body is the segment content for a flush. A point this memtable also removes
// is dropped along with the removal, which can refer to nothing else since a
// doc ID is added once.
func (m *Memtable) Body() *Body {
	m.lock.RLock()
	defer m.lock.RUnlock()

	points := make([]Point, 0, len(m.points))
	carried := m.deleted.Clone()
	for _, p := range m.points {
		if m.deleted.Contains(p.DocID) {
			carried.Remove(p.DocID)
			continue
		}
		points = append(points, p)
	}

	body := &Body{Points: points}
	if !carried.IsEmpty() {
		body.Deletions = carried.ToBuffer()
	}
	return body
}

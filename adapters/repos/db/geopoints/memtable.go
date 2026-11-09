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

// Memtable is the in-memory layer of a bucket: points in arrival order and
// the doc IDs removed since the last flush. A query scans it, which is cheap
// because the flush threshold bounds it. The lock guards reads against
// concurrent writes; the lsmkv memtable serializes writers itself.
type Memtable struct {
	lock    sync.RWMutex
	points  []Point
	deleted *sroar.Bitmap
	// deletedCount tracks removals for Size, which GetCardinality would
	// recompute on every write
	deletedCount int
}

func NewMemtable() *Memtable {
	return &Memtable{deleted: sroar.NewBitmap()}
}

// Add records a point. A doc ID removed earlier in this memtable comes back,
// which the write path never asks for but a replayed log must not break on.
func (m *Memtable) Add(docID uint64, lat, lon float32) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.points = append(m.points, Point{DocID: docID, Lat: lat, Lon: lon})
	if m.deleted.Contains(docID) {
		m.deleted.Remove(docID)
		m.deletedCount--
	}
}

// Remove records that docID's point, wherever it is, is gone.
func (m *Memtable) Remove(docID uint64) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.deleted.Set(docID) {
		m.deletedCount++
	}
}

// Size estimates the memory held, in bytes. It drives the flush decision and
// must be positive once anything was written, or the write-ahead log is
// discarded as empty.
func (m *Memtable) Size() int {
	m.lock.RLock()
	defer m.lock.RUnlock()

	return len(m.points)*16 + m.deletedCount*8
}

// WithinRange implements InnerReader.
func (m *Memtable) WithinRange(ctx context.Context, c *Circle, out, deleted *sroar.Bitmap) error {
	m.lock.RLock()
	defer m.lock.RUnlock()

	deleted.Or(m.deleted)
	for i := range m.points {
		if i%4096 == 4095 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		p := &m.points[i]
		if c.Contains(p.Lat, p.Lon) {
			out.Set(p.DocID)
		}
	}
	return nil
}

// Serialize builds the segment body for a flush. A point removed again in this
// same memtable is dropped along with its deletion: a doc ID is added once, so
// that deletion can refer to nothing else.
func (m *Memtable) Serialize() []byte {
	m.lock.RLock()
	defer m.lock.RUnlock()

	points := make([]Point, 0, len(m.points))
	deleted := m.deleted.Clone()
	for _, p := range m.points {
		if deleted.Contains(p.DocID) {
			deleted.Remove(p.DocID)
			continue
		}
		points = append(points, p)
	}

	var deletions []byte
	if !deleted.IsEmpty() {
		deletions = deleted.ToBuffer()
	}
	return Build(points, deletions)
}

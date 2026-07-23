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

package roaringset

import (
	"github.com/sirupsen/logrus"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/errors"
	entsync "github.com/weaviate/weaviate/entities/sync"
)

// SegmentInMemory holds the fully merged state (additions minus deletions) of
// all disk segments of a roaring set bucket, kept up to date by merging every
// flushed memtable forward. It serves reads without touching disk segments.
type SegmentInMemory struct {
	logger logrus.FieldLogger

	bitmaps     map[string]*sroar.Bitmap
	bitmapsLock *entsync.ReadPreferringRWMutex
	pending     *PendingMerges[*BinarySearchTree] // flushed memtables, waiting to be merged into bitmaps
}

func NewSegmentInMemory(logger logrus.FieldLogger) *SegmentInMemory {
	return &SegmentInMemory{
		logger:      logger,
		bitmaps:     map[string]*sroar.Bitmap{},
		bitmapsLock: entsync.NewReadPreferringRWMutex(),
		pending:     NewPendingMerges[*BinarySearchTree](),
	}
}

// MergeSegmentByCursor folds one disk segment into the merged state. Keys
// whose bitmap becomes empty are dropped to bound heap usage.
func (s *SegmentInMemory) MergeSegmentByCursor(cursor SegmentCursor) error {
	s.bitmapsLock.Lock()
	defer s.bitmapsLock.Unlock()

	for key, layer, err := cursor.First(); ; key, layer, err = cursor.Next() {
		if err != nil {
			return err
		}
		if key == nil {
			return nil
		}

		bm, ok := s.bitmaps[string(key)]
		if !ok {
			bm = sroar.NewBitmap()
			s.bitmaps[string(key)] = bm
		}
		bm.AndNotConc(layer.Deletions, concurrency.SROAR_MERGE)
		bm.OrConc(layer.Additions, concurrency.SROAR_MERGE)
		if bm.IsEmpty() {
			delete(s.bitmaps, string(key))
		}
	}
}

func (s *SegmentInMemory) MergeMemtableEventually(memtable *BinarySearchTree) {
	// run background merge only once,
	// handle also all memtables added while merge is performed
	if s.pending.Add(memtable) {
		errors.GoWrapper(s.mergeMemtables, s.logger)
	}
}

func (s *SegmentInMemory) mergeMemtables() {
	s.bitmapsLock.Lock()
	defer s.bitmapsLock.Unlock()

	s.pending.Drain(func(memtable *BinarySearchTree) {
		for _, node := range memtable.FlattenInOrder() {
			bm, ok := s.bitmaps[string(node.Key)]
			if !ok {
				bm = sroar.NewBitmap()
				s.bitmaps[string(node.Key)] = bm
			}
			bm.AndNotConc(node.Value.Deletions, concurrency.SROAR_MERGE)
			bm.OrConc(node.Value.Additions, concurrency.SROAR_MERGE)
			if bm.IsEmpty() {
				delete(s.bitmaps, string(node.Key))
			}
		}
	})
}

// Get returns the merged bitmap for key as a pooled clone plus the snapshot of
// memtables still pending merge. Pending trees are immutable once extracted
// from their memtable, so they can be read after the locks are released.
// Lock order is always bitmapsLock -> pending queue lock, never reversed.
func (s *SegmentInMemory) Get(key []byte, bufPool BitmapBufPool,
) (root BitmapLayer, release func(), found bool, pending []*BinarySearchTree) {
	s.bitmapsLock.RLock()
	defer s.bitmapsLock.RUnlock()

	release = noopRelease
	if bm, ok := s.bitmaps[string(key)]; ok {
		// the clone is required: BitmapLayers.Flatten mutates the first layer
		// in place, the shared bitmap must never escape
		root.Additions, release = bufPool.CloneToBuf(bm)
		found = true
	}

	pending = s.pending.Snapshot()

	return root, release, found, pending
}

func (s *SegmentInMemory) Size() int {
	s.bitmapsLock.RLock()
	defer s.bitmapsLock.RUnlock()

	size := 0
	for _, bm := range s.bitmaps {
		size += bm.LenInBytes()
	}
	return size
}

var noopRelease = func() { /* nothing to release when the key was not found */ }

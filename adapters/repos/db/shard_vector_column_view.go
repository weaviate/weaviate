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

package db

import (
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
)

// vectorColumnRescoreView is the composite consistent view handed to a
// vector index for one rescore pass when columnarRescore serves the target
// vector. It pins two buckets at once:
//
//   - column: the per-target vector-column bucket's consistent view. Pinning
//     happens via segment refcounts (lsmkv.GetConsistentView increments every
//     disk segment's refcount; compaction parks replaced segments until their
//     refcount drops to zero before munmapping/deleting them). This is what
//     makes the zero-copy read path safe: a []float32 returned by
//     ColumnarGetVectorFloatsWithView may alias a segment's mmap, and that
//     mapping cannot disappear while this view is held. Memtable-resident
//     rows are NOT covered by this guarantee — memtable rows are heap memory
//     mutated in place under the memtable's locks (re-puts, tombstone flips),
//     so the lsmkv layer copies those instead of aliasing (see
//     ColumnarGetVectorFloatsWithView for the full analysis).
//
//   - objects: the objects bucket's consistent view, used by the per-docID
//     fallback path (rows the column cannot serve: written while the flag was
//     disabled and not yet backfilled, or column misses after a delete race).
//
// The view is created once per rescore pass (GetViewThunk) and released after
// the last candidate read; aliased slices must not be read after ReleaseView,
// since released segments may be munmapped by a pending compaction drop.
type vectorColumnRescoreView struct {
	column  lsmkv.BucketConsistentView
	objects common.BucketView
}

func (v *vectorColumnRescoreView) ReleaseView() {
	v.column.ReleaseView()
	v.objects.ReleaseView()
}

// getVectorColumnRescoreView returns the view a rescore pass should run on:
// the composite (column + objects) view when the target's vector column can
// serve reads, otherwise the plain objects-bucket view. The decision is made
// once per pass; the read thunks type-assert the view to pick the matching
// read path, so a column that becomes servable (or stops being servable)
// mid-pass is simply picked up by the next pass.
func (s *Shard) getVectorColumnRescoreView(targetVector string) common.BucketView {
	bucket := s.servableVectorColumnBucket(targetVector)
	if bucket == nil {
		return s.GetObjectsBucketView()
	}
	return &vectorColumnRescoreView{
		column:  bucket.GetConsistentView(),
		objects: s.GetObjectsBucketView(),
	}
}

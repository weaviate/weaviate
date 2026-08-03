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
	"sort"

	"github.com/weaviate/sroar"
)

// ContainsAnyResolver is a resident accelerator for batched ContainsAny
// resolution on a roaringset bucket: it maps the bucket's keys to docIDs in a
// columnar layout and resolves a batch of sorted query keys without per-key
// segment lookups. It is implemented by the inverted/columnar package and
// attached to a bucket by the query layer — lsmkv holds it behind this
// interface because it cannot import columnar (which imports lsmkv).
type ContainsAnyResolver interface {
	// ResolveContainsAny returns the docIDs whose key is in sortedKeys (encoded,
	// ascending). The returned bitmap is owned by the caller.
	ResolveContainsAny(sortedKeys [][]byte) *sroar.Bitmap
}

// containsAccelerator pairs a resolver with the identity of the disk segment set
// it was built from, so staleness can be detected after a flush or compaction.
// A nil resolver is a cached "declined" outcome (e.g. the property is not unique)
// so build is not retried until the segment set changes.
type containsAccelerator struct {
	resolver ContainsAnyResolver
	segPaths []string // sorted paths of the disk segments the resolver was built from
}

// HasUnflushedData reports whether the view carries writes not yet on disk
// (non-empty active or flushing memtable). A base-only accelerator built from
// disk segments is only correct to serve when this is false.
func (cv BucketConsistentView) HasUnflushedData() bool {
	if cv.Flushing != nil && cv.Flushing.Size() > 0 {
		return true
	}
	return cv.Active != nil && cv.Active.Size() > 0
}

// segmentSetIdentity returns the sorted disk-segment paths — a stable identity
// that changes on any flush (new segment file) or compaction (merged file).
func segmentSetIdentity(disk []Segment) []string {
	paths := make([]string, len(disk))
	for i, s := range disk {
		paths[i] = s.getPath()
	}
	sort.Strings(paths)
	return paths
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// GetOrBuildContainsAnyAccelerator returns a resolver that can serve the given
// view, or nil if the bucket cannot be accelerated for it. It returns nil
// immediately when the view has unflushed data (the base-only accelerator cannot
// layer memtables yet). Otherwise it returns the cached resolver if it was built
// from exactly this disk segment set, or calls build() once and caches the
// outcome — including a nil "declined" result (e.g. a non-unique property), so
// build is not retried until the segments change via flush or compaction.
//
// Consistency note: build() is expected to snapshot the bucket itself; under a
// concurrent flush its data and this view's staleness tag could diverge by one
// generation. That race is benign for the current single-writer tests/benchmarks
// and closes when memtable layering removes the fully-flushed requirement.
func (b *Bucket) GetOrBuildContainsAnyAccelerator(
	view BucketConsistentView, build func() ContainsAnyResolver,
) ContainsAnyResolver {
	if view.HasUnflushedData() {
		return nil
	}
	segs := segmentSetIdentity(view.Disk)

	b.containsAccMu.RLock()
	acc := b.containsAcc
	b.containsAccMu.RUnlock()
	if acc != nil && equalStrings(acc.segPaths, segs) {
		return acc.resolver // may be nil (previously declined for this segment set)
	}

	resolver := build()
	b.containsAccMu.Lock()
	b.containsAcc = &containsAccelerator{resolver: resolver, segPaths: segs}
	b.containsAccMu.Unlock()
	return resolver
}

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
	"errors"

	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	entlsmkv "github.com/weaviate/weaviate/entities/lsmkv"
)

// ContainsAnyResolver is a resident accelerator for batched ContainsAny
// resolution on a roaringset bucket: a columnar key→docID index that resolves a
// batch of sorted query keys without per-key segment lookups. Implemented by the
// inverted/columnar package and attached to a bucket at open via a factory —
// lsmkv holds it behind this interface because it cannot import columnar (which
// imports lsmkv).
//
// It covers only flushed data: the disk segments present at build (the base),
// plus every memtable flushed since, fed in via AbsorbFlush. Live active/flushing
// memtables are layered separately by LayerMemtablesOverBase at read time.
type ContainsAnyResolver interface {
	// AbsorbFlush copies a just-flushed memtable (iterated via its roaringset
	// cursor) into the index so its data stays served without re-reading disk.
	// It returns once the flushed data is queryable; any consolidation it
	// schedules off the back of it runs in the background.
	AbsorbFlush(cursor roaringset.InnerCursor) error
}

// RoaringSetLayerReader reads one unflushed tier's view of a key. Handed out by
// [Bucket.MemtableReaders] so a query layer can consult tiers it cannot reach
// itself — memtables and their accessors are unexported.
type RoaringSetLayerReader interface {
	// Get returns the tier's additions and deletions for key. A key the tier
	// never touched reports an empty layer, not an error.
	Get(key []byte) (roaringset.BitmapLayer, error)
}

type memtableLayerReader struct{ mt memtable }

func (r memtableLayerReader) Get(key []byte) (roaringset.BitmapLayer, error) {
	layer, err := r.mt.roaringSetGet(key)
	if err != nil && errors.Is(err, entlsmkv.NotFound) {
		return roaringset.BitmapLayer{}, nil
	}
	return layer, err
}

// MemtableReaders returns readers over the tiers this bucket has not flushed
// yet, oldest first — flushing before active, so a caller replaying them in
// order sees a document added while flushing and deleted in the active memtable
// as deleted. Empty tiers are left out.
//
// Exists because resolving a query against an accelerator that covers only
// flushed data has to consult these too, and the tiers themselves are not
// reachable from outside this package.
func (b *Bucket) MemtableReaders(view BucketConsistentView) []RoaringSetLayerReader {
	var readers []RoaringSetLayerReader
	for _, mt := range []memtable{view.Flushing, view.Active} {
		if mt == nil || mt.Size() == 0 {
			continue
		}
		readers = append(readers, memtableLayerReader{mt: mt})
	}
	return readers
}

// ContainsAcceleratorFactory builds an accelerator for a freshly opened bucket
// (disk segments loaded, memtables empty — so the base is disk-only for free).
// Returns nil to decline (e.g. the property is not unique). Supplied by the
// query layer, which can import columnar.
type ContainsAcceleratorFactory func(b *Bucket) ContainsAnyResolver

type containsAccelerator struct {
	resolver ContainsAnyResolver
}

// initContainsAccelerator builds and attaches the accelerator at bucket open, if
// a factory was configured and the strategy is roaringset. Called once during
// bucket construction, before any flush, so the flush hook always has a target.
func (b *Bucket) initContainsAccelerator() {
	if b.strategy != StrategyRoaringSet || b.containsAccFactory == nil {
		return
	}
	if resolver := b.containsAccFactory(b); resolver != nil {
		b.containsAcc.Store(&containsAccelerator{resolver: resolver})
	}
}

// ContainsAnyAccelerator returns the bucket's attached accelerator, or nil if
// none (no factory, the factory declined, or a flush detached it). Lock-free.
func (b *Bucket) ContainsAnyAccelerator() ContainsAnyResolver {
	acc := b.containsAcc.Load()
	if acc == nil {
		return nil
	}
	return acc.resolver
}

// DetachContainsAccelerator drops the bucket's accelerator, sending ContainsAny
// back to the standard fold from here on. Callers use this when something the
// accelerator assumes about the bucket's contents stops holding — its
// key→document mapping is built for one key per document, so a change that lets
// a document span several keys invalidates it.
func (b *Bucket) DetachContainsAccelerator() {
	b.containsAcc.Store(nil)
}

// absorbFlushIntoAccelerator feeds a just-flushed memtable to the accelerator.
// Called from the flush path once the memtable is durable but before the segment
// swap, so the copy stays off flushLock. The memtable is immutable by then
// (writers have drained and it is no longer active), and it is still visible to
// readers as `flushing`, so publishing its run early only ever double-applies —
// never hides — its writes. If the accelerator declines (e.g. the flush revealed
// a non-unique key), it is detached: the index may now be missing docIDs, so
// ContainsAny falls back to the fold from here on.
func (b *Bucket) absorbFlushIntoAccelerator(flushing memtable) error {
	acc := b.containsAcc.Load()
	if acc == nil || acc.resolver == nil {
		return nil
	}
	if flushing == nil || flushing.Size() == 0 {
		return nil // nothing was flushed; the swap discards this memtable
	}
	if err := acc.resolver.AbsorbFlush(flushing.newRoaringSetCursor()); err != nil {
		b.containsAcc.Store(nil)
		return err
	}
	return nil
}

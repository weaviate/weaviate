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

	"github.com/weaviate/sroar"
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
	// ResolveContainsAny returns the docIDs whose key is in sortedKeys (encoded,
	// ascending), over the base plus all absorbed flushes. Caller owns the result.
	ResolveContainsAny(sortedKeys [][]byte) *sroar.Bitmap
	// AbsorbFlush copies a just-flushed memtable (iterated via its roaringset
	// cursor) into the index so its data stays served without re-reading disk.
	AbsorbFlush(cursor roaringset.InnerCursor) error
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

// absorbFlushIntoAccelerator feeds a just-flushed memtable to the accelerator.
// Called from the flush path with the flushing memtable still in hand. If the
// accelerator declines (e.g. the flush revealed a non-unique key), it is
// detached — the index may now be missing docIDs, so ContainsAny falls back to
// the fold from here on.
func (b *Bucket) absorbFlushIntoAccelerator(flushing memtable) error {
	acc := b.containsAcc.Load()
	if acc == nil || acc.resolver == nil {
		return nil
	}
	if err := acc.resolver.AbsorbFlush(flushing.newRoaringSetCursor()); err != nil {
		b.containsAcc.Store(nil)
		return err
	}
	return nil
}

// LayerMemtablesOverBase applies the active + flushing memtables (oldest→newest)
// over a base ContainsAny result for the given query keys, returning the net
// result. The base covers flushed data only; this adds the unflushed writes.
//
// Under 1-doc-per-key, per tier: base = (base AndNot tier.dels) Or tier.adds. The
// order (flushing then active) matters — a docID added in flushing and deleted in
// active must not survive. base is mutated in place and returned.
func (b *Bucket) LayerMemtablesOverBase(view BucketConsistentView, keys [][]byte,
	base *sroar.Bitmap,
) (*sroar.Bitmap, error) {
	for _, mt := range []memtable{view.Flushing, view.Active} {
		if mt == nil {
			continue
		}
		adds := sroar.NewBitmap()
		dels := sroar.NewBitmap()
		for _, key := range keys {
			layer, err := mt.roaringSetGet(key)
			if err != nil {
				if errors.Is(err, entlsmkv.NotFound) {
					continue
				}
				return nil, err
			}
			if layer.Additions != nil {
				adds.Or(layer.Additions)
			}
			if layer.Deletions != nil {
				dels.Or(layer.Deletions)
			}
		}
		base.AndNot(dels)
		base.Or(adds)
	}
	return base, nil
}

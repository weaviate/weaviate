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
	// ResolvePerKey resolves each key of sortedKeys (encoded, ascending)
	// independently over the base plus all absorbed flushes, returning a docID
	// and a liveness flag per query position. Per key rather than as one set so
	// the caller can layer unflushed memtables on the same terms — a deletion
	// belongs to the key it was issued under, not to the result as a whole.
	ResolvePerKey(sortedKeys [][]byte) (docs []uint64, live []bool)
	// AbsorbFlush copies a just-flushed memtable (iterated via its roaringset
	// cursor) into the index so its data stays served without re-reading disk.
	// It returns once the flushed data is queryable; any consolidation it
	// schedules off the back of it runs in the background.
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

// errAcceleratorNotApplicable reports that the accelerator cannot represent what
// the memtables hold, and the caller should fall back to the standard fold.
var errAcceleratorNotApplicable = errors.New("columnar accelerator cannot represent this key's docIDs")

// LayerMemtablesOverBase applies the flushing then active memtables over a
// per-key resolution of the flushed tiers, returning the net docIDs.
//
// Each key is carried independently: for that key, a tier first retires the
// docID the key currently holds if the tier deleted it, then takes the tier's
// addition. Deletions therefore reach only the key they were issued under, which
// is what stops a document that also sits under another key from vanishing from
// both. The order (flushing before active) matters — a docID added while
// flushing and deleted in the active memtable must not survive.
//
// docs and live come from ContainsAnyResolver.ResolvePerKey and are mutated in
// place. A key whose memtable additions hold more than one docID cannot be
// represented by the accelerator's scalar column, so the whole query declines
// and falls back to the fold.
func (b *Bucket) LayerMemtablesOverBase(view BucketConsistentView, keys [][]byte,
	docs []uint64, live []bool,
) error {
	for _, mt := range []memtable{view.Flushing, view.Active} {
		if mt == nil || mt.Size() == 0 {
			continue // no unflushed writes in this tier — nothing to layer
		}
		for i, key := range keys {
			layer, err := mt.roaringSetGet(key)
			if err != nil {
				if errors.Is(err, entlsmkv.NotFound) {
					continue
				}
				return err
			}
			if live[i] && layer.Deletions.Contains(docs[i]) {
				live[i] = false
			}
			if layer.Additions.IsEmpty() {
				continue
			}
			id := layer.Additions.Minimum()
			if layer.Additions.Maximum() != id {
				return errAcceleratorNotApplicable
			}
			// An addition adds a docID to the key's set; it does not replace what
			// the key already holds. A different docID still living under this key
			// means the key now has two, which the scalar column cannot represent —
			// and the memtable alone cannot show that, since the other docID may
			// have come from the flushed tiers.
			if live[i] && docs[i] != id {
				return errAcceleratorNotApplicable
			}
			docs[i], live[i] = id, true
		}
	}
	return nil
}

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
	"fmt"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/storobj"
)

type slot int32

const noSlot slot = -1

type propSlots struct {
	filterable, searchable, rangeable, length, null, nestedMeta slot
}

var noSlots = propSlots{noSlot, noSlot, noSlot, noSlot, noSlot, noSlot}

// invertedWrite pins every bucket an object write touches before its first
// change, so a missing bucket refuses the write with nothing changed and no
// teardown frees a bucket mid-write. The caller owes release on every exit:
// a leaked pin hangs every later shutdown of that bucket.
type invertedWrite struct {
	s  *Shard
	st *propValueIndexState

	next, prev         *storobj.Object
	addDocID, delDocID uint64
	status             objectInsertStatus

	nextProps, prevProps []inverted.Property
	toAdd, toDel         []inverted.Property
	nilAdd, nilDel       []inverted.NilProperty
	nestedAdd, nestedDel []inverted.NestedProperty
	mirrorAdd, mirrorDel [][]inverted.Property

	// slots runs parallel to toDel, toAdd, nilDel, nilAdd, nestedDel, nestedAdd, in that order.
	slots   []propSlots
	dims    slot
	buckets []*lsmkv.Bucket
}

// prepareInvertedWrite takes a nil next for a delete and a nil prev for a create.
func (s *Shard) prepareInvertedWrite(next, prev *storobj.Object, status objectInsertStatus) (*invertedWrite, error) {
	// Loaded before analysis: the swap publishes the overlay before it disarms,
	// so a disarmed snapshot means the analysis already sees the overlay.
	w := &invertedWrite{s: s, st: s.loadPropValueIndexState(), next: next, prev: prev, status: status, dims: noSlot}
	if err := w.analyze(); err != nil {
		return nil, err
	}
	if err := w.resolve(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *invertedWrite) analyze() error {
	var nextNil, prevNil []inverted.NilProperty
	var nextNested, prevNested []inverted.NestedProperty
	var err error
	if w.next != nil {
		if w.nextProps, nextNil, nextNested, err = w.s.AnalyzeObject(w.next); err != nil {
			return fmt.Errorf("analyze next object: %w", err)
		}
	}
	if w.prev != nil {
		if w.prevProps, prevNil, prevNested, err = w.s.AnalyzeObject(w.prev); err != nil {
			return fmt.Errorf("analyze previous object: %w", err)
		}
	}

	switch {
	case w.next == nil:
		w.delDocID = w.status.oldDocID
		w.toDel, w.nilDel, w.nestedDel = w.prevProps, prevNil, prevNested
	case w.status.docIDPreserved:
		delta := inverted.DeltaSkipSearchable(w.prevProps, w.nextProps, w.s.getSearchableBlockmaxProperties())
		deltaNil := inverted.DeltaNil(prevNil, nextNil)
		w.toAdd, w.toDel, w.nilAdd, w.nilDel = delta.ToAdd, delta.ToDelete, deltaNil.ToAdd, deltaNil.ToDelete
	default:
		w.toAdd, w.toDel = inverted.DedupItems(w.nextProps), inverted.DedupItems(w.prevProps)
		w.nilAdd, w.nilDel = nextNil, prevNil
	}
	if w.next != nil {
		w.addDocID, w.delDocID = w.status.docID, w.status.oldDocID
		w.nestedAdd = nextNested
		if w.prev != nil {
			w.nestedDel = prevNested
		}
	}

	if w.mirrorAdd, err = w.s.analyzeMirror(w.st, w.next); err != nil {
		return err
	}
	w.mirrorDel, err = w.s.analyzeMirror(w.st, w.prev)
	return err
}

// After all analysis, so no pin is held while waiting on the overlay lock it takes.
func (w *invertedWrite) resolve() error {
	s, cfg := w.s, w.s.index.invertedIndexConfig
	w.slots = make([]propSlots, 0, len(w.toDel)+len(w.toAdd)+len(w.nilDel)+len(w.nilAdd)+len(w.nestedDel)+len(w.nestedAdd))
	names := make([]string, 0, 2*cap(w.slots)+1)
	add := func(name string) slot {
		names = append(names, name)
		return slot(len(names) - 1)
	}

	for _, props := range [2][]inverted.Property{w.toDel, w.toAdd} {
		for i := range props {
			p, sl := &props[i], noSlots
			if p.HasFilterableIndex {
				sl.filterable = add(helpers.BucketFromPropNameLSM(p.Name))
			}
			if p.HasSearchableIndex {
				sl.searchable = add(helpers.BucketSearchableFromPropNameLSM(p.Name))
			}
			if p.HasRangeableIndex {
				sl.rangeable = add(helpers.BucketRangeableFromPropNameLSM(p.Name))
			}
			if !isMetaCountProperty(*p) && !isInternalProperty(*p) && !p.OverlayForcedOnly {
				if cfg.IndexPropertyLength && p.Length >= 0 {
					sl.length = add(helpers.BucketFromPropNameLengthLSM(p.Name))
				}
				if cfg.IndexNullState {
					sl.null = add(helpers.BucketFromPropNameNullLSM(p.Name))
				}
			}
			w.slots = append(w.slots, sl)
		}
	}
	for _, props := range [2][]inverted.NilProperty{w.nilDel, w.nilAdd} {
		for _, p := range props {
			sl := noSlots
			if cfg.IndexPropertyLength && p.AddToPropertyLength {
				sl.length = add(helpers.BucketFromPropNameLengthLSM(p.Name))
			}
			if cfg.IndexNullState {
				sl.null = add(helpers.BucketFromPropNameNullLSM(p.Name))
			}
			w.slots = append(w.slots, sl)
		}
	}
	for _, props := range [2][]inverted.NestedProperty{w.nestedDel, w.nestedAdd} {
		for i := range props {
			np, sl := &props[i], noSlots
			if np.HasFilterableIndex {
				sl.filterable = add(helpers.BucketNestedFromPropNameLSM(np.Name))
			}
			if len(np.Idx) > 0 || len(np.Exists) > 0 {
				sl.nestedMeta = add(helpers.BucketNestedMetaFromPropNameLSM(np.Name))
			}
			w.slots = append(w.slots, sl)
		}
	}
	if s.index.Config.TrackVectorDimensions {
		w.dims = add(helpers.DimensionsBucketLSM)
	}

	w.buckets = make([]*lsmkv.Bucket, len(names))
	if missing := s.store.AcquireBucketsForWrite(names, w.buckets); missing >= 0 {
		return s.refuseWrite(names[missing])
	}
	return nil
}

func (s *Shard) refuseWrite(bucket string) error {
	err := fmt.Errorf("%w: %q", lsmkv.ErrBucketNotFound, bucket)
	s.refusedWriteSampler.WithSampling(func(l logrus.FieldLogger) {
		l.WithField("bucket", bucket).
			Warnf("refused a write that needs a bucket this shard does not have, nothing was changed: %v", err)
	})
	return err
}

func (w *invertedWrite) release() {
	if w != nil {
		w.s.store.ReleaseBucketsForWrite(w.buckets)
	}
}

func (w *invertedWrite) bucket(sl slot) *lsmkv.Bucket {
	return w.buckets[sl]
}

func (w *invertedWrite) apply() error {
	s, status := w.s, w.status
	nDel, nAdd, nNil := len(w.toDel), len(w.toAdd), len(w.nilDel)+len(w.nilAdd)
	delSlots, addSlots := w.slots[:nDel], w.slots[nDel:nDel+nAdd]
	nilSlots, nestedSlots := w.slots[nDel+nAdd:nDel+nAdd+nNil], w.slots[nDel+nAdd+nNil:]

	if w.next == nil {
		if err := s.subtractPropLengths(w.prevProps); err != nil {
			return fmt.Errorf("subtract prop lengths: %w", err)
		}
		s.bitmapFactory.RemoveIds(w.delDocID)
	} else {
		if status.docIDChanged || status.docIDPreserved {
			if err := s.subtractPropLengths(w.prevProps); err != nil {
				s.index.logger.WithField("action", "subtractPropLengths").Errorf("could not subtract prop lengths: %v", err)
			}
		}
		// NotEquals inverts Equals, so the old doc ID leaves the factory before its postings.
		if status.docIDChanged {
			s.bitmapFactory.RemoveIds(status.oldDocID)
		}
		if err := s.SetPropertyLengths(w.nextProps); err != nil {
			return fmt.Errorf("store field length values for props: %w", err)
		}
	}

	if w.prev != nil {
		if err := w.deleteProps(delSlots, nilSlots[:len(w.nilDel)]); err != nil {
			return fmt.Errorf("delete inverted indices props: %w", err)
		}
		if err := w.nested(w.nestedDel, nestedSlots[:len(w.nestedDel)], w.delDocID, true); err != nil {
			return fmt.Errorf("delete nested inverted indices: %w", err)
		}
		if w.dims != noSlot {
			if err := w.prev.IterateThroughVectorDimensions(func(targetVector string, dims int) error {
				return s.addToDimensionBucket(w.bucket(w.dims), dims, w.delDocID, targetVector, true)
			}); err != nil {
				return fmt.Errorf("remove dimension tracking: %w", err)
			}
		}
	}

	if w.next != nil {
		before := time.Now()
		if err := w.addProps(addSlots, nilSlots[len(w.nilDel):]); err != nil {
			return fmt.Errorf("put inverted indices props: %w", err)
		}
		if err := w.nested(w.nestedAdd, nestedSlots[len(w.nestedDel):], w.addDocID, false); err != nil {
			return fmt.Errorf("put nested inverted indices: %w", err)
		}
		s.metrics.InvertedExtend(before, nAdd)
		if w.dims != noSlot {
			if err := w.next.IterateThroughVectorDimensions(func(targetVector string, dims int) error {
				return s.extendDimensionTrackerLSM(w.bucket(w.dims), dims, w.addDocID, targetVector)
			}); err != nil {
				return fmt.Errorf("add dimension tracking: %w", err)
			}
		}
	}

	if err := s.fireMirror(w.st, w.delDocID, w.mirrorDel, false); err != nil {
		return fmt.Errorf("migration double-write delete: %w", err)
	}
	if err := s.fireMirror(w.st, w.addDocID, w.mirrorAdd, true); err != nil {
		return fmt.Errorf("migration double-write: %w", err)
	}
	return nil
}

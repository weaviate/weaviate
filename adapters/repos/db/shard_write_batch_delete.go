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
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	enterrors "github.com/weaviate/weaviate/entities/errors"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/usecases/objects"
)

// return value map[int]error gives the error for the index as it received it
func (s *Shard) DeleteObjectBatch(ctx context.Context, uuids []strfmt.UUID, deletionTime time.Time, dryRun bool) objects.BatchSimpleObjects {
	s.activityTrackerWrite.Add(1)
	if err := s.isReadOnly(); err != nil {
		return objects.BatchSimpleObjects{
			objects.BatchSimpleObject{Err: err},
		}
	}
	return newDeleteObjectsBatcher(s).Delete(ctx, uuids, deletionTime, dryRun)
}

type deleteObjectsBatcher struct {
	sync.Mutex
	shard   ShardLike
	objects objects.BatchSimpleObjects
}

func newDeleteObjectsBatcher(shard ShardLike) *deleteObjectsBatcher {
	return &deleteObjectsBatcher{shard: shard}
}

func (b *deleteObjectsBatcher) Delete(ctx context.Context, uuids []strfmt.UUID, deletionTime time.Time, dryRun bool) objects.BatchSimpleObjects {
	b.delete(ctx, uuids, deletionTime, dryRun)
	b.flushWALs(ctx)
	return b.objects
}

func (b *deleteObjectsBatcher) delete(ctx context.Context, uuids []strfmt.UUID, deletionTime time.Time, dryRun bool) {
	b.objects = b.deleteSingleBatchInLSM(ctx, uuids, deletionTime, dryRun)
}

// deleteSingleBatchInLSM deletes the batch in two phases so the whole batch
// shares ONE durability barrier:
//
//	phase 1 (concurrent): per object, read the row and remove its inverted
//	  postings (prepareBatchDelete) — no row deletes yet.
//	barrier (once): make every phase-1 posting removal durable over the
//	  union of touched buckets (invertedDeleteBarrier).
//	phase 2 (concurrent): per object, re-validate and delete the row
//	  (finalizeBatchDelete); a row changed by a concurrent put between the
//	  phases falls back to a full per-object crash-safe delete.
//
// This order guarantees no crash can leave an orphan posting (docID in a
// posting, no object row), which — once docIDs are reused — would resolve to
// a different object.
func (b *deleteObjectsBatcher) deleteSingleBatchInLSM(ctx context.Context,
	batch []strfmt.UUID, deletionTime time.Time, dryRun bool,
) objects.BatchSimpleObjects {
	before := time.Now()
	defer b.shard.Metrics().BatchDelete(before, "shard_delete_all")

	result := make(objects.BatchSimpleObjects, len(batch))
	objLock := &sync.Mutex{}

	// if the context is expired fail all
	if err := ctx.Err(); err != nil {
		for i := range result {
			result[i] = objects.BatchSimpleObject{Err: errors.Wrap(err, "begin batch")}
		}
		return result
	}

	// phase 1: concurrent inverted cleanups, accumulating the union of
	// touched buckets; rows stay in place
	preps := make([]*preparedBatchDelete, len(batch))
	touchedUnion := newTouchedBuckets()

	eg := enterrors.NewErrorGroupWrapper(b.shard.Index().logger)
	eg.SetLimit(_NUMCPU) // prevent unbounded concurrency

	lastScheduled := -1
outer:
	for i, uuid := range batch {
		select {
		case <-ctx.Done():
			break outer
		default:
		}

		f := func() error {
			obj, prep := b.prepareObjectOfBatchInLSM(ctx, uuid, deletionTime, dryRun)
			objLock.Lock()
			result[i] = obj
			if prep != nil {
				preps[i] = prep
				touchedUnion.merge(prep.touched)
			}
			objLock.Unlock()
			return nil
		}
		eg.Go(f, i, uuid)
		lastScheduled = i

	}
	// safe to ignore error, as the internal routines never return an error
	eg.Wait()

	ctxErr := ctx.Err()
	for i, count := lastScheduled+1, len(batch); i < count; i++ {
		result[i] = objects.BatchSimpleObject{UUID: batch[i], Err: ctxErr}
	}

	// barrier: phase 1's posting removals must be durable before ANY row
	// delete below can possibly become durable
	anyPrepared := false
	for i := range preps {
		if preps[i] != nil && preps[i].found && result[i].Err == nil {
			anyPrepared = true
			break
		}
	}
	if anyPrepared {
		if err := b.shard.invertedDeleteBarrier(ctx, touchedUnion); err != nil {
			// rows were not deleted; every prepared object converges on retry
			err = errors.Wrap(err, "inverted delete barrier")
			for i := range preps {
				if preps[i] != nil && preps[i].found && result[i].Err == nil {
					result[i] = objects.BatchSimpleObject{UUID: batch[i], Err: err}
				}
			}
			return result
		}
	}

	// phase 2: concurrent re-validation + row deletes
	eg = enterrors.NewErrorGroupWrapper(b.shard.Index().logger)
	eg.SetLimit(_NUMCPU)
	for i := range preps {
		if preps[i] == nil || !preps[i].found || result[i].Err != nil {
			continue
		}
		prep := preps[i]
		f := func() error {
			err := b.shard.finalizeBatchDelete(ctx, prep, deletionTime)
			objLock.Lock()
			result[i] = objects.BatchSimpleObject{UUID: prep.uuid, Err: err}
			objLock.Unlock()
			return nil
		}
		eg.Go(f, i, prep.uuid)
	}
	eg.Wait()

	return result
}

// prepareObjectOfBatchInLSM runs phase 1 for one object of the batch. The
// returned prep is nil for dry runs and failures.
func (b *deleteObjectsBatcher) prepareObjectOfBatchInLSM(ctx context.Context,
	uuid strfmt.UUID, deletionTime time.Time, dryRun bool,
) (objects.BatchSimpleObject, *preparedBatchDelete) {
	before := time.Now()
	defer b.shard.Metrics().BatchDelete(before, "shard_delete_individual_total")
	if !dryRun {
		prep, err := b.shard.prepareBatchDelete(ctx, uuid, deletionTime)
		if err != nil {
			return objects.BatchSimpleObject{UUID: uuid, Err: err}, nil
		}
		return objects.BatchSimpleObject{UUID: uuid, Err: nil}, prep
	}

	return objects.BatchSimpleObject{UUID: uuid, Err: nil}, nil
}

func (b *deleteObjectsBatcher) flushWALs(ctx context.Context) {
	before := time.Now()
	defer b.shard.Metrics().BatchDelete(before, "shard_flush_wals")

	if err := b.shard.Store().WriteWALs(); err != nil {
		for i := range b.objects {
			b.setErrorAtIndex(err, i)
		}
	}

	_ = b.shard.ForEachVectorQueue(func(targetVector string, queue *VectorIndexQueue) error {
		if err := queue.Flush(); err != nil {
			for i := range b.objects {
				b.setErrorAtIndex(fmt.Errorf("target vector %s: %w", targetVector, err), i)
			}
		}
		return nil
	})

	_ = b.shard.ForEachGeoQueue(func(propName string, queue *VectorIndexQueue) error {
		if err := queue.Flush(); err != nil {
			for i := range b.objects {
				b.setErrorAtIndex(fmt.Errorf("geo prop %s: %w", propName, err), i)
			}
		}
		return nil
	})

	if err := b.shard.GetPropertyLengthTracker().Flush(); err != nil {
		for i := range b.objects {
			b.setErrorAtIndex(err, i)
		}
	}
}

func (b *deleteObjectsBatcher) setErrorAtIndex(err error, index int) {
	b.Lock()
	defer b.Unlock()
	b.objects[index].Err = err
}

func (s *Shard) FindUUIDs(ctx context.Context, filters *filters.LocalFilter, limit int) (uuids []strfmt.UUID, err error) {
	logger := s.index.logger.WithField("shard", s.name)
	logger.Debug("Shard::FindUUIDs started")

	start := time.Now()

	allowList, err := inverted.NewSearcher(s.index.logger, s.store, s.index.getSchema.ReadOnlyClass,
		s.propertyIndicesSnapshot(), s.index.classSearcher, s.index.getStopwordProvider(), s.versioner.version, s.isFallbackToSearchable,
		s.IsRangeableLocallyReady, s.tenant(), s.index.Config.QueryNestedRefLimit, s.bitmapFactory).
		WithTokenizationResolver(s.TokenizationFor).
		WithBatchedContainsEnabled(s.index.Config.QueryBatchedContainsEnabled).
		DocIDsLimited(ctx, filters, additional.Properties{}, s.index.Config.ClassName, limit)
	if err != nil {
		return nil, fmt.Errorf("docIds: %w", err)
	}
	defer allowList.Close()

	fetchStart := time.Now()
	it := allowList.LimitedIterator(limit) // ensures only up to [limit] docIDs will be returned
	uuids = make([]strfmt.UUID, it.Len())
	currIdx := 0

	defer func() {
		logger := logger.WithFields(logrus.Fields{
			"took":           time.Since(start).String(),
			"filter_took":    fetchStart.Sub(start).String(),
			"docids_found":   it.Len(),
			"uuids_resolved": currIdx,
		})
		if err != nil {
			// log as debug
			logger.WithError(err).Debug("Shard::FindUUIDs failed")
			return
		}
		logger.Debug("Shard::FindUUIDs finished")
	}()

	for docID, ok := it.Next(); ok; docID, ok = it.Next() {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("uuids loop: %w", ctx.Err())
		default:
		}

		uuid, err := s.uuidFromDocID(docID)
		if err != nil {
			// TODO: More than likely this will occur due to an object which has already been deleted.
			//       However, this is not a guarantee. This can be improved by logging, or handling
			//       errors other than `id not found` rather than skipping them entirely.
			s.index.logger.WithField("op", "shard.find_uuids").WithField("docID", docID).WithError(err).Debug("failed to find UUID for docID")
			continue
		}
		uuids[currIdx] = uuid
		currIdx++
	}
	return uuids[:currIdx], nil
}

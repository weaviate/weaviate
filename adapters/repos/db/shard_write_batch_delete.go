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
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/usecases/objects"
)

// deadDocIDPruneBatch is how many doc ids without an object FindUUIDs collects before
// subtracting them from the doc id universe in one go.
const deadDocIDPruneBatch = 1024

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

	eg := enterrors.NewErrorGroupWrapper(b.shard.Index().logger)
	eg.SetLimit(_NUMCPU) // prevent unbounded concurrency

	lastDeleted := -1
outer:
	for i, uuid := range batch {
		select {
		case <-ctx.Done():
			break outer
		default:
		}

		f := func() error {
			// perform delete
			obj := b.deleteObjectOfBatchInLSM(ctx, uuid, deletionTime, dryRun)
			objLock.Lock()
			result[i] = obj
			objLock.Unlock()
			return nil
		}
		eg.Go(f, i, uuid)
		lastDeleted = i

	}
	// safe to ignore error, as the internal routines never return an error
	eg.Wait()

	ctxErr := ctx.Err()
	for i, count := lastDeleted+1, len(batch); i < count; i++ {
		result[i] = objects.BatchSimpleObject{UUID: batch[i], Err: ctxErr}
	}

	return result
}

func (b *deleteObjectsBatcher) deleteObjectOfBatchInLSM(ctx context.Context,
	uuid strfmt.UUID, deletionTime time.Time, dryRun bool,
) objects.BatchSimpleObject {
	before := time.Now()
	defer b.shard.Metrics().BatchDelete(before, "shard_delete_individual_total")
	if !dryRun {
		err := b.shard.batchDeleteObject(ctx, uuid, deletionTime)
		return objects.BatchSimpleObject{UUID: uuid, Err: err}
	}

	return objects.BatchSimpleObject{UUID: uuid, Err: nil}
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

// FindUUIDs returns the UUID of every object the filter matches, at most limit of them
// when limit is positive. The limit counts UUIDs returned, not doc ids read, so a shard
// holding more than limit matching objects returns limit of them and which ones is
// unspecified. A read error fails the call rather than skipping the doc id.
//
// It mutates the shard: a doc id whose object row is gone is dropped from the doc id
// universe a deny-list filter starts from, so a later call does not read it again. That
// happens on a dry run too, since the scan is the same. Only ids below
// [Shard.docIDPruneWatermark] are dropped.
func (s *Shard) FindUUIDs(ctx context.Context, filters *filters.LocalFilter, limit int) (uuids []strfmt.UUID, err error) {
	logger := s.index.logger.WithField("shard", s.name)
	logger.Debug("Shard::FindUUIDs started")

	start := time.Now()

	// The filter resolves unbounded even when limit is positive. Pushing the limit into
	// the resolve stops the row reader at limit+1 doc ids for a leaf allow-list filter
	// (inverted/searcher_doc_bitmap.go:96), and a dead doc id inside a window that short
	// has nothing left to be retried against, which drops the reply to exactly limit.
	allowList, err := inverted.NewSearcher(s.index.logger, s.store, s.index.getSchema.ReadOnlyClass,
		s.propertyIndicesSnapshot(), s.index.classSearcher, s.index.getStopwordProvider(), s.versioner.version, s.isFallbackToSearchable,
		s.IsRangeableLocallyReady, s.tenant(), s.index.Config.QueryNestedRefLimit, s.bitmapFactory).
		WithTokenizationResolver(s.TokenizationFor).
		WithBatchedContainsEnabled(s.index.Config.QueryBatchedContainsEnabled).
		DocIDs(ctx, filters, additional.Properties{}, s.index.Config.ClassName)
	if err != nil {
		return nil, fmt.Errorf("docIds: %w", err)
	}
	defer allowList.Close()

	fetchStart := time.Now()
	// limit counts UUIDs returned, not doc ids read: the allow list above is complete,
	// so a dead doc id is retried against the next one and a shard holding more than
	// limit matching objects returns limit of them.
	it := allowList.Iterator()
	capacity := it.Len()
	if limit > 0 && limit < capacity {
		capacity = limit
	}
	uuids = make([]strfmt.UUID, capacity)
	currIdx := 0

	bucket, release, err := s.objectsBucket()
	if err != nil {
		return nil, fmt.Errorf("objects bucket: %w", err)
	}
	defer release()

	lookup, releaseView := bucket.SecondaryViewLookup()
	defer releaseView()

	docIDBuf := make([]byte, 8)
	// Reused across iterations and grown to fit the largest object seen. Safe to reuse
	// because uuidFromDocIDWithLookup copies the id out before returning.
	var objBuf []byte

	// A doc id with no object is dropped from the doc id universe a deny-list filter
	// starts from, which is otherwise rebuilt with every deleted id in it at shard init.
	// Only below the watermark: at or above it the id may be an allocated-but-unwritten
	// insert, and dropping that one hides a live object until the next shard init.
	watermark := s.docIDPruneWatermark
	deadDocIDs := sroar.NewBitmap()
	deadCount := 0
	pruneDeadDocIDs := func() {
		if deadCount == 0 {
			return
		}
		s.bitmapFactory.Remove(deadDocIDs)
		deadDocIDs = sroar.NewBitmap().CloneToBuf(deadDocIDs.ToBuffer())
		deadCount = 0
	}
	defer pruneDeadDocIDs()

	defer func() {
		logger := logger.WithFields(logrus.Fields{
			"took":           time.Since(start).String(),
			"filter_took":    fetchStart.Sub(start).String(),
			"docids_found":   it.Len(),
			"uuids_resolved": currIdx,
		})
		if err != nil {
			// log as debug
			logger.Debugf("Shard::FindUUIDs failed: %v", err)
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

		uuid, newBuf, found, err := uuidFromDocIDWithLookup(ctx, lookup, docID, docIDBuf, objBuf)
		objBuf = newBuf
		if err != nil {
			return nil, fmt.Errorf("resolve doc id %d: %w", docID, err)
		}
		if !found {
			if docID < watermark && deadDocIDs.Set(docID) {
				if deadCount++; deadCount >= deadDocIDPruneBatch {
					pruneDeadDocIDs()
				}
			}
			continue
		}

		uuids[currIdx] = uuid
		currIdx++
		if limit > 0 && currIdx == limit {
			break
		}
	}
	return uuids[:currIdx], nil
}

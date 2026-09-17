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
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
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
// The filter resolves capped at limit, which is what every caller paid before the cap
// on UUIDs existed. Only when that cap both truncated the allow list and the walk
// skipped a doc id whose object row was gone is the filter resolved a second time with
// no cap: those are the calls whose reply would otherwise be short through no fault of
// the data. A shard that matched fewer objects than limit has already given its whole
// answer and is never resolved twice.
//
// It mutates the shard: a doc id whose object row is gone is dropped from the doc id
// universe a deny-list filter starts from, so a later call does not read it again. That
// happens on a dry run too, since the scan is the same. Only ids below
// [Shard.docIDPruneWatermark] are dropped.
func (s *Shard) FindUUIDs(ctx context.Context, filters *filters.LocalFilter, limit int) (uuids []strfmt.UUID, err error) {
	logger := s.index.logger.WithField("shard", s.name)
	logger.Debug("Shard::FindUUIDs started")

	start := time.Now()

	bucket, release, err := s.objectsBucket()
	if err != nil {
		return nil, fmt.Errorf("objects bucket: %w", err)
	}
	defer release()

	lookup, releaseView := bucket.SecondaryViewLookup()
	defer releaseView()

	var (
		docIDsRead  int
		pruned      int
		kept        int
		resolveTook time.Duration
		secondPass  bool
	)

	defer func() {
		logger := logger.WithFields(logrus.Fields{
			"took":           time.Since(start).String(),
			"filter_took":    resolveTook.String(),
			"docids_found":   docIDsRead,
			"uuids_resolved": len(uuids),
			// pruned and kept are summed over both passes. A kept doc id is read again by
			// every later call, so kept rising over a shard's life is a write that took a
			// doc id and never wrote a row.
			"dead_docids_pruned": pruned,
			"dead_docids_kept":   kept,
			"second_pass":        secondPass,
		})
		if err != nil {
			// log as debug
			logger.Debugf("Shard::FindUUIDs failed: %v", err)
			return
		}
		logger.Debug("Shard::FindUUIDs finished")
	}()

	pass, err := s.resolveAndCollect(ctx, filters, limit, limit, lookup)
	docIDsRead, pruned, kept, resolveTook = pass.docIDsRead, pass.pruned, pass.kept, pass.resolveTook
	if err != nil {
		return nil, err
	}
	uuids = pass.uuids

	// A short reply is only suspect when the resolve could have been cut off, which the
	// searcher does not report: an allow list at least as long as the limit it was given
	// may have been truncated at it (inverted/searcher_doc_bitmap.go:96).
	if limit > 0 && len(uuids) < limit && pass.skippedDead && pass.docIDsRead >= limit {
		secondPass = true

		var uncapped findUUIDsPass
		uncapped, err = s.resolveAndCollect(ctx, filters, 0, limit, lookup)
		docIDsRead = uncapped.docIDsRead
		pruned += uncapped.pruned
		kept += uncapped.kept
		resolveTook += uncapped.resolveTook
		if err != nil {
			return nil, err
		}
		uuids = uncapped.uuids
	}

	return uuids, nil
}

// findUUIDsPass is what one resolve and walk produced. docIDsRead separates a resolve
// the searcher may have cut off at the limit from a shard that simply matched fewer
// objects, which is the difference between "resolve again" and "this is the answer".
type findUUIDsPass struct {
	uuids       []strfmt.UUID
	docIDsRead  int
	skippedDead bool
	pruned      int
	kept        int
	resolveTook time.Duration
}

// resolveAndCollect resolves filter and walks the allow list for at most limit UUIDs.
// resolveLimit caps the inverted resolve itself; zero leaves it uncapped. The allow list
// is released before this returns, so a caller running a second pass holds only one.
func (s *Shard) resolveAndCollect(ctx context.Context, filter *filters.LocalFilter,
	resolveLimit, limit int, lookup secondaryDocIDLookup,
) (pass findUUIDsPass, err error) {
	resolveStart := time.Now()

	searcher := inverted.NewSearcher(s.index.logger, s.store, s.index.getSchema.ReadOnlyClass,
		s.propertyIndicesSnapshot(), s.index.classSearcher, s.index.getStopwordProvider(), s.versioner.version, s.isFallbackToSearchable,
		s.IsRangeableLocallyReady, s.tenant(), s.index.Config.QueryNestedRefLimit, s.bitmapFactory).
		WithTokenizationResolver(s.TokenizationFor).
		WithBatchedContainsEnabled(s.index.Config.QueryBatchedContainsEnabled)

	var allowList helpers.AllowList
	if resolveLimit > 0 {
		allowList, err = searcher.DocIDsLimited(ctx, filter, additional.Properties{},
			s.index.Config.ClassName, resolveLimit)
	} else {
		allowList, err = searcher.DocIDs(ctx, filter, additional.Properties{},
			s.index.Config.ClassName)
	}
	if err != nil {
		return pass, fmt.Errorf("docIds: %w", err)
	}
	defer allowList.Close()

	pass.resolveTook = time.Since(resolveStart)

	it := allowList.Iterator()
	pass.docIDsRead = it.Len()

	capacity := pass.docIDsRead
	if limit > 0 && limit < capacity {
		capacity = limit
	}
	uuids := make([]strfmt.UUID, capacity)
	currIdx := 0

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
		pass.pruned += deadCount
		deadDocIDs = sroar.NewBitmap().CloneToBuf(deadDocIDs.ToBuffer())
		deadCount = 0
	}
	defer pruneDeadDocIDs()

	for docID, ok := it.Next(); ok; docID, ok = it.Next() {
		select {
		case <-ctx.Done():
			return pass, fmt.Errorf("uuids loop: %w", ctx.Err())
		default:
		}

		uuid, newBuf, found, err := uuidFromDocIDWithLookup(ctx, lookup, docID, docIDBuf, objBuf)
		objBuf = newBuf
		if err != nil {
			return pass, fmt.Errorf("resolve doc id %d: %w", docID, err)
		}
		if !found {
			pass.skippedDead = true
			if docID >= watermark {
				pass.kept++
				continue
			}
			if deadDocIDs.Set(docID) {
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

	pass.uuids = uuids[:currIdx]
	return pass, nil
}

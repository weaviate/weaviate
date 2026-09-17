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

// deadDocIDPruneBatch is how many doc ids with no object row FindUUIDs collects before
// it subtracts them from the doc id universe in one go.
const deadDocIDPruneBatch = 1024

// resolveUncapped is the resolveLimit that leaves the inverted resolve uncapped, which is
// the sentinel inverted.Searcher.DocIDs takes for the same thing.
const resolveUncapped = 0

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
// The filter resolves capped at limit, which is what every caller paid before the cap on
// UUIDs existed, and is resolved again without the cap when the bounded resolve may have
// been cut off by it. A shard that matched fewer objects than limit has already given its
// whole answer and is never resolved twice.
//
// The object view is taken before the resolve, so a row written into a new active
// memtable after a flush swap is invisible to this call. Such ids are at or above
// [Shard.docIDPruneWatermark] and are kept, never pruned.
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
		total      findUUIDsPass
		secondPass bool
	)

	defer func() {
		logger := logger.WithFields(logrus.Fields{
			"took":               time.Since(start).String(),
			"filter_took":        total.resolveTook.String(),
			"docids_found":       total.docIDsRead,
			"uuids_resolved":     len(uuids),
			"dead_docids_pruned": total.pruned,
			// A doc id above the watermark is read again by every later call, so a count
			// that rises over a shard's life is the signal that a write took a doc id and
			// never wrote its row.
			"dead_docids_above_watermark": total.kept,
			"second_pass":                 secondPass,
		})
		if err != nil {
			// log as debug
			logger.Debugf("Shard::FindUUIDs failed: %v", err)
			return
		}
		logger.Debug("Shard::FindUUIDs finished")
	}()

	total, err = s.resolveAndCollect(ctx, filters, limit, limit, lookup)
	if err != nil {
		return nil, err
	}
	uuids = total.uuids

	// A short reply is only suspect when the resolve could have been cut off, and the
	// searcher does not report that. The one signal it leaves is an allow list at least
	// as long as the limit it was given (inverted/searcher_doc_bitmap.go:96).
	//
	// Two shapes satisfy this without having been truncated, and both pay an uncapped
	// resolve for nothing: a compound root, which inverted/prop_value_pairs.go:142 and
	// :227 resolve with the limit dropped, and a deny list, which is resolved against the
	// whole doc id universe and after a restart carries the ids earlier calls deleted.
	// Telling them apart needs a truncated bit on helpers.AllowList that the searcher does
	// not set today.
	if limit > 0 && len(uuids) < limit && total.skippedDead && total.docIDsRead >= limit {
		secondPass = true

		var uncapped findUUIDsPass
		uncapped, err = s.resolveAndCollect(ctx, filters, resolveUncapped, limit, lookup)
		total.docIDsRead = uncapped.docIDsRead
		// pruned, kept and resolveTook are summed over both passes; docIDsRead is the
		// second pass's, since the first pass's ids are a subset of it.
		total.pruned += uncapped.pruned
		total.kept += uncapped.kept
		total.resolveTook += uncapped.resolveTook
		if err != nil {
			return nil, err
		}
		uuids = uncapped.uuids

		// Above debug because this is the one cost an operator cannot see from the reply:
		// the call paid a full uncapped resolve of the filter. Bounded by call rate, at
		// most one line per call.
		logger.WithFields(logrus.Fields{
			"action":       "find_uuids_second_resolve",
			"limit":        limit,
			"docids_found": uncapped.docIDsRead,
		}).Infof("batch delete resolved the filter a second time with no cap: a doc id in the window bounded at %d had no object row", limit)
	}

	return uuids, nil
}

// findUUIDsPass is the result of one resolve-and-walk pass: the UUIDs it produced and
// what the walk saw on the way there.
type findUUIDsPass struct {
	// uuids is what the walk resolved, at most limit of them.
	uuids []strfmt.UUID
	// docIDsRead is the allow list's length. It separates a resolve the searcher may have
	// cut off at the limit from a shard that simply matched fewer objects, which is the
	// difference between "resolve again" and "this is the answer".
	docIDsRead int
	// skippedDead is whether the walk read a doc id with no object row behind it.
	skippedDead bool
	// pruned is how many of those ids left the doc id universe, kept how many stayed
	// because they sit at or above the watermark.
	pruned int
	kept   int
	// resolveTook is how long the inverted resolve took, without the walk.
	resolveTook time.Duration
}

// resolveAndCollect resolves the filter and walks the allow list for at most limit UUIDs.
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
	// objBuf is reused across iterations and grows to fit the largest object seen. That
	// is safe because uuidFromDocIDWithLookup copies the id out before returning.
	var objBuf []byte

	// A doc id with no object row is dropped from the doc id universe a deny-list filter
	// starts from, which is otherwise rebuilt with every deleted id in it at shard init.
	// Only below the watermark: at or above it the id may be an allocated-but-unwritten
	// insert, and dropping that one hides a live object until the next shard init.
	watermark := s.docIDPruneWatermark
	deadDocIDs := sroar.NewBitmap()
	deadCount := 0
	// pruneDeadDocIDs writes through the named result, which the deferred call below needs:
	// with a plain local, the last partial batch would land in a copy the caller never sees.
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

	// noteDeadDocID records one doc id the walk read with no object row behind it.
	noteDeadDocID := func(docID uint64) {
		pass.skippedDead = true
		if docID >= watermark {
			pass.kept++
			return
		}
		if deadDocIDs.Set(docID) {
			if deadCount++; deadCount >= deadDocIDPruneBatch {
				pruneDeadDocIDs()
			}
		}
	}

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
			noteDeadDocID(docID)
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

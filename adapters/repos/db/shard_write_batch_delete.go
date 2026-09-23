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
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
)

// deadDocIDPruneBatch is how many doc ids with no object row FindUUIDs collects before
// it subtracts them from the doc id universe in one go.
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
// unspecified. A read error fails the call rather than skipping the doc id. A row that
// carries no readable id is skipped, and the skips are logged at most once per
// unreadableRowLogWindow per shard.
//
// The filter is resolved once and without a cap, so a doc id whose object row is gone, or
// whose row carries no readable id, cannot shorten the reply: the walk reads past it to the
// next match.
//
// The object rows are read after the resolve, through one consistent view per bucket
// call. A doc id an insert has taken but not yet written reads as missing; such ids are at
// or above [Shard.docIDPruneWatermark] and are kept, never pruned. An id below it only
// ever goes from live to dead, so a later view cannot turn a pruned id back into a row.
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

	var pass findUUIDsPass

	defer func() {
		logger := logger.WithFields(logrus.Fields{
			"took":               time.Since(start).String(),
			"filter_took":        pass.resolveTook.String(),
			"docids_found":       pass.docIDsRead,
			"uuids_resolved":     len(uuids),
			"dead_docids_pruned": pass.pruned,
			// A doc id above the watermark is read again by every later call, so a count
			// that rises over a shard's life is the signal that a write took a doc id and
			// never wrote its row.
			"dead_docids_above_watermark": pass.kept,
		})
		if err != nil {
			logger.Debugf("Shard::FindUUIDs failed: %v", err)
			return
		}
		logger.Debug("Shard::FindUUIDs finished")
	}()

	pass, err = s.resolveAndCollectUUIDs(ctx, filters, limit, bucket)
	if err != nil {
		return nil, err
	}

	if pass.unreadable.count > 0 {
		s.warnUnreadableRows(logger, pass.unreadable)
	}
	return pass.uuids, nil
}

// unreadableRowLogWindow is how often one shard warns about rows with no readable id.
const unreadableRowLogWindow = time.Minute

// warnUnreadableRows reports the rows a call skipped because they carry no readable id.
func (s *Shard) warnUnreadableRows(logger logrus.FieldLogger, unreadable unreadableRows) {
	write := func(l logrus.FieldLogger) {
		l.WithField("op", "shard.find_uuids").
			Warnf("skipped %d doc ids without a readable id, one of them: %v",
				unreadable.count, unreadable.first)
	}

	// A Shard built without NewShard has no sampler.
	if s.unreadableRowSampler == nil {
		write(logger)
		return
	}
	s.unreadableRowSampler.WithSampling(write)
}

// findUUIDsPass is the result of one resolve-and-walk pass: the UUIDs it produced and
// what the walk saw on the way there.
type findUUIDsPass struct {
	// uuids is what the walk resolved, at most limit of them.
	uuids []strfmt.UUID
	// docIDsRead is the allow list's length.
	docIDsRead int
	// pruned is how many doc ids with no object row left the doc id universe, kept how
	// many stayed because they sit at or above the watermark.
	pruned int
	kept   int
	// unreadable counts rows the walk skipped because they carry no readable id. Those
	// rows exist, so their doc ids are never pruned.
	unreadable unreadableRows
	// resolveTook is how long the inverted resolve took, without the walk.
	resolveTook time.Duration
}

// unreadableRows counts object rows a resolve read that carry no readable id.
type unreadableRows struct {
	count int
	// first is the parse error of one of them, for the log line.
	first error
}

// resolveAndCollectUUIDs resolves the filter with no cap and walks the allow list for at
// most limit UUIDs.
func (s *Shard) resolveAndCollectUUIDs(ctx context.Context, filter *filters.LocalFilter,
	limit int, bucket docIDBatchBucket,
) (pass findUUIDsPass, err error) {
	resolveStart := time.Now()

	searcher := inverted.NewSearcher(s.index.logger, s.store, s.index.getSchema.ReadOnlyClass,
		s.propertyIndicesSnapshot(), s.index.classSearcher, s.index.getStopwordProvider(), s.versioner.version, s.isFallbackToSearchable,
		s.IsRangeableLocallyReady, s.tenant(), s.index.Config.QueryNestedRefLimit, s.bitmapFactory).
		WithTokenizationResolver(s.TokenizationFor).
		WithBatchedContainsEnabled(s.index.Config.QueryBatchedContainsEnabled)

	allowList, err := searcher.DocIDs(ctx, filter, additional.Properties{},
		s.index.Config.ClassName)
	if err != nil {
		return pass, fmt.Errorf("docIds: %w", err)
	}
	defer allowList.Close()

	pass.resolveTook = time.Since(resolveStart)

	it := allowList.Iterator()
	pass.docIDsRead = it.Len()

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

	want := pass.docIDsRead
	if limit > 0 {
		want = min(limit, want)
	}
	pass.uuids, pass.unreadable, err = resolveUUIDs(ctx, bucket, it, want, noteDeadDocID)
	return pass, err
}

// docIDBatchBucket is the objects bucket as the uuid resolve uses it.
type docIDBatchBucket interface {
	GetBySecondaryBatch(ctx context.Context, pos int, keys [][]byte, visit func(i int, value []byte) error) error
}

// docIDIterator yields the doc ids to read.
type docIDIterator interface {
	Next() (uint64, bool)
	Len() int
}

// resolveUUIDs reads the id of the object behind each of the iterator's doc ids until it
// has limit of them. A doc id with no object row costs no slot and goes to onMissing,
// which runs on the calling goroutine. An object whose stored bytes carry no readable id
// is skipped and counted in the returned unreadableRows. A bucket read error fails the
// call.
func resolveUUIDs(ctx context.Context, bucket docIDBatchBucket, it docIDIterator, limit int,
	onMissing func(docID uint64),
) ([]strfmt.UUID, unreadableRows, error) {
	// The searcher sets its concurrency budget on a context it does not return,
	// so the uuid resolve has to set its own.
	ctx = concurrency.CtxWithBudgetIfAbsent(ctx, concurrency.TimesGOMAXPROCS(2))

	var unreadableMu sync.Mutex
	var unreadable unreadableRows
	uuids, err := storobj.DecodeByDocID(ctx, bucket, it, limit, func(docID uint64, object []byte) (strfmt.UUID, bool, error) {
		if object == nil { // deleted after the allow list was built, or dead since before
			onMissing(docID)
			return "", false, nil
		}
		prop, _, err := storobj.ParseAndExtractProperty(object, "id")
		if err == nil && len(prop) == 0 {
			err = errors.New("no id property")
		}
		if err != nil {
			unreadableMu.Lock()
			defer unreadableMu.Unlock()
			if unreadable.count++; unreadable.first == nil {
				unreadable.first = fmt.Errorf("doc id %d: %w", docID, err)
			}
			return "", false, nil
		}
		return strfmt.UUID(prop[0]), true, nil
	})
	if err != nil {
		return nil, unreadableRows{}, fmt.Errorf("resolve uuids: %w", err)
	}
	return uuids, unreadable, nil
}

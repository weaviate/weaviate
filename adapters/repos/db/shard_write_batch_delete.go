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
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/storobj"
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

	defer func() {
		logger := logger.WithFields(logrus.Fields{
			"took":           time.Since(start).String(),
			"filter_took":    fetchStart.Sub(start).String(),
			"docids_found":   it.Len(),
			"uuids_resolved": len(uuids),
		})
		if err != nil {
			logger.Debugf("Shard::FindUUIDs failed: %v", err)
			return
		}
		logger.Debug("Shard::FindUUIDs finished")
	}()

	bucket, release, err := s.objectsBucket()
	if err != nil {
		return nil, err
	}
	defer release()

	uuids, err = resolveUUIDs(ctx, logger, bucket, it)
	return uuids, err
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

// resolveUUIDs reads the id of every object the iterator's doc ids point at. An
// object whose stored bytes carry no readable id is skipped, and the skips are
// logged once after the read. A bucket read error fails the call.
func resolveUUIDs(ctx context.Context, logger logrus.FieldLogger, bucket docIDBatchBucket, it docIDIterator) ([]strfmt.UUID, error) {
	// DocIDsLimited sets its concurrency budget on a context it does not return,
	// so the uuid resolve has to set its own.
	ctx = concurrency.CtxWithBudgetIfAbsent(ctx, concurrency.TimesGOMAXPROCS(2))

	var unreadableMu sync.Mutex
	var unreadable int
	var unreadableCause error
	uuids, err := storobj.DecodeByDocID(ctx, bucket, it, it.Len(), func(docID uint64, object []byte) (strfmt.UUID, bool, error) {
		if object == nil { // deleted after the allow list was built
			return "", false, nil
		}
		prop, _, err := storobj.ParseAndExtractProperty(object, "id")
		if err == nil && len(prop) == 0 {
			err = errors.New("no id property")
		}
		if err != nil {
			unreadableMu.Lock()
			defer unreadableMu.Unlock()
			if unreadable++; unreadableCause == nil {
				unreadableCause = fmt.Errorf("doc id %d: %w", docID, err)
			}
			return "", false, nil
		}
		return strfmt.UUID(prop[0]), true, nil
	})
	if err != nil {
		return nil, fmt.Errorf("resolve uuids: %w", err)
	}
	if unreadable > 0 {
		logger.WithField("op", "shard.find_uuids").
			Warnf("skipped %d doc ids without a readable id, one of them: %v", unreadable, unreadableCause)
	}
	return uuids, nil
}

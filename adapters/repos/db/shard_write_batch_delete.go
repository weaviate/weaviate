//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/usecases/objects"
)

// return value map[int]error gives the error for the index as it received it
func (s *Shard) DeleteObjectBatch(ctx context.Context, docIDs []uint64, dryRun bool) objects.BatchSimpleObjects {
	if s.isReadOnly() {
		return objects.BatchSimpleObjects{
			objects.BatchSimpleObject{Err: storagestate.ErrStatusReadOnly},
		}
	}
	return newDeleteObjectsBatcher(s).Delete(ctx, docIDs, dryRun)
}

type deleteObjectsBatcher struct {
	sync.Mutex
	shard   ShardLike
	objects objects.BatchSimpleObjects
}

func newDeleteObjectsBatcher(shard ShardLike) *deleteObjectsBatcher {
	return &deleteObjectsBatcher{shard: shard}
}

func (b *deleteObjectsBatcher) Delete(ctx context.Context, docIDs []uint64, dryRun bool) objects.BatchSimpleObjects {
	b.delete(ctx, docIDs, dryRun)
	b.flushWALs(ctx)
	return b.objects
}

func (b *deleteObjectsBatcher) delete(ctx context.Context, docIDs []uint64, dryRun bool) {
	b.objects = b.deleteSingleBatchInLSM(ctx, docIDs, dryRun)
}

func (b *deleteObjectsBatcher) deleteSingleBatchInLSM(ctx context.Context, batch []uint64, dryRun bool) objects.BatchSimpleObjects {
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

	wg := &sync.WaitGroup{}
	for j, docID := range batch {
		wg.Add(1)
		go func(index int, docID uint64, dryRun bool) {
			defer wg.Done()
			// perform delete
			obj := b.deleteObjectOfBatchInLSM(ctx, docID, dryRun)
			objLock.Lock()
			result[index] = obj
			objLock.Unlock()
		}(j, docID, dryRun)
	}
	wg.Wait()

	return result
}

func (b *deleteObjectsBatcher) deleteObjectOfBatchInLSM(ctx context.Context, docID uint64, dryRun bool) objects.BatchSimpleObject {
	before := time.Now()
	defer b.shard.Metrics().BatchDelete(before, "shard_delete_individual_total")

	uuid, err := b.shard.uuidFromDocID(docID)
	if err != nil {
		return objects.BatchSimpleObject{UUID: uuid, Err: err}
	}

	if !dryRun {
		err := b.shard.batchDeleteObject(ctx, uuid)
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

	if err := b.shard.VectorIndex().Flush(); err != nil {
		for i := range b.objects {
			b.setErrorAtIndex(err, i)
		}
	}

	if err := b.shard.GetPropertyLengthTracker().Flush(false); err != nil {
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

func (s *Shard) FindDocIDs(ctx context.Context, filters *filters.LocalFilter) ([]uint64, error) {
	allowList, err := inverted.NewSearcher(s.index.logger, s.store,
		s.index.getSchema.GetSchemaSkipAuth(), nil,
		s.index.classSearcher, s.deletedDocIDs, s.index.stopwords,
		s.versioner.version, s.isFallbackToSearchable,
		s.tenant(), s.index.Config.QueryNestedRefLimit).
		DocIDs(ctx, filters, additional.Properties{}, s.index.Config.ClassName)
	if err != nil {
		return nil, err
	}
	return allowList.Slice(), nil
}

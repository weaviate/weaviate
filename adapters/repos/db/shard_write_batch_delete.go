//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/objects"
)

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

	// If the context is expired, fail all
	if err := ctx.Err(); err != nil {
		for i := range result {
			result[i] = objects.BatchSimpleObject{Err: errors.Wrap(err, "begin batch")}
		}
		return result
	}

	eg := enterrors.NewErrorGroupWrapper(b.shard.Index().logger)
	eg.SetLimit(runtime.GOMAXPROCS(0)) // Prevent unbounded concurrency

	for j, uuid := range batch {
		index := j
		uuid := uuid
		f := func() error {
			// Perform delete
			obj := b.deleteObjectOfBatchInLSM(ctx, uuid, deletionTime, dryRun)
			objLock.Lock()
			result[index] = obj
			objLock.Unlock()
			return nil
		}
		eg.Go(f, index, uuid)
	}
	// Wait for all deletions to complete
	eg.Wait()

	return result
}

func (b *deleteObjectsBatcher) deleteObjectOfBatchInLSM(ctx context.Context,
	uuid strfmt.UUID, deletionTime time.Time, dryRun bool,
) objects.BatchSimpleObject {
	before := time.Now()
	defer b.shard.Metrics().BatchDelete(before, "shard_delete_individual_total")

	if !dryRun {
		err := b.shard.batchDeleteObject(ctx, uuid, deletionTime)
		if err != nil {
			b.shard.Index().logger.WithFields(map[string]interface{}{
				"uuid": uuid,
				"err":  err,
			}).Error("Failed to delete object from BatchDeleteObject")
			return objects.BatchSimpleObject{UUID: uuid, Err: err}
		}
	}

	return objects.BatchSimpleObject{UUID: uuid, Err: nil}
}

func (b *deleteObjectsBatcher) flushWALs(ctx context.Context) {
	before := time.Now()
	defer b.shard.Metrics().BatchDelete(before, "shard_flush_wals")

	// Flush the main Write-Ahead Logs
	if err := b.shard.Store().WriteWALs(); err != nil {
		b.shard.Index().logger.WithError(err).Error("Failed to write WALs during flush")
		for i := range b.objects {
			b.setErrorAtIndex(err, i)
		}
	}

	// Flush all queues, including vector index queues
	if b.shard.hasTargetVectors() {
		for targetVector, queue := range b.shard.Queues() {
			if err := queue.Flush(); err != nil {
				b.shard.Index().logger.WithFields(map[string]interface{}{
					"targetVector": targetVector,
					"err":          err,
				}).Error("Failed to flush target vector queue")
				for i := range b.objects {
					b.setErrorAtIndex(fmt.Errorf("target vector %s: %w", targetVector, err), i)
				}
			}
		}
	} else {
		if err := b.shard.Queue().Flush(); err != nil {
			b.shard.Index().logger.WithError(err).Error("Failed to flush default queue")
			for i := range b.objects {
				b.setErrorAtIndex(err, i)
			}
		}
	}

	// Flush the property length tracker
	if err := b.shard.GetPropertyLengthTracker().Flush(); err != nil {
		b.shard.Index().logger.WithError(err).Error("Failed to flush property length tracker")
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

// DeleteObjectBatch initiates the batch deletion process.
func (s *Shard) DeleteObjectBatch(ctx context.Context, uuids []strfmt.UUID, deletionTime time.Time, dryRun bool) objects.BatchSimpleObjects {
	s.activityTracker.Add(1)
	if err := s.isReadOnly(); err != nil {
		return objects.BatchSimpleObjects{
			objects.BatchSimpleObject{Err: err},
		}
	}
	return newDeleteObjectsBatcher(s).Delete(ctx, uuids, deletionTime, dryRun)
}

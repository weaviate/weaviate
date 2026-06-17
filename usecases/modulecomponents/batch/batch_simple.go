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

package batch

import (
	"context"
	"fmt"
	"sync"

	"github.com/sirupsen/logrus"
	"golang.org/x/time/rate"

	"github.com/weaviate/weaviate/entities/dto"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
)

type objectVectorizer[T dto.Embedding] func(context.Context, *models.Object, moduletools.ClassConfig) (T, models.AdditionalProperties, error)

type batchObjectsVectorizer[T dto.Embedding] func(context.Context, []*models.Object, moduletools.ClassConfig) ([]T, models.AdditionalProperties, error)

// BatchSimple exposes the per-object (VectorizeBatch) and per-batch
// (VectorizeBatchObjects) vectorization helpers used by modules that do not
// need the full token-aware Batch[T] pipeline.
//
// When a positive RPM is supplied to NewBatchSimple, requests are paced by a
// token-bucket limiter. One limiter token is reserved per input embedding
// (NOT per HTTP request) because some providers — e.g. Google Gemini — count
// each embedding as a separate request against the RPM quota even when
// multiple inputs are batched into a single HTTP call.
type BatchSimple[T dto.Embedding] struct {
	logger      logrus.FieldLogger
	rateLimiter *rate.Limiter
}

// NewBatchSimple returns a BatchSimple. When rpm > 0 requests are limited to
// approximately rpm embeddings per minute with a burst of max(rpm/60, 1).
// When rpm <= 0 no rate limiting is applied.
func NewBatchSimple[T dto.Embedding](logger logrus.FieldLogger, rpm int) *BatchSimple[T] {
	sb := &BatchSimple[T]{logger: logger}
	if rpm > 0 {
		sb.rateLimiter = rate.NewLimiter(rate.Limit(float64(rpm)/60.0), max(rpm/60, 1))
	}
	return sb
}

// waitForQuota blocks until the rate limiter grants quota for n embeddings.
// Requests larger than the limiter's burst size are reserved in chunks.
// Returns nil immediately when the limiter is disabled.
func (b *BatchSimple[T]) waitForQuota(ctx context.Context, n int) error {
	if b.rateLimiter == nil {
		return nil
	}
	for n > 0 {
		take := min(n, b.rateLimiter.Burst())
		if err := b.rateLimiter.WaitN(ctx, take); err != nil {
			return fmt.Errorf("wait for rate limit quota: %w", err)
		}
		n -= take
	}
	return nil
}

func (b *BatchSimple[T]) VectorizeBatch(
	ctx context.Context,
	objs []*models.Object,
	skipObject []bool,
	cfg moduletools.ClassConfig,
	vectorize objectVectorizer[T],
) ([]T, []models.AdditionalProperties, map[int]error) {
	vecs := make([]T, len(objs))
	// error should be the exception so dont preallocate
	errs := make(map[int]error, 0)
	errorLock := sync.Mutex{}

	// error group is used to limit concurrency
	eg := enterrors.NewErrorGroupWrapper(b.logger)
	eg.SetLimit(_NUMCPU * 2)
	for i := range objs {
		i := i

		if skipObject[i] {
			continue
		}
		eg.Go(func() error {
			if err := b.waitForQuota(ctx, 1); err != nil {
				errorLock.Lock()
				defer errorLock.Unlock()
				errs[i] = err
				return nil
			}
			vec, _, err := vectorize(ctx, objs[i], cfg)
			if err != nil {
				errorLock.Lock()
				defer errorLock.Unlock()
				errs[i] = err
			}
			vecs[i] = vec
			return nil
		})
	}
	err := eg.Wait()
	if err != nil {
		for i := range objs {
			if skipObject[i] {
				continue
			}
			errs[i] = err
		}
		return nil, nil, errs
	}
	return vecs, nil, errs
}

type batchObjects struct {
	indexes []int
	objs    []*models.Object
}

func (b *BatchSimple[T]) VectorizeBatchObjects(
	ctx context.Context,
	objs []*models.Object,
	skipObject []bool,
	cfg moduletools.ClassConfig,
	vectorize batchObjectsVectorizer[T],
	batchSize int,
) ([]T, []models.AdditionalProperties, map[int]error) {
	vecs := make([]T, len(objs))
	// error should be the exception so dont preallocate
	errs := make(map[int]error, 0)
	errorLock := sync.Mutex{}
	// create batches
	batchOfObjects := make([]batchObjects, 1)
	j := 0
	for i := range objs {
		if skipObject[i] {
			continue
		}
		if len(batchOfObjects[j].objs) >= batchSize {
			batchOfObjects = append(batchOfObjects, batchObjects{})
			j++
		}
		batchOfObjects[j].indexes = append(batchOfObjects[j].indexes, i)
		batchOfObjects[j].objs = append(batchOfObjects[j].objs, objs[i])
	}
	// error group is used to limit concurrency
	eg := enterrors.NewErrorGroupWrapper(b.logger)
	eg.SetLimit(_NUMCPU * 2)
	for batchIndex := range batchOfObjects {
		eg.Go(func() error {
			if err := b.waitForQuota(ctx, len(batchOfObjects[batchIndex].objs)); err != nil {
				errorLock.Lock()
				defer errorLock.Unlock()
				for _, index := range batchOfObjects[batchIndex].indexes {
					errs[index] = err
				}
				return nil
			}
			res, _, err := vectorize(ctx, batchOfObjects[batchIndex].objs, cfg)
			if err != nil {
				errorLock.Lock()
				defer errorLock.Unlock()
				for _, index := range batchOfObjects[batchIndex].indexes {
					errs[index] = err
				}
			} else {
				for i, index := range batchOfObjects[batchIndex].indexes {
					vecs[index] = res[i]
				}
			}
			return nil
		})
	}
	err := eg.Wait()
	if err != nil {
		for i := range objs {
			if skipObject[i] {
				continue
			}
			errs[i] = err
		}
		return nil, nil, errs
	}
	return vecs, nil, errs
}

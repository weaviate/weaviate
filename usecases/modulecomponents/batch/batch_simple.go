//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package batch

import (
	"context"
	"sync"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/dto"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
)

type objectVectorizer[T dto.Embedding] func(context.Context, *models.Object, moduletools.ClassConfig) (T, models.AdditionalProperties, error)

func VectorizeBatch[T dto.Embedding](
	ctx context.Context,
	objs []*models.Object,
	skipObject []bool,
	cfg moduletools.ClassConfig,
	logger logrus.FieldLogger,
	objectVectorizer objectVectorizer[T],
) ([]T, []models.AdditionalProperties, map[int]error) {
	vecs := make([]T, len(objs))
	// error should be the exception so dont preallocate
	errs := make(map[int]error, 0)
	errorLock := sync.Mutex{}

	// error group is used to limit concurrency
	eg := enterrors.NewErrorGroupWrapper(logger)
	eg.SetLimit(_NUMCPU * 2)
	for i := range objs {
		i := i

		if skipObject[i] {
			continue
		}
		eg.Go(func() error {
			vec, _, err := objectVectorizer(ctx, objs[i], cfg)
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

type batchObjectsVectorizer[T dto.Embedding] func(context.Context, []*models.Object, moduletools.ClassConfig) ([]T, models.AdditionalProperties, error)

type batchOfObjects struct {
	indexes []int
	objs    []*models.Object
}

func VectorizeBatchObjects[T dto.Embedding](
	ctx context.Context,
	objs []*models.Object,
	skipObject []bool,
	cfg moduletools.ClassConfig,
	logger logrus.FieldLogger,
	batchObjectVectorizer batchObjectsVectorizer[T],
	batchSize int,
) ([]T, []models.AdditionalProperties, map[int]error) {
	vecs := make([]T, len(objs))
	// error should be the exception so dont preallocate
	errs := make(map[int]error, 0)
	errorLock := sync.Mutex{}
	// create batches
	batchOfObjects := make([]batchOfObjects, int(len(objs)/batchSize)+1)
	j := 0
	for i := range objs {
		if skipObject[i] {
			continue
		}
		batchOfObjects[j].indexes = append(batchOfObjects[j].indexes, i)
		batchOfObjects[j].objs = append(batchOfObjects[j].objs, objs[i])
		if (i+1)%batchSize == 0 {
			j++
		}
	}
	// error group is used to limit concurrency
	eg := enterrors.NewErrorGroupWrapper(logger)
	eg.SetLimit(_NUMCPU * 2)
	for i := range batchOfObjects {
		eg.Go(func() error {
			res, _, err := batchObjectVectorizer(ctx, batchOfObjects[i].objs, cfg)
			if err != nil {
				errorLock.Lock()
				defer errorLock.Unlock()
				for _, index := range batchOfObjects[i].indexes {
					errs[index] = err
				}
			} else {
				for i, index := range batchOfObjects[i].indexes {
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

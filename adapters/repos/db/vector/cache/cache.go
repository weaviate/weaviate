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

package cache

import (
	"context"
	"time"
)

const DefaultDeletionInterval = 3 * time.Second

type CacheMultiple[T any] interface {
	GetMultiple(ctx context.Context, docID uint64, relativeID uint64) ([]T, error)
	MultiGetMultiple(ctx context.Context, docIDs []uint64, relativeID []uint64) ([][]float32, []error)
	PreloadMultiple(docID uint64, relativeID uint64, vec []float32)
	PrefetchMultiple(docID uint64, relativeID uint64)
}

type Cache[T any] interface {
	CacheMultiple[T]
	Get(ctx context.Context, id uint64) ([]T, error)
	MultiGet(ctx context.Context, ids []uint64) ([][]T, []error)
	Len() int32
	CountVectors() int64
	Delete(ctx context.Context, id uint64)
	Preload(id uint64, vec []T)
	PreloadNoLock(id uint64, vec []T)
	SetSizeAndGrowNoLock(id uint64)
	Prefetch(id uint64)
	Grow(size uint64)
	Drop()
	UpdateMaxSize(size int64)
	CopyMaxSize() int64
	All() [][]T
	LockAll()
	UnlockAll()
}

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

type Cache[T any] interface {
	Get(ctx context.Context, id uint64) ([]T, error)
	MultiGet(ctx context.Context, ids []uint64) ([][]T, []error)
	Len() int32
	CountVectors() int64
	Delete(ctx context.Context, id uint64)
	Preload(id uint64, vec []T)
	Prefetch(id uint64)
	Grow(size uint64)
	Drop()
	UpdateMaxSize(size int64)
	CopyMaxSize() int64
	All() [][]T
}

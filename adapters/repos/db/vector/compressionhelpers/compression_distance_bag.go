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

package compressionhelpers

import (
	"context"
	"fmt"
)

type CompressionDistanceBag interface {
	Load(ctx context.Context, id uint64) error
	Distance(x, y uint64) (float32, error)
}

type quantizedDistanceBag[T byte | uint64] struct {
	elements   map[uint64][]T
	compressor *quantizedVectorsCompressor[T]
}

func (bag *quantizedDistanceBag[T]) Load(ctx context.Context, id uint64) error {
	var err error
	bag.elements[id], err = bag.compressor.cache.Get(ctx, id)
	return err
}

func (bag *quantizedDistanceBag[T]) Distance(x, y uint64) (float32, error) {
	v1, found := bag.elements[x]
	if !found {
		return 0, fmt.Errorf("missing id in bag: %d", x)
	}
	v2, found := bag.elements[y]
	if !found {
		return 0, fmt.Errorf("missing id in bag: %d", y)
	}
	return bag.compressor.DistanceBetweenCompressedVectors(v1, v2)
}

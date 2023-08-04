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

package hnsw

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
)

func dummyCompressedVectorForID(context.Context, uint64) ([]byte, error) {
	return nil, nil
}

func TestCompressedVectorCacheGrowth(t *testing.T) {
	logger, _ := test.NewNullLogger()
	vectorCache := newCompressedShardedLockCache(dummyCompressedVectorForID, 1000000, logger)
	id := 100000
	assert.True(t, len(vectorCache.cache) < id)
	vectorCache.grow(uint64(id))
	assert.True(t, len(vectorCache.cache) > id)
	last := vectorCache.count
	vectorCache.grow(uint64(id))
	assert.True(t, len(vectorCache.cache) == int(last))
}

func TestCompressedVectorCacheCacheMiss(t *testing.T) {
	ctx := context.Background()

	logger, _ := test.NewNullLogger()
	called := 0
	m := map[uint64][]byte{
		10: {0, 1, 2, 3},
		11: {4, 5, 6, 7},
	}

	vectorCache := newCompressedShardedLockCache(func(ctx context.Context, id uint64) ([]byte, error) {
		called++
		return m[id], nil
	}, 1000000, logger)

	got, err := vectorCache.get(ctx, 10)
	assert.Nil(t, err)
	assert.Equal(t, m[10], got)

	got, err = vectorCache.get(ctx, 11)
	assert.Nil(t, err)
	assert.Equal(t, m[11], got)
}

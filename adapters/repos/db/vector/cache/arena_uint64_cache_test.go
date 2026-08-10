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

package cache

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"
	"unsafe"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/storobj"
)

// testRecordWords is the uncentered 1536d rq1 code: 1 metadata word + 24
// bit words.
const testRecordWords = 25

func arenaTestWordsFor(id uint64, words int) []uint64 {
	code := make([]uint64, words)
	for i := range code {
		code[i] = id*0x9E3779B97F4A7C15 + uint64(i)
	}
	return code
}

func arenaTestU64ForID(limit uint64, words int) func(ctx context.Context, id uint64) ([]uint64, error) {
	return func(_ context.Context, id uint64) ([]uint64, error) {
		if id >= limit || id%7 == 3 {
			return nil, storobj.NewErrNotFoundf(id, "not backed")
		}
		return arenaTestWordsFor(id, words), nil
	}
}

// TestArenaUint64CacheParity mirrors the byte-cache parity gate for the
// word-based adapter against shardedLockCache[uint64] as the oracle.
func TestArenaUint64CacheParity(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	arena, err := NewArenaUint64Cache(arenaTestU64ForID(5000, testRecordWords), testRecordWords,
		1e12, 1, logger, 0, nil)
	require.NoError(t, err)
	t.Cleanup(arena.Drop)
	oracle := NewShardedUInt64LockCache(arenaTestU64ForID(5000, testRecordWords), 1e12, 1, logger, 0, nil)
	t.Cleanup(oracle.Drop)

	rng := rand.New(rand.NewPCG(0xa1e4b, 0xca11f))
	randomID := func() uint64 {
		if rng.IntN(50) == 0 {
			return uint64(rng.IntN(60000))
		}
		return uint64(rng.IntN(10000))
	}

	for step := 0; step < 10000; step++ {
		id := randomID()
		op := rng.IntN(100)
		desc := fmt.Sprintf("step %d op %d id %d", step, op, id)

		switch {
		case op < 40:
			gotVec, gotErr := arena.Get(ctx, id)
			wantVec, wantErr := oracle.Get(ctx, id)
			if wantErr != nil {
				assert.Error(t, gotErr, desc)
			} else {
				require.NoError(t, gotErr, desc)
				assert.Equal(t, wantVec, gotVec, desc)
			}
		case op < 65:
			code := arenaTestWordsFor(id, testRecordWords)
			arena.Preload(id, code)
			oracle.Preload(id, code)
		case op < 75:
			arena.Delete(ctx, id)
			oracle.Delete(ctx, id)
		case op < 80:
			arena.Grow(id)
			oracle.Grow(id)
		default:
			assert.Equal(t, oracle.PrefetchGet(id), arena.PrefetchGet(id), desc)
		}

		assert.Equal(t, oracle.Len(), arena.Len(), desc)
		assert.Equal(t, oracle.CountVectors(), arena.CountVectors(), desc)
	}
}

// TestArenaUint64CacheAlignmentAndViews pins that word views are 64-byte
// aligned (records are) and round-trip the exact stored words.
func TestArenaUint64CacheAlignmentAndViews(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	c, err := NewArenaUint64Cache(arenaTestU64ForID(0, testRecordWords), testRecordWords,
		1e12, 1, logger, 0, nil)
	require.NoError(t, err)
	defer c.Drop()

	ids := []uint64{0, 1, 4095, 4096, 12288, 20000}
	for _, id := range ids {
		c.Preload(id, arenaTestWordsFor(id, testRecordWords))
	}
	for _, id := range ids {
		vec, err := c.Get(ctx, id)
		require.NoError(t, err)
		require.Equal(t, testRecordWords, len(vec))
		assert.Equal(t, arenaTestWordsFor(id, testRecordWords), vec)
		addr := uintptr(unsafe.Pointer(&vec[0]))
		assert.Zerof(t, addr%64, "record %d not 64-byte aligned (addr %x)", id, addr)
	}
}

// TestVectorCacheImplToggle pins the env contract: default sharded, arena
// on request, unknown values fall back to sharded.
func TestVectorCacheImplToggle(t *testing.T) {
	logger, _ := test.NewNullLogger()
	t.Setenv(VectorCacheImplEnv, "")
	assert.False(t, ArenaCacheSelected(logger))
	t.Setenv(VectorCacheImplEnv, "sharded")
	assert.False(t, ArenaCacheSelected(logger))
	t.Setenv(VectorCacheImplEnv, "arena")
	assert.True(t, ArenaCacheSelected(logger))
	t.Setenv(VectorCacheImplEnv, "bogus")
	assert.False(t, ArenaCacheSelected(logger))
}

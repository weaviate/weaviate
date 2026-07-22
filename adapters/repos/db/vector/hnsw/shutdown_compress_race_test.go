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

package hnsw

import (
	"context"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storobj"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

func Test_ShutdownWaitsForInFlightCompress(t *testing.T) {
	dist := distancer.NewL2SquaredProvider()
	logger, _ := test.NewNullLogger()
	ctx := context.Background()

	uc := ent.UserConfig{}
	uc.SetDefaults()
	uc.VectorCacheMaxObjects = 1e12

	store := testinghelpers.NewDummyStore(t)
	defer store.Shutdown(context.Background())

	index, err := New(Config{
		RootPath:              t.TempDir(),
		ID:                    "shutdown-compress-race",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      dist,
		AllocChecker:          memwatch.NewDummyMonitor(),
		MakeBucketOptions:     lsmkv.MakeNoopBucketOptions,
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return nil, storobj.NewErrNotFoundf(id, "out of range")
		},
		GetViewThunk: func() common.BucketView {
			return &noopBucketView{}
		},
		TempVectorForIDWithViewThunk: func(ctx context.Context, id uint64, container *common.VectorSlice, view common.BucketView) ([]float32, error) {
			return nil, storobj.NewErrNotFoundf(id, "out of range")
		},
	}, uc, cyclemanager.NewCallbackGroupNoop(), store)
	require.NoError(t, err)

	// Stand in for an in-flight compress(): it holds compressActionLock across
	// the whole Preload phase.
	index.compressActionLock.Lock()

	done := make(chan error, 1)
	enterrors.GoWrapper(func() { done <- index.Shutdown(ctx) }, logger)

	// Shutdown must not complete (and therefore not drop the cache) while the
	// lock is held.
	select {
	case <-done:
		t.Fatal("Shutdown completed while compression still held compressActionLock; the cache drop would race the Preload readers")
	case <-time.After(200 * time.Millisecond):
		// expected: Shutdown is blocked on compressActionLock
	}

	// Once the in-flight compress finishes, Shutdown proceeds cleanly.
	index.compressActionLock.Unlock()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown did not complete after compressActionLock was released")
	}
}

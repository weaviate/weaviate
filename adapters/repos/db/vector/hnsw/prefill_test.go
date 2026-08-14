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
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storobj"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

func TestShutdownDrainsCachePrefill(t *testing.T) {
	const indexID = "prefill_drain_test"

	ctx := context.Background()
	tempDir := t.TempDir()
	logger, _ := test.NewNullLogger()
	dummyStore := testinghelpers.NewDummyStoreFromFolder(tempDir, t)
	vectors, _ := testinghelpers.RandomVecs(5, 0, 8)

	uc := ent.UserConfig{}
	uc.SetDefaults()

	var (
		armed    atomic.Bool
		entered  = make(chan struct{}, 1)
		released = make(chan struct{})
	)
	cfg := Config{
		RootPath:            tempDir,
		ID:                  indexID,
		DistanceProvider:    distancer.NewL2SquaredProvider(),
		WaitForCachePrefill: false,
		AllocChecker:        memwatch.NewDummyMonitor(),
		MakeBucketOptions:   lsmkv.MakeNoopBucketOptions,
		MakeCommitLoggerThunk: func(opts ...CommitlogOption) (CommitLogger, error) {
			return NewCommitLogger(tempDir, indexID, logger, cyclemanager.NewCallbackGroupNoop(), opts...)
		},
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			if armed.Load() {
				select {
				case entered <- struct{}{}:
				default:
				}
				<-released
			}
			if int(id) >= len(vectors) {
				return nil, storobj.NewErrNotFoundf(id, "out of range")
			}
			return vectors[int(id)], nil
		},
		GetViewThunk: func() common.BucketView { return &noopBucketView{} },
		TempVectorForIDWithViewThunk: func(ctx context.Context, id uint64, container *common.VectorSlice, view common.BucketView) ([]float32, error) {
			copy(container.Slice, vectors[int(id)])
			return container.Slice, nil
		},
	}

	// commit-log state for the restart below to prefill
	index, err := New(cfg, uc, cyclemanager.NewCallbackGroupNoop(), dummyStore)
	require.Nil(t, err)
	for id, vec := range vectors {
		require.Nil(t, index.Add(ctx, uint64(id), vec))
	}
	require.Nil(t, index.Flush())
	require.Nil(t, index.Shutdown(ctx))
	dummyStore.FlushMemtables(ctx)

	index, err = New(cfg, uc, cyclemanager.NewCallbackGroupNoop(), dummyStore)
	require.Nil(t, err)

	// New() replays the commit log through the same thunk, so arm only once the
	// index is built or the restart itself parks
	armed.Store(true)
	index.PostStartup(ctx)

	select {
	case <-entered:
	case <-time.After(30 * time.Second):
		t.Fatal("prefill never reached VectorForID")
	}

	done := make(chan error, 1)
	go func() { done <- index.Shutdown(ctx) }()

	select {
	case err := <-done:
		t.Fatalf("Shutdown returned while the prefill was still reading: %v", err)
	case <-time.After(300 * time.Millisecond):
	}

	close(released)

	select {
	case err := <-done:
		require.Nil(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("Shutdown did not return after the prefill finished")
	}
}

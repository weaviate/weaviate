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
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// validatePQSegments reads h.pqConfig from the insert-validation path
// (ValidateBeforeInsert / ValidateMultiBeforeInsert) while UpdateUserConfig
// writes it under compressActionLock; the read must take the same lock.
func TestValidateBeforeInsert_RacesUpdateUserConfig(t *testing.T) {
	tests := []struct {
		name        string
		multivector bool
		validate    func(h *hnsw) error
	}{
		{
			name:        "single-vector ValidateBeforeInsert",
			multivector: false,
			validate:    func(h *hnsw) error { return h.ValidateBeforeInsert(testVectors[0]) },
		},
		{
			name:        "multi-vector ValidateMultiBeforeInsert",
			multivector: true,
			validate:    func(h *hnsw) error { return h.ValidateMultiBeforeInsert(testMultiVectors[0]) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			index, base := buildConfigRaceIndex(t, tt.multivector)
			// Make UpdateUserConfig write the compression config and return early
			// instead of kicking off a full rebuild.
			index.cachePrefilled.Store(false)

			done := make(chan struct{})

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 5000; i++ {
					updated := base
					updated.PQ.Enabled = true
					updated.PQ.Segments = 1 + (i % 2) // vary -> real struct write each time
					_ = index.UpdateUserConfig(updated, func() {})
				}
				close(done)
			}()

			timeout := time.After(60 * time.Second)
			for {
				select {
				case <-done:
					wg.Wait()
					return
				case <-timeout:
					t.Fatal("UpdateUserConfig writer did not finish; possible hang")
				default:
					// reader side: validate reads pqConfig under compressActionLock
					_ = tt.validate(index)
					runtime.Gosched()
				}
			}
		})
	}
}

func buildConfigRaceIndex(t *testing.T, multivector bool) (*hnsw, ent.UserConfig) {
	t.Helper()
	store := testinghelpers.NewDummyStore(t)
	t.Cleanup(func() { store.Shutdown(context.Background()) })

	uc := ent.UserConfig{
		VectorCacheMaxObjects: 1e12,
		MaxConnections:        8,
		EFConstruction:        64,
		EF:                    64,
	}
	cfg := Config{
		RootPath:              t.TempDir(),
		ID:                    "config-race",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewL2SquaredProvider(),
		AllocChecker:          memwatch.NewDummyMonitor(),
		MakeBucketOptions:     lsmkv.MakeNoopBucketOptions,
		GetViewThunk:          func() common.BucketView { return &noopBucketView{} },
	}
	if multivector {
		uc.Multivector = ent.MultivectorConfig{Enabled: true}
		cfg.DistanceProvider = distancer.NewDotProductProvider()
		cfg.VectorForIDThunk = func(ctx context.Context, id uint64) ([]float32, error) {
			return []float32{0}, errors.New("can not use VectorForIDThunk with multivector")
		}
		cfg.MultiVectorForIDThunk = testMultiVectorForID
	} else {
		cfg.VectorForIDThunk = testVectorForID
	}

	index, err := New(cfg, uc, cyclemanager.NewCallbackGroupNoop(), store)
	require.NoError(t, err)
	t.Cleanup(func() { index.Shutdown(context.Background()) })

	// Seed one element so h.dims is set and the non-empty validate path runs.
	if multivector {
		require.NoError(t, index.AddMulti(context.Background(), 0, testMultiVectors[0]))
	} else {
		require.NoError(t, index.Add(context.Background(), 0, testVectors[0]))
	}
	return index, uc
}

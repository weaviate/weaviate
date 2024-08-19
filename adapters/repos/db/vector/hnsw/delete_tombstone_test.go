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

//go:build tombstone
// +build tombstone

package hnsw

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

func startMonitoringForTests() (*monitoring.PrometheusMetrics, func()) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", 2112),
		Handler: mux,
	}

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("Failed to start monitoring server: %v", err)
		}
	}()

	stopServer := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := server.Shutdown(ctx); err != nil {
			log.Printf("Failed to shutdown monitoring server: %v", err)
		}
	}

	return monitoring.GetMetrics(), stopServer
}

func TestDelete_ConcurrentWithMetrics(t *testing.T) {
	var vectors [][]float32
	for i := 0; i < 100000; i++ {
		vectors = append(vectors, []float32{rand.Float32(), rand.Float32(), rand.Float32()})
	}
	var vectorIndex *hnsw
	store := testinghelpers.NewDummyStore(t)
	defer store.Shutdown(context.Background())

	metrics, shutdownFn := startMonitoringForTests()
	defer shutdownFn()

	t.Run("import the test vectors", func(t *testing.T) {
		index, err := New(Config{
			RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
			ID:                    "delete-test",
			MakeCommitLoggerThunk: MakeNoopCommitLogger,
			DistanceProvider:      distancer.NewCosineDistanceProvider(),
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				return vectors[int(id)], nil
			},
			TempVectorForIDThunk: TempVectorForIDThunk(vectors),
			PrometheusMetrics:    metrics,
			ClassName:            "Tombstone",
			ShardName:            uuid.New().String(),
		}, ent.UserConfig{
			MaxConnections:        30,
			EFConstruction:        128,
			VectorCacheMaxObjects: 200000,
		}, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), store)
		require.Nil(t, err)
		vectorIndex = index
		vectorIndex.logger = logrus.New()

		for i, vec := range vectors {
			err := vectorIndex.Add(uint64(i), vec)
			require.Nil(t, err)
		}
	})

	t.Run("concurrent entrypoint deletion and tombstone cleanup", func(t *testing.T) {
		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			err := vectorIndex.CleanUpTombstonedNodes(neverStop)
			require.Nil(t, err)
		}()

		go func() {
			defer wg.Done()
			for i := range vectors {
				err := vectorIndex.Delete(uint64(i))
				require.Nil(t, err)
			}
		}()

		wg.Wait()
	})

	t.Run("inserting more vectors (with largers ids)", func(t *testing.T) {
		for i, vec := range vectors {
			err := vectorIndex.Add(uint64(len(vectors)+i), vec)
			require.Nil(t, err)
		}
	})

	t.Run("concurrent entrypoint deletion and tombstone cleanup", func(t *testing.T) {
		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			err := vectorIndex.CleanUpTombstonedNodes(neverStop)
			require.Nil(t, err)
		}()

		go func() {
			defer wg.Done()
			for i := range vectors {
				err := vectorIndex.Delete(uint64(len(vectors) + i))
				require.Nil(t, err)
			}
		}()

		wg.Wait()
	})

	t.Run("final tombstone cleanup", func(t *testing.T) {
		err := vectorIndex.CleanUpTombstonedNodes(neverStop)
		require.Nil(t, err)
	})

	t.Run("verify the graph no longer has any tombstones", func(t *testing.T) {
		assert.Len(t, vectorIndex.tombstones, 0)
	})

	t.Run("destroy the index", func(t *testing.T) {
		time.Sleep(30 * time.Second) // wait to capture final metrics
		require.Nil(t, vectorIndex.Drop(context.Background()))
	})
}

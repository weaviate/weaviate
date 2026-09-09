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

//go:build integrationTest

package db

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/queryadmission"
)

// TestQueryAdmissionConcurrency bursts concurrent filtered+BM25 shard
// searches to verify clean shedding, bounded goroutine fan-out, and that the
// runtime kill switch disables shedding live.
func TestQueryAdmissionConcurrency(t *testing.T) {
	const (
		className   = "AdmissionClass"
		numObjects  = 20000
		concurrency = 200
		budget      = 32
		maxQueue    = 8
	)

	ctx := context.Background()
	disabled := configRuntime.NewDynamicValue(false)
	repo, shard := setupAdmissionRepo(t, className, budget, maxQueue, disabled)
	defer repo.Shutdown(ctx)

	importAdmissionObjects(t, repo, className, numObjects)

	// filter+kwr hit the admitted (filter+BM25) path; category is evenly spread
	// over 10 values (~10% match), body always contains "alpha".
	filter := &filters.LocalFilter{
		Root: &filters.Clause{
			Operator: filters.OperatorEqual,
			On: &filters.Path{
				Class:    schema.ClassName(className),
				Property: "category",
			},
			Value: &filters.Value{Value: 0, Type: schema.DataTypeInt},
		},
	}
	kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"body"}, Query: "alpha"}
	props := []string{"body", "category"}

	// burst fires `concurrency` searches at once and reports shed count and
	// peak goroutine delta above the pre-burst baseline.
	burst := func(t *testing.T) (shed int, peakDelta int) {
		baseline := runtime.NumGoroutine()
		var peak atomic.Int64
		peak.Store(int64(baseline))
		stop := make(chan struct{})
		go func() {
			ticker := time.NewTicker(200 * time.Microsecond)
			defer ticker.Stop()
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
					if g := int64(runtime.NumGoroutine()); g > peak.Load() {
						peak.Store(g)
					}
				}
			}
		}()

		var (
			wg        sync.WaitGroup
			shedCount atomic.Int64
			badErr    atomic.Value
		)
		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
				defer cancel()
				_, _, err := shard.ObjectSearch(cctx, 100, filter, kwr, nil, nil, additional.Properties{}, props)
				switch {
				case err == nil:
				case errors.Is(err, queryadmission.ErrOverloaded):
					shedCount.Add(1)
				case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
				default:
					badErr.Store(err.Error())
				}
			}()
		}
		wg.Wait()
		close(stop)

		if v := badErr.Load(); v != nil {
			t.Fatalf("unexpected search error (not nil/overloaded/deadline): %v", v)
		}
		return int(shedCount.Load()), int(peak.Load()) - baseline
	}

	// Only ~budget+queue can be in flight, so the limiter sheds most of the burst.
	shedEnabled, peakEnabled := burst(t)
	t.Logf("enabled:  shed=%d/%d peakGoroutineDelta=%d gomaxprocs=%d",
		shedEnabled, concurrency, peakEnabled, runtime.GOMAXPROCS(0))
	require.Positive(t, shedEnabled, "expected shedding at %d concurrent vs budget %d / queue %d",
		concurrency, budget, maxQueue)

	// The kill switch makes Admit a passthrough on an already-built limiter.
	require.NoError(t, disabled.SetValue(true))
	shedDisabled, peakDisabled := burst(t)
	require.NoError(t, disabled.SetValue(false))
	t.Logf("disabled: shed=%d/%d peakGoroutineDelta=%d",
		shedDisabled, concurrency, peakDisabled)
	require.Zero(t, shedDisabled, "disabled admission must never shed")

	// Admission caps concurrent queries, bounding aggregate fan-out below the
	// unbounded baseline; only observable with real parallelism (GOMAXPROCS>=2).
	if runtime.GOMAXPROCS(0) >= 2 {
		require.Less(t, peakEnabled, peakDisabled,
			"admission should bound goroutine fan-out below the unbounded baseline")
	}
}

func setupAdmissionRepo(t *testing.T, className string, budget, maxQueue int,
	disabled *configRuntime.DynamicValue[bool],
) (*DB, ShardLike) {
	t.Helper()
	vFalse, vTrue := false, true
	class := &models.Class{
		Class:               className,
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: BM25FinvertedConfig(1.2, 0.75, "none"),
		Properties: []*models.Property{
			{
				Name:            "body",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationWord,
				IndexFilterable: &vFalse,
				IndexSearchable: &vTrue,
			},
			{
				Name:            "category",
				DataType:        schema.DataTypeInt.PropString(),
				IndexFilterable: &vTrue,
				IndexSearchable: &vFalse,
			},
		},
	}
	return newAdmissionRepo(t, admissionRepoParams{
		budget:   budget,
		maxQueue: maxQueue,
		disabled: disabled,
	}, className, class)
}

func importAdmissionObjects(t *testing.T, repo *DB, className string, count int) {
	t.Helper()
	const chunk = 2000
	for start := 0; start < count; start += chunk {
		end := min(start+chunk, count)
		batch := make(objects.BatchObjects, 0, end-start)
		for i := start; i < end; i++ {
			id := strfmt.UUID(fmt.Sprintf("%08x-0000-0000-0000-%012d", i, i))
			batch = append(batch, objects.BatchObject{
				OriginalIndex: i - start,
				UUID:          id,
				Object: &models.Object{
					Class: className,
					ID:    id,
					Properties: map[string]interface{}{
						"body":     fmt.Sprintf("alpha beta gamma object number %d", i),
						"category": int64(i % 10),
					},
					Vector: []float32{0.1, 0.2, 0.3},
				},
			})
		}
		_, err := repo.BatchPutObjects(context.Background(), batch, nil, 0)
		require.NoError(t, err)
	}
}

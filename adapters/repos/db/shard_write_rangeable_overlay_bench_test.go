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

package db

import (
	"fmt"
	"testing"

	"github.com/google/uuid"

	"github.com/weaviate/weaviate/entities/models"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// BenchmarkRangeableForceIndexOverlay_SteadyState reproduces QA's
// re-verify measurement on PR #12206 (5 props, steady state, no
// active migration): rangeableForceIndexOverlay running on every
// AnalyzeObject call for a shard that has never had a rangeable
// migration touch it (the common case for the vast majority of
// collections, which never use enable-rangeable/repair-rangeable).
//
// QA's numbers on the pre-fast-exit shape: 85ns/80B/1 alloc per op
// pre-#12206, 568ns/360B/16 allocs post; +15 allocs/op unconditional
// steady-state regression. The fast exit in this benchmark's target
// function should restore per-op cost close to the pre-#12206
// baseline for this shard shape: it returns nil before ever calling
// IsRangeableLocallyReady, so none of the per-prop bucket-name-build +
// store.Bucket lookups run.
func BenchmarkRangeableForceIndexOverlay_SteadyState(b *testing.B) {
	ctx := testCtx()
	className := "RangeableOverlayBench_" + uuid.NewString()[:8]

	falseVal := false
	class := &models.Class{
		Class:             className,
		VectorIndexConfig: enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 60,
			Stopwords:              &models.StopwordConfig{Preset: "none"},
		},
	}
	props := make([]*models.Property, 5)
	for i := range props {
		props[i] = &models.Property{
			Name:              propNameForBenchIndex(i),
			DataType:          []string{"int"},
			IndexRangeFilters: &falseVal,
		}
	}
	class.Properties = props

	shdIface, _ := testShardWithSettings(b, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shdIface.(*Shard)
	defer shard.Shutdown(ctx)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = shard.rangeableForceIndexOverlay(props)
	}
}

// BenchmarkRangeableForceIndexOverlay_MidMigration is the companion
// measurement for the OTHER steady state: a shard mid-migration
// (locally ready, cluster flag not yet flipped), so the fast exit's
// cost isn't accidentally read as "the overlay got cheaper for
// everyone": this state must still pay the per-prop
// IsRangeableLocallyReady check, by design (the write-loss fix this
// overlay exists for only works because it keeps checking).
func BenchmarkRangeableForceIndexOverlay_MidMigration(b *testing.B) {
	ctx := testCtx()
	className := "RangeableOverlayBenchMid_" + uuid.NewString()[:8]
	const propName = "score"
	class := newFilterableToRangeableTestClass(className)

	shdIface, _ := testShardWithSettings(b, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shdIface.(*Shard)
	defer shard.Shutdown(ctx)

	shard.setRangeableLocallyReady(propName, true)
	falseVal := false
	props := []*models.Property{{Name: propName, DataType: []string{"int"}, IndexRangeFilters: &falseVal}}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = shard.rangeableForceIndexOverlay(props)
	}
}

func propNameForBenchIndex(i int) string {
	return fmt.Sprintf("num_prop_%d", i)
}

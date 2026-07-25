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

package roaringset

import (
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/dustin/go-humanize"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// shippedMaxMemory mirrors config.DefaultQueryBitmapBufsMaxMemory, duplicated
// here because a storage package must not import usecases/config.
const shippedMaxMemory = 1 << 27

// mergeShape describes what the merge following a pooled clone does to that
// clone. The range cascade only ever shrinks it; a roaringset layer merge can
// grow it.
type mergeShape string

const (
	shapeShrink mergeShape = "shrink"
	shapeGrow   mergeShape = "grow"
)

func benchPool(tb testing.TB, metrics *monitoring.PrometheusMetrics, maxMemo int) BitmapBufPool {
	tb.Helper()
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	pool, stop := NewBitmapBufPoolDefault(logger, metrics, 1<<25, maxMemo)
	tb.Cleanup(stop)
	return pool
}

// denseBitmap returns a bitmap of at least targetBytes built from full
// containers, matching the shape of a whole-shard allow list.
func denseBitmap(targetBytes int) *sroar.Bitmap {
	const idsPerContainer = 1 << 16
	ids := uint64(idsPerContainer)
	bm := sroar.Prefill(ids)
	for bm.LenInBytes() < targetBytes {
		ids += idsPerContainer
		bm = sroar.Prefill(ids)
	}
	return bm
}

// disjointBitmap returns a bitmap whose containers sit above src, so an Or
// against it has to add keys and containers -- the shape of merging a newer
// segment's additions into an older segment's.
func disjointBitmap(src *sroar.Bitmap, containers int) *sroar.Bitmap {
	base := src.Maximum() + 1<<16
	bm := sroar.NewBitmap()
	for c := 0; c < containers; c++ {
		start := base + uint64(c)<<16
		for i := uint64(0); i < 1<<16; i += 2 {
			bm.Set(start + i)
		}
	}
	return bm
}

// BenchmarkBitmapBufPoolClone measures a pooled clone plus the merge that
// follows it, holding the buffer for the whole merge the way a query does.
// Run with -cpu 1,4,8 to sweep merge worker counts, on this branch and on the
// base branch, to see what the clone growth headroom changes. %miss is the same
// quantity inmemo_created / (inmemo_created + inmemo_got) reports in
// production.
func BenchmarkBitmapBufPoolClone(b *testing.B) {
	const MiB = 1 << 20
	metrics := monitoring.GetMetrics()

	sizes := []struct {
		name  string
		bytes int
	}{
		// ~2.9 MiB whole-shard allow list. Lands mid-class, so the 4 MiB
		// class it is served from already carries 1.2 MiB of headroom.
		{"midClass", 2_900_000},
		// Just under the 4 MiB class ceiling: the pooled buffer has almost
		// no room left for extra containers.
		{"classCeiling", 4*MiB - 48*1024},
	}
	for _, size := range sizes {
		src := denseBitmap(size.bytes)
		// 64 containers exceeds the residual headroom of either size, so the
		// shipped arm has to expand for real.
		grower := disjointBitmap(src, 64)
		shrinker := src.Clone()
		shrinker.RemoveRange(0, 1<<16)

		for _, shape := range []mergeShape{shapeShrink, shapeGrow} {
			name := fmt.Sprintf("%s_%s/%s", size.name,
				humanize.IBytes(uint64(src.LenInBytes())), shape)
			b.Run(name, func(b *testing.B) {
				pool := benchPool(b, metrics, shippedMaxMemory)
				before := counterSnapshot(metrics)

				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						cloned, put := pool.CloneToBuf(src)
						switch shape {
						case shapeGrow:
							cloned.Or(grower)
						case shapeShrink:
							cloned.And(shrinker)
						}
						put()
					}
				})
				b.StopTimer()

				if created, got := counterDelta(metrics, before); created+got > 0 {
					b.ReportMetric(created/(created+got)*100, "%miss")
				}
			})
		}
	}
}

// counterSnapshot records inmemo_created / inmemo_got for every size class the
// ladder can produce, so a delta isolates one sub-benchmark.
func counterSnapshot(metrics *monitoring.PrometheusMetrics) map[string]float64 {
	snapshot := map[string]float64{}
	for p2 := 9; p2 <= 40; p2++ {
		size := humanize.IBytes(uint64(1) << p2)
		for _, op := range []string{"inmemo_created", "inmemo_got"} {
			snapshot[size+"/"+op] = testutil.ToFloat64(
				metrics.LSMBitmapBuffersUsage.WithLabelValues(size, op))
		}
	}
	return snapshot
}

func counterDelta(metrics *monitoring.PrometheusMetrics, before map[string]float64) (created, got float64) {
	const createdSuffix = "inmemo_created"
	for key, v := range counterSnapshot(metrics) {
		delta := v - before[key]
		if delta == 0 {
			continue
		}
		if strings.HasSuffix(key, createdSuffix) {
			created += delta
		} else {
			got += delta
		}
	}
	return created, got
}

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

package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hfresh"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storobj"
	enthfresh "github.com/weaviate/weaviate/entities/vectorindex/hfresh"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type baselineNoopBucketView struct{}

func (baselineNoopBucketView) ReleaseView() {}

func heapInUse() uint64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapInuse
}

func newScratchStore(dir string) (*lsmkv.Store, error) {
	logger := logrus.New()
	logger.Out = os.Stderr
	logger.SetLevel(logrus.WarnLevel)
	return lsmkv.New(dir, dir, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
}

var baselineCSVHeader = strings.Join([]string{
	"timestamp", "dataset", "index", "config", "param", "queries", "k",
	"recall", "p50_ms", "p95_ms", "p99_ms", "qps",
	"build_s", "settle_s", "heap_total_mib", "vectors_mib", "index_mib", "disk_mib", "notes",
}, ",")

type baselinePoint struct {
	index    string
	config   string
	param    int
	recall   float64
	lat      latencyStats
	qps      float64
	buildS   float64
	settleS  float64
	heapMiB  float64
	vecMiB   float64
	idxMiB   float64
	diskMiB  float64
	notes    string
	nQueries int
	k        int
}

func appendBaselineCSV(path, dataset string, p baselinePoint) error {
	newFile := false
	if _, err := os.Stat(path); os.IsNotExist(err) {
		newFile = true
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if newFile {
		fmt.Fprintln(f, baselineCSVHeader)
	}
	_, err = fmt.Fprintf(f, "%s,%s,%s,%q,%d,%d,%d,%.4f,%.3f,%.3f,%.3f,%.2f,%.1f,%.1f,%.1f,%.1f,%.1f,%.1f,%q\n",
		time.Now().Format(time.RFC3339), dataset, p.index, p.config, p.param, p.nQueries, p.k,
		p.recall, ms(p.lat.p50), ms(p.lat.p95), ms(p.lat.p99), p.qps,
		p.buildS, p.settleS, p.heapMiB, p.vecMiB, p.idxMiB, p.diskMiB, p.notes)
	return err
}

// measureBaseline runs the single-threaded query loop against an index.
func measureBaseline(search func(q []float32, k int) ([]uint64, error), queries []float32, dims, numQueries, k int, gt []int32, gtCols int) (float64, latencyStats, float64, error) {
	lat := make([]time.Duration, numQueries)
	gtBuf := make([]uint64, k)
	var hits, wanted uint64
	loopStart := time.Now()
	for qi := 0; qi < numQueries; qi++ {
		q := queries[qi*dims : (qi+1)*dims]
		t := time.Now()
		ids, err := search(q, k)
		lat[qi] = time.Since(t)
		if err != nil {
			return 0, latencyStats{}, 0, err
		}
		for i, id := range gt[qi*gtCols : qi*gtCols+k] {
			gtBuf[i] = uint64(id)
		}
		hits += testinghelpers.MatchesInLists(gtBuf, ids)
		wanted += uint64(k)
	}
	wall := time.Since(loopStart)
	return float64(hits) / float64(wanted), percentiles(lat), float64(numQueries) / wall.Seconds(), nil
}

func dirSizeMiB(path string) float64 {
	out, err := exec.Command("du", "-sk", path).Output()
	if err != nil {
		return 0
	}
	var kb float64
	fmt.Sscanf(string(out), "%f", &kb)
	return kb / 1024
}

// runHNSWBaseline builds an uncompressed HNSW index over the base vectors
// (parallel build — stated in output; queries are single-threaded) and
// sweeps ef.
func runHNSWBaseline(base []float32, dims, n int, queries []float32, gt []int32, gtCols, numQueries, k int, sweep []int, csvPath, dataset string) error {
	ctx := context.Background()
	heap0 := heapInUse()

	scratch, err := os.MkdirTemp("", "bitpack-hnsw-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(scratch)
	store, err := newScratchStore(scratch)
	if err != nil {
		return err
	}

	uc := enthnsw.NewDefaultUserConfig()
	uc.VectorCacheMaxObjects = 1e13
	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)

	vecForID := func(ctx context.Context, id uint64) ([]float32, error) {
		if int(id) >= n {
			return nil, storobj.NewErrNotFoundf(id, "out of range")
		}
		return base[int(id)*dims : (int(id)+1)*dims], nil
	}
	index, err := hnsw.New(hnsw.Config{
		AllocChecker:          memwatch.NewDummyMonitor(),
		RootPath:              scratch,
		ID:                    "baseline",
		MakeCommitLoggerThunk: hnsw.MakeNoopCommitLogger,
		ClassName:             "Baseline",
		ShardName:             "shard",
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:      vecForID,
		GetViewThunk:          func() common.BucketView { return baselineNoopBucketView{} },
		TempVectorForIDWithViewThunk: func(ctx context.Context, id uint64, container *common.VectorSlice, view common.BucketView) ([]float32, error) {
			copy(container.Slice, base[int(id)*dims:(int(id)+1)*dims])
			return container.Slice, nil
		},
		MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
	}, uc, cyclemanager.NewCallbackGroupNoop(), store)
	if err != nil {
		return err
	}
	defer index.Shutdown(ctx)

	config := fmt.Sprintf("uncompressed maxConn=%d efC=%d cache=all", uc.MaxConnections, uc.EFConstruction)
	fmt.Fprintf(os.Stderr, "building HNSW (%s, parallel build) ...\n", config)
	buildStart := time.Now()
	compressionhelpers.Concurrently(logger, uint64(n), func(id uint64) {
		if err := index.Add(ctx, id, base[int(id)*dims:(int(id)+1)*dims]); err != nil {
			panic(err)
		}
	})
	buildS := time.Since(buildStart).Seconds()
	fmt.Fprintf(os.Stderr, "built in %.1fs\n", buildS)

	heap1 := heapInUse()
	vecMiB := float64(n) * float64(dims) * 4 / (1 << 20)
	heapMiB := float64(heap1-heap0) / (1 << 20)
	idxMiB := heapMiB - vecMiB // graph estimate: total heap delta minus raw vector bytes

	for _, ef := range sweep {
		uc.EF = ef
		done := make(chan struct{})
		if err := index.UpdateUserConfig(uc, func() { close(done) }); err != nil {
			return err
		}
		<-done
		recall, lat, qps, err := measureBaseline(func(q []float32, k int) ([]uint64, error) {
			ids, _, err := index.SearchByVector(ctx, q, k, nil)
			return ids, err
		}, queries, dims, numQueries, k, gt, gtCols)
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stdout, "hnsw ef=%-5d recall@%d=%.4f p50=%.3fms p95=%.3fms p99=%.3fms qps=%.1f\n",
			ef, k, recall, ms(lat.p50), ms(lat.p95), ms(lat.p99), qps)
		if err := appendBaselineCSV(csvPath, dataset, baselinePoint{
			index: "hnsw", config: config, param: ef, recall: recall, lat: lat, qps: qps,
			buildS: buildS, heapMiB: heapMiB, vecMiB: vecMiB, idxMiB: idxMiB,
			notes: "parallel build; graph=heap-delta minus vectors", nQueries: numQueries, k: k,
		}); err != nil {
			return err
		}
	}
	return nil
}

// runHFreshBaseline builds an HFresh index (parallel ingest, async
// background tasks; settle time reported separately) and sweeps searchProbe.
func runHFreshBaseline(base []float32, dims, n int, queries []float32, gt []int32, gtCols, numQueries, k int, sweep []int, csvPath, dataset string) error {
	ctx := context.Background()
	heap0 := heapInUse()

	scratch, err := os.MkdirTemp("", "bitpack-hfresh-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(scratch)
	store, err := newScratchStore(scratch)
	if err != nil {
		return err
	}

	l := logrus.New()
	l.SetLevel(logrus.WarnLevel)
	dist := distancer.NewCosineDistanceProvider()

	cfg := hfresh.DefaultConfig()
	cfg.RootPath = scratch
	cfg.ID = "baseline"
	cfg.DistanceProvider = dist
	cfg.Logger = l
	cfg.TombstoneCallbacks = cyclemanager.NewCallbackGroupNoop()
	cfg.Centroids.HNSWConfig = &hnsw.Config{
		RootPath:              scratch,
		ID:                    "centroids",
		MakeCommitLoggerThunk: hnsw.MakeNoopCommitLogger,
		GetViewThunk:          func() common.BucketView { return baselineNoopBucketView{} },
		DistanceProvider:      dist,
		AllocChecker:          memwatch.NewDummyMonitor(),
		MakeBucketOptions:     lsmkv.MakeNoopBucketOptions,
	}
	scheduler := queue.NewScheduler(queue.SchedulerOptions{Logger: l})
	scheduler.Start()
	cfg.Scheduler = scheduler
	cfg.PrometheusMetrics = monitoring.GetMetrics()
	cfg.VectorForIDThunk = hnsw.NewVectorForIDThunk(cfg.TargetVector, func(ctx context.Context, id uint64, targetVector string) ([]float32, error) {
		if int(id) >= n {
			return nil, fmt.Errorf("vector not found for ID %d", id)
		}
		return base[int(id)*dims : (int(id)+1)*dims], nil
	})

	uc := enthfresh.NewDefaultUserConfig()
	index, err := hfresh.New(cfg, uc, store)
	if err != nil {
		return err
	}
	defer index.Shutdown(ctx)

	config := "defaults"
	fmt.Fprintf(os.Stderr, "building HFresh (%s, parallel ingest) ...\n", config)
	buildStart := time.Now()
	compressionhelpers.Concurrently(l, uint64(n), func(id uint64) {
		if err := index.Add(ctx, id, base[int(id)*dims:(int(id)+1)*dims]); err != nil {
			panic(err)
		}
	})
	buildS := time.Since(buildStart).Seconds()
	fmt.Fprintf(os.Stderr, "ingest done in %.1fs, waiting for background tasks ...\n", buildS)

	settleStart := time.Now()
	for i := 0; i < 3; i++ {
		if err := index.Flush(); err != nil {
			return err
		}
		if err := scheduler.WaitAll(ctx); err != nil {
			return err
		}
		time.Sleep(time.Second)
	}
	settleS := time.Since(settleStart).Seconds()
	fmt.Fprintf(os.Stderr, "settled in %.1fs\n", settleS)

	heap1 := heapInUse()
	heapMiB := float64(heap1-heap0) / (1 << 20)
	diskMiB := dirSizeMiB(scratch)

	for _, probe := range sweep {
		uc.SearchProbe = uint32(probe)
		done := make(chan struct{})
		if err := index.UpdateUserConfig(uc, func() { close(done) }); err != nil {
			return err
		}
		<-done
		recall, lat, qps, err := measureBaseline(func(q []float32, k int) ([]uint64, error) {
			ids, _, err := index.SearchByVector(ctx, q, k, nil)
			return ids, err
		}, queries, dims, numQueries, k, gt, gtCols)
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stdout, "hfresh probe=%-5d recall@%d=%.4f p50=%.3fms p95=%.3fms p99=%.3fms qps=%.1f\n",
			probe, k, recall, ms(lat.p50), ms(lat.p95), ms(lat.p99), qps)
		if err := appendBaselineCSV(csvPath, dataset, baselinePoint{
			index: "hfresh", config: config, param: probe, recall: recall, lat: lat, qps: qps,
			buildS: buildS, settleS: settleS, heapMiB: heapMiB, diskMiB: diskMiB,
			notes: "parallel ingest; async tasks settled before measurement", nQueries: numQueries, k: k,
		}); err != nil {
			return err
		}
	}
	return nil
}

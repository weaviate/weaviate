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
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// parseIntList parses a comma-separated list of non-negative ints (unlike
// parseBudgets, zero is allowed — it means "no rescore").
func parseIntList(s string) ([]int, error) {
	parts := strings.Split(s, ",")
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		v, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil || v < 0 {
			return nil, fmt.Errorf("invalid value %q", p)
		}
		out = append(out, v)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("empty list")
	}
	return out, nil
}

// rqCodeBytes returns the total stored bytes of one RQ code, metadata
// included, for the given bit depth and retained rotated dimensions.
func rqCodeBytes(bits, retained int) int {
	switch bits {
	case 8:
		return compressionhelpers.RQMetadataSize + retained
	case 4:
		return compressionhelpers.RQ4MetadataSize + retained/2
	case 1:
		// One metadata word, plus one more when centered; the caller passes
		// centered==true via rq1MetaWords. Handled by the caller because the
		// layout depends on centering.
		return 8 + retained/8
	}
	return 0
}

type rqHNSWParams struct {
	bits        int   // RQ bit depth
	retained    int   // retained rotated dims at build time (0 = full width)
	center      bool  // subtract the dataset mean before quantization
	efSweep     []int // ef values for the QPS-recall curve
	rescoreEF   int   // fixed ef for the rescore-window search
	swaps       []int // Task 3: query widths to swap to after building
	rescoreLims []int // rescore limits evaluated at every ef (0 = no rescore)
	targetRec   float64
}

// runRQHNSW builds an HNSW index compressed with RQ from the first insert
// (rqActive path) at the given bit depth / retained width, sweeps ef at the
// given rescore limits, finds the rescore window needed for the target
// recall, and optionally re-encodes to narrower widths over the SAME graph
// (build-full / query-truncated diagnostic).
func runRQHNSW(base []float32, dims, n int, queries []float32, gt []int32, gtCols, numQueries, k int,
	p rqHNSWParams, csvPath, dataset string,
) error {
	ctx := context.Background()
	heap0 := heapInUse()

	var mean []float32
	if p.center {
		fmt.Fprintln(os.Stderr, "computing dataset mean ...")
		mean = columnMeans(base, dims)
	}
	compressionhelpers.SetBenchRQCenteringMean(mean)
	defer compressionhelpers.SetBenchRQCenteringMean(nil)

	scratch, err := os.MkdirTemp("", "bitpack-rqhnsw-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(scratch)
	store, err := newScratchStore(scratch)
	if err != nil {
		return err
	}

	fullWidth := ((dims + 63) / 64) * 64
	uc := enthnsw.NewDefaultUserConfig()
	uc.VectorCacheMaxObjects = 1e13
	uc.RQ.Enabled = true
	uc.RQ.Bits = int16(p.bits)
	uc.RQ.TruncatedDims = p.retained
	uc.RQ.RescoreLimit = p.rescoreLims[0]
	if p.retained == fullWidth {
		// Full width: identical to the untruncated quantizer; avoid tripping
		// per-format width restrictions.
		uc.RQ.TruncatedDims = 0
	}

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
		ID:                    "rqbench",
		MakeCommitLoggerThunk: hnsw.MakeNoopCommitLogger,
		ClassName:             "RQBench",
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

	widthLabel := p.retained
	if widthLabel == 0 {
		widthLabel = ((dims + 63) / 64) * 64
	}
	centLabel := "uncentered"
	if p.center {
		centLabel = "centered"
	}
	codeBytes := rqCodeBytes(p.bits, widthLabel)
	if p.bits == 1 && p.center {
		codeBytes += 8 // extra metadata word for the centering correction
	}

	fmt.Fprintf(os.Stderr, "building HNSW rq%d D=%d %s (maxConn=%d efC=%d, %dB/code, parallel build) ...\n",
		p.bits, widthLabel, centLabel, uc.MaxConnections, uc.EFConstruction, codeBytes)
	buildStart := time.Now()
	compressionhelpers.Concurrently(logger, uint64(n), func(id uint64) {
		if err := index.Add(ctx, id, base[int(id)*dims:(int(id)+1)*dims]); err != nil {
			panic(err)
		}
	})
	buildS := time.Since(buildStart).Seconds()
	fmt.Fprintf(os.Stderr, "built in %.1fs\n", buildS)

	heap1 := heapInUse()
	heapMiB := float64(heap1-heap0) / (1 << 20)
	gstats := index.BenchGraphStats()
	graphMiB := float64(gstats.TotalBytes()) / (1 << 20)
	codesMiB := float64(uint64(n)*uint64(codeBytes)) / (1 << 20)
	fmt.Fprintf(os.Stderr, "memory: graph=%.1f MiB (%d edges, %.1f MiB packed conns) codes=%.1f MiB (%dB/code) heap-delta=%.1f MiB\n",
		graphMiB, gstats.Edges, float64(gstats.ConnectionBytes)/(1<<20), codesMiB, codeBytes, heapMiB)

	sweepAt := func(buildD, queryD int) error {
		config := fmt.Sprintf("rq%d build=%d query=%d %s %dB/code", p.bits, buildD, queryD, centLabel, codeBytes)
		for _, rl := range p.rescoreLims {
			for _, ef := range p.efSweep {
				uc.EF = ef
				uc.RQ.RescoreLimit = rl
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
				fmt.Fprintf(os.Stdout, "rq%d D=%d/%d %s rescore=%-4d ef=%-5d recall@%d=%.4f p50=%.3fms p95=%.3fms p99=%.3fms qps=%.1f\n",
					p.bits, buildD, queryD, centLabel, rl, ef, k, recall, ms(lat.p50), ms(lat.p95), ms(lat.p99), qps)
				if err := appendBaselineCSV(csvPath, dataset, baselinePoint{
					index: "rq-hnsw", config: config + fmt.Sprintf(" rescore=%d", rl), param: ef,
					recall: recall, lat: lat, qps: qps, buildS: buildS,
					heapMiB: heapMiB, vecMiB: codesMiB, idxMiB: graphMiB,
					notes:    fmt.Sprintf("graph: %d edges, %d conn bytes, %d overhead bytes", gstats.Edges, gstats.ConnectionBytes, gstats.VertexOverhead),
					nQueries: numQueries, k: k,
				}); err != nil {
					return err
				}
			}
		}
		return nil
	}

	// rescoreWindow sweeps the rescore limit upward at a fixed ef until the
	// target recall is reached, recording every point (the cost curve).
	rescoreWindow := func(buildD, queryD int) error {
		config := fmt.Sprintf("rq%d build=%d query=%d %s window-search", p.bits, buildD, queryD, centLabel)
		for _, rl := range []int{0, 10, 20, 40, 80, 160, 320, 640, 1280} {
			uc.EF = p.rescoreEF
			uc.RQ.RescoreLimit = rl
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
			fmt.Fprintf(os.Stdout, "rq%d D=%d/%d %s window ef=%d rescore=%-5d recall@%d=%.4f qps=%.1f p50=%.3fms\n",
				p.bits, buildD, queryD, centLabel, p.rescoreEF, rl, k, recall, qps, ms(lat.p50))
			if err := appendBaselineCSV(csvPath, dataset, baselinePoint{
				index: "rq-hnsw-window", config: config, param: rl,
				recall: recall, lat: lat, qps: qps, buildS: buildS,
				heapMiB: heapMiB, vecMiB: codesMiB, idxMiB: graphMiB,
				notes:    fmt.Sprintf("ef=%d", p.rescoreEF),
				nQueries: numQueries, k: k,
			}); err != nil {
				return err
			}
			if recall >= p.targetRec {
				fmt.Fprintf(os.Stdout, "rq%d D=%d/%d: rescore window %d reaches %.4f >= %.2f\n",
					p.bits, buildD, queryD, rl, recall, p.targetRec)
				break
			}
		}
		return nil
	}

	if err := sweepAt(widthLabel, widthLabel); err != nil {
		return err
	}
	if err := rescoreWindow(widthLabel, widthLabel); err != nil {
		return err
	}

	// Task 3 diagnostic: same graph, narrower query codes.
	for _, swapD := range p.swaps {
		fmt.Fprintf(os.Stderr, "swapping compressor to D=%d (graph unchanged) ...\n", swapD)
		swapStart := time.Now()
		if err := index.BenchSwapRQCompressor(ctx, compressionhelpers.RQOptions{
			TruncatedDims: swapD,
			Mean:          mean,
		}); err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "swapped in %.1fs\n", time.Since(swapStart).Seconds())
		codeBytes = rqCodeBytes(p.bits, swapD)
		if p.bits == 1 && p.center {
			codeBytes += 8
		}
		codesMiB = float64(uint64(n)*uint64(codeBytes)) / (1 << 20)
		if err := sweepAt(widthLabel, swapD); err != nil {
			return err
		}
		if err := rescoreWindow(widthLabel, swapD); err != nil {
			return err
		}
	}
	return nil
}

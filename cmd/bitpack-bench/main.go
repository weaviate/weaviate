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

// bitpack-bench measures the best-case performance of a column-major,
// bit-packed vector store with progressive elimination as a candidate scan
// path for filtered vector search. Everything in memory, no filters, no
// disk, single-threaded. See README.md.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/vectorindex/compression"
)

const rotationRounds = 3 // same as RQ/BRQ

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "bitpack-bench: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		dataDir    = flag.String("data", filepath.Join(os.Getenv("HOME"), "Documents/datasets/dbpedia-openai-1000k-angular.bin"), "directory produced by convert.sh (train.f32, test.f32, neighbors.i32)")
		dims       = flag.Int("dims", 1536, "input vector dimensionality")
		retained   = flag.Int("retained", 1536, "retained dimensions after rotation (multiple of 64)")
		budgetsArg = flag.String("budgets", "100000,20000,5000,1500,600,350", "survivor budget per block; last value repeats for remaining blocks")
		rotate     = flag.Bool("rotate", true, "apply the RQ fast rotation before sign-bit packing")
		seed       = flag.Uint64("seed", compression.DefaultFastRotationSeed, "rotation seed")
		rescoreN   = flag.Int("rescore", 0, "exact-rescore window (0 = all final survivors)")
		numQueries = flag.Int("queries", 0, "number of queries to run (0 = all)")
		k          = flag.Int("k", 10, "final result count / recall@k")
		csvPath    = flag.String("csv", "bitpack-bench-results.csv", "CSV file to append the run's results to")
	)
	flag.Parse()

	if *retained <= 0 || *retained%64 != 0 {
		return fmt.Errorf("-retained must be a positive multiple of 64, got %d", *retained)
	}
	if *retained > 65535 {
		return fmt.Errorf("-retained %d overflows the uint16 distance accumulator", *retained)
	}
	budgets, err := parseBudgets(*budgetsArg)
	if err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "loading %s ...\n", *dataDir)
	base, n, err := loadFloat32Matrix(filepath.Join(*dataDir, "train.f32"), *dims)
	if err != nil {
		return err
	}
	queries, nq, err := loadFloat32Matrix(filepath.Join(*dataDir, "test.f32"), *dims)
	if err != nil {
		return err
	}
	gtPath := filepath.Join(*dataDir, "neighbors.i32")
	gtCols, err := int32Cols(gtPath, nq)
	if err != nil {
		return err
	}
	gt, _, err := loadInt32Matrix(gtPath, gtCols)
	if err != nil {
		return err
	}
	if gtCols < *k {
		return fmt.Errorf("ground truth has %d neighbors per query, need k=%d", gtCols, *k)
	}
	fmt.Fprintf(os.Stderr, "loaded: %d base, %d queries, %d gt neighbors, %d dims\n", n, nq, gtCols, *dims)

	fmt.Fprintln(os.Stderr, "normalizing ...")
	normalizeRows(base, *dims)
	normalizeRows(queries, *dims)

	var rotation *compression.FastRotation
	if *rotate {
		rotation = compression.NewFastRotation(*dims, rotationRounds, *seed)
		if *retained > int(rotation.OutputDim) {
			return fmt.Errorf("-retained %d exceeds rotation output dim %d", *retained, rotation.OutputDim)
		}
	} else if *retained > *dims {
		return fmt.Errorf("-retained %d exceeds input dims %d without rotation", *retained, *dims)
	}

	fmt.Fprintln(os.Stderr, "building column-major bit-packed store ...")
	buildStart := time.Now()
	store := buildBitStore(base, *dims, n, *retained, rotation)
	fmt.Fprintf(os.Stderr, "built in %.1fs\n", time.Since(buildStart).Seconds())

	if *numQueries <= 0 || *numQueries > nq {
		*numQueries = nq
	}

	sc := newScanner(store, base, *dims, budgets, *rescoreN, *k)

	// Per-query metric collection (allocated up front, outside the hot path).
	blocks := store.blocks
	block0Lat := make([]time.Duration, *numQueries)
	restLat := make([]time.Duration, *numQueries)
	rescoreLat := make([]time.Duration, *numQueries)
	totalLat := make([]time.Duration, *numQueries)
	survivorSums := make([]float64, blocks)
	survivors := make([]int, blocks)
	topK := make([]uint32, *k)
	gtBuf := make([]uint64, *k)
	resBuf := make([]uint64, *k)
	var hits, wanted uint64

	fmt.Fprintf(os.Stderr, "running %d queries ...\n", *numQueries)
	for qi := 0; qi < *numQueries; qi++ {
		q := queries[qi**dims : (qi+1)**dims]
		res := sc.search(q, topK, survivors)

		block0Lat[qi] = res.block0
		restLat[qi] = res.restBlocks
		rescoreLat[qi] = res.rescore
		totalLat[qi] = res.block0 + res.restBlocks + res.rescore
		for b, s := range res.survivors {
			survivorSums[b] += float64(s)
		}

		gtRow := gt[qi*gtCols : qi*gtCols+*k]
		for i, id := range gtRow {
			gtBuf[i] = uint64(id)
		}
		resBuf = resBuf[:len(res.topK)]
		for i, id := range res.topK {
			resBuf[i] = uint64(id)
		}
		hits += testinghelpers.MatchesInLists(gtBuf, resBuf)
		wanted += uint64(*k)
	}

	recall := float64(hits) / float64(wanted)
	avgSurvivors := make([]float64, blocks)
	for b := range survivorSums {
		avgSurvivors[b] = survivorSums[b] / float64(*numQueries)
	}
	bytesPerVec := blocks * 8
	storeBytes := int64(blocks) * int64(n) * 8
	floatBytes := int64(n) * int64(*dims) * 4

	report := runReport{
		dataset:      filepath.Base(*dataDir),
		n:            n,
		dims:         *dims,
		retained:     *retained,
		rotate:       *rotate,
		seed:         *seed,
		budgets:      *budgetsArg,
		rescoreN:     *rescoreN,
		numQueries:   *numQueries,
		k:            *k,
		recall:       recall,
		block0:       percentiles(block0Lat),
		rest:         percentiles(restLat),
		rescore:      percentiles(rescoreLat),
		total:        percentiles(totalLat),
		avgSurvivors: avgSurvivors,
		bytesPerVec:  bytesPerVec,
		storeBytes:   storeBytes,
		floatBytes:   floatBytes,
	}
	report.print(os.Stdout)
	if err := report.appendCSV(*csvPath); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "results appended to %s\n", *csvPath)
	return nil
}

func parseBudgets(s string) ([]int, error) {
	parts := strings.Split(s, ",")
	budgets := make([]int, 0, len(parts))
	for _, p := range parts {
		v, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil || v <= 0 {
			return nil, fmt.Errorf("invalid budget %q", p)
		}
		budgets = append(budgets, v)
	}
	if len(budgets) == 0 {
		return nil, fmt.Errorf("empty budget schedule")
	}
	return budgets, nil
}

func int32Cols(path string, rows int) (int, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	if rows <= 0 || fi.Size()%int64(rows*4) != 0 {
		return 0, fmt.Errorf("%s: size %d does not divide into %d rows of int32", path, fi.Size(), rows)
	}
	return int(fi.Size() / int64(rows*4)), nil
}

type latencyStats struct {
	p50, p95, p99 time.Duration
}

func percentiles(lat []time.Duration) latencyStats {
	s := make([]time.Duration, len(lat))
	copy(s, lat)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	pick := func(q float64) time.Duration {
		if len(s) == 0 {
			return 0
		}
		return s[int(q*float64(len(s)-1)+0.5)]
	}
	return latencyStats{p50: pick(0.50), p95: pick(0.95), p99: pick(0.99)}
}

type runReport struct {
	dataset      string
	n, dims      int
	retained     int
	rotate       bool
	seed         uint64
	budgets      string
	rescoreN     int
	numQueries   int
	k            int
	recall       float64
	block0       latencyStats
	rest         latencyStats
	rescore      latencyStats
	total        latencyStats
	avgSurvivors []float64
	bytesPerVec  int
	storeBytes   int64
	floatBytes   int64
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

func (r *runReport) print(w *os.File) {
	fmt.Fprintf(w, "\n=== bitpack-bench: %s ===\n", r.dataset)
	fmt.Fprintf(w, "n=%d dims=%d retained=%d rotate=%v budgets=%s rescore=%d queries=%d\n",
		r.n, r.dims, r.retained, r.rotate, r.budgets, r.rescoreN, r.numQueries)
	fmt.Fprintf(w, "recall@%d: %.4f\n", r.k, r.recall)
	fmt.Fprintf(w, "latency (ms):        p50      p95      p99\n")
	fmt.Fprintf(w, "  block-0 pass  %8.3f %8.3f %8.3f\n", ms(r.block0.p50), ms(r.block0.p95), ms(r.block0.p99))
	fmt.Fprintf(w, "  rest blocks   %8.3f %8.3f %8.3f\n", ms(r.rest.p50), ms(r.rest.p95), ms(r.rest.p99))
	fmt.Fprintf(w, "  exact rescore %8.3f %8.3f %8.3f\n", ms(r.rescore.p50), ms(r.rescore.p95), ms(r.rescore.p99))
	fmt.Fprintf(w, "  total         %8.3f %8.3f %8.3f\n", ms(r.total.p50), ms(r.total.p95), ms(r.total.p99))
	fmt.Fprintf(w, "avg survivors per block:")
	for _, s := range r.avgSurvivors {
		fmt.Fprintf(w, " %.0f", s)
	}
	fmt.Fprintf(w, "\npacked store: %d bytes/vector, %.1f MiB total (floats for rescore: %.1f MiB)\n",
		r.bytesPerVec, float64(r.storeBytes)/(1<<20), float64(r.floatBytes)/(1<<20))
}

var csvHeader = strings.Join([]string{
	"timestamp", "dataset", "n", "dims", "retained", "rotate", "seed", "budgets", "rescore", "queries", "k",
	"recall", "block0_p50_ms", "block0_p95_ms", "block0_p99_ms",
	"rest_p50_ms", "rest_p95_ms", "rest_p99_ms",
	"rescore_p50_ms", "rescore_p95_ms", "rescore_p99_ms",
	"total_p50_ms", "total_p95_ms", "total_p99_ms",
	"avg_survivors", "bytes_per_vec", "store_bytes", "float_bytes",
}, ",")

func (r *runReport) appendCSV(path string) error {
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
		fmt.Fprintln(f, csvHeader)
	}
	surv := make([]string, len(r.avgSurvivors))
	for i, s := range r.avgSurvivors {
		surv[i] = strconv.FormatFloat(s, 'f', 1, 64)
	}
	_, err = fmt.Fprintf(f, "%s,%s,%d,%d,%d,%v,%d,%q,%d,%d,%d,%.4f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%q,%d,%d,%d\n",
		time.Now().Format(time.RFC3339), r.dataset, r.n, r.dims, r.retained, r.rotate, r.seed, r.budgets,
		r.rescoreN, r.numQueries, r.k, r.recall,
		ms(r.block0.p50), ms(r.block0.p95), ms(r.block0.p99),
		ms(r.rest.p50), ms(r.rest.p95), ms(r.rest.p99),
		ms(r.rescore.p50), ms(r.rescore.p95), ms(r.rescore.p99),
		ms(r.total.p50), ms(r.total.p95), ms(r.total.p99),
		strings.Join(surv, ";"), r.bytesPerVec, r.storeBytes, r.floatBytes)
	return err
}

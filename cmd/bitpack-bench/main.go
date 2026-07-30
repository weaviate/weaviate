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
		dataDir     = flag.String("data", filepath.Join(os.Getenv("HOME"), "Documents/datasets/dbpedia-openai-1000k-angular.bin"), "directory produced by convert.sh (train.f32, test.f32, neighbors.i32)")
		dims        = flag.Int("dims", 1536, "input vector dimensionality")
		retained    = flag.Int("retained", 1536, "retained dimensions after rotation (multiple of 64)")
		budgetsArg  = flag.String("budgets", "100000,20000,5000,1500,600,350", "survivor budget per block; last value repeats for remaining blocks (schedule mode)")
		mode        = flag.String("mode", "schedule", "scan mode: schedule (progressive elimination), full (no-elimination baseline), rankcurve (prefix rank measurement)")
		rotate      = flag.Bool("rotate", true, "apply the RQ fast rotation before sign-bit packing")
		center      = flag.Bool("center", false, "subtract the dataset mean before rotation and sign extraction")
		seed        = flag.Uint64("seed", compression.DefaultFastRotationSeed, "rotation seed")
		rescoreN    = flag.Int("rescore", 0, "exact-rescore window (0 = all final survivors in schedule/hybrid mode, 350 in full mode)")
		numQueries  = flag.Int("queries", 0, "number of queries to run (0 = all)")
		rankQueries = flag.Int("rank-queries", 400, "queries to sample in rankcurve mode / schedule generation")
		genQuantile = flag.Float64("gen-quantile", 0, "generate the budget schedule from the expected-case rank curve at this quantile (0 = use -budgets)")
		sweepArg    = flag.String("rescore-sweep", "", "full mode: comma-separated rescore windows evaluated in one scan (recall per window)")
		k           = flag.Int("k", 10, "final result count / recall@k")
		csvPath     = flag.String("csv", "bitpack-bench-results.csv", "CSV file to append the run's results to")
	)
	flag.Parse()

	switch *mode {
	case "schedule", "full", "hybrid", "rankcurve":
	default:
		return fmt.Errorf("unknown -mode %q", *mode)
	}
	if *mode == "full" && *rescoreN == 0 {
		*rescoreN = 350
	}
	if *genQuantile < 0 || *genQuantile >= 1 {
		return fmt.Errorf("-gen-quantile must be in (0,1), got %v", *genQuantile)
	}

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

	var mean []float32
	if *center {
		fmt.Fprintln(os.Stderr, "computing dataset mean ...")
		mean = columnMeans(base, *dims)
	}

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
	store := buildBitStore(base, *dims, n, *retained, rotation, mean)
	fmt.Fprintf(os.Stderr, "built in %.1fs\n", time.Since(buildStart).Seconds())

	configLabel := "raw"
	switch {
	case *center && *rotate:
		configLabel = "center+rotate"
	case *rotate:
		configLabel = "rotate"
	case *center:
		configLabel = "center"
	}

	if *mode == "rankcurve" {
		fmt.Fprintf(os.Stderr, "measuring prefix rank curve (%s, %d queries) ...\n", configLabel, *rankQueries)
		res := rankCurve(store, queries, *dims, nq, gt, gtCols, *k, *rankQueries)
		rows := rankCurveStats(res)
		sampleN := len(res.worst[0])
		printRankCurve(os.Stdout, configLabel, sampleN, *k, rows)
		if err := appendRankCSV(*csvPath, filepath.Base(*dataDir), configLabel, *rotate, *center, *retained, sampleN, *k, rows); err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "rank curve appended to %s\n", *csvPath)
		return nil
	}

	if *genQuantile > 0 {
		floor := 350
		if *rescoreN > 0 {
			floor = *rescoreN
		}
		fmt.Fprintf(os.Stderr, "generating budget schedule (q=%.2f, floor=%d, %d sampled queries) ...\n", *genQuantile, floor, *rankQueries)
		res := rankCurve(store, queries, *dims, nq, gt, gtCols, *k, *rankQueries)
		budgets = generateBudgets(res, *genQuantile, floor, n)
		parts := make([]string, len(budgets))
		for i, b := range budgets {
			parts[i] = strconv.Itoa(b)
		}
		*budgetsArg = strings.Join(parts, ",")
		fmt.Fprintf(os.Stderr, "generated schedule: %s\n", *budgetsArg)
	}

	if *numQueries <= 0 || *numQueries > nq {
		*numQueries = nq
	}

	sc := newScanner(store, base, *dims, budgets, *rescoreN, *k)

	if *sweepArg != "" {
		if *mode != "full" {
			return fmt.Errorf("-rescore-sweep requires -mode=full")
		}
		windows, err := parseBudgets(*sweepArg)
		if err != nil {
			return err
		}
		sort.Ints(windows)
		return runRescoreSweep(sc, store, queries, gt, gtCols, *numQueries, nq, *dims, *k, windows, *csvPath, filepath.Base(*dataDir), configLabel, *rotate, *center, *retained, *seed)
	}

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

	fmt.Fprintf(os.Stderr, "running %d queries (%s mode, %s) ...\n", *numQueries, *mode, configLabel)
	var totalBytes int64
	var scanTime time.Duration
	loopStart := time.Now()
	for qi := 0; qi < *numQueries; qi++ {
		q := queries[qi**dims : (qi+1)**dims]
		var res queryResult
		switch *mode {
		case "full":
			res = sc.searchFull(q, topK, survivors)
		case "hybrid":
			res = sc.searchHybrid(q, topK, survivors, qi%16 == 0)
		default:
			res = sc.search(q, topK, survivors)
		}
		totalBytes += res.bytesRead
		scanTime += res.block0 + res.restBlocks

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

	wall := time.Since(loopStart)

	recall := float64(hits) / float64(wanted)
	avgSurvivors := make([]float64, blocks)
	for b := range survivorSums {
		avgSurvivors[b] = survivorSums[b] / float64(*numQueries)
	}
	bytesPerVec := blocks * 8
	storeBytes := int64(blocks) * int64(n) * 8
	floatBytes := int64(n) * int64(*dims) * 4
	bytesPerQuery := totalBytes / int64(*numQueries)
	bandwidth := float64(totalBytes) / scanTime.Seconds() / (1 << 30)
	qps := float64(*numQueries) / wall.Seconds()

	// Hybrid mode: replace the model numbers with measured, line-granularity
	// ones, and split bandwidth by access mode.
	var streamGiBs, gatherGiBs float64
	blockModes := ""
	if *mode == "hybrid" {
		streamBytes := sc.hybStreamedCols * int64(n) * 8
		var gatherBytes int64
		for b := 0; b < blocks; b++ {
			if sc.hybGatherCount[b] == 0 {
				continue
			}
			if sc.hybLineSamples[b] > 0 {
				avgLines := float64(sc.hybLineSum[b]) / float64(sc.hybLineSamples[b])
				gatherBytes += int64(avgLines*64) * sc.hybGatherCount[b]
			} else if b > 0 {
				// Never sampled (tiny query counts): fall back to the
				// one-line-per-survivor model on the pre-block survivor count.
				avgK := survivorSums[b-1] / float64(*numQueries)
				gatherBytes += int64(avgK*64) * sc.hybGatherCount[b]
			}
		}
		bytesPerQuery = (streamBytes + gatherBytes) / int64(*numQueries)
		bandwidth = float64(streamBytes+gatherBytes) / scanTime.Seconds() / (1 << 30)
		if sc.hybStreamTime > 0 {
			streamGiBs = float64(streamBytes) / sc.hybStreamTime.Seconds() / (1 << 30)
		}
		if sc.hybGatherTime > 0 {
			gatherGiBs = float64(gatherBytes) / sc.hybGatherTime.Seconds() / (1 << 30)
		}
		modes := make([]byte, blocks)
		for b := 0; b < blocks; b++ {
			switch {
			case sc.hybGatherCount[b] == 0:
				modes[b] = 'S'
			case sc.hybStreamCount[b] == 0:
				modes[b] = 'G'
			default:
				modes[b] = 'M' // mixed across queries
			}
		}
		blockModes = string(modes)
	}

	report := runReport{
		dataset:      filepath.Base(*dataDir),
		mode:         *mode,
		n:            n,
		dims:         *dims,
		retained:     *retained,
		rotate:       *rotate,
		center:       *center,
		config:       configLabel,
		seed:         *seed,
		budgets:      *budgetsArg,
		rescoreN:     *rescoreN,
		numQueries:   *numQueries,
		k:            *k,
		recall:       recall,
		bytesPerQ:    bytesPerQuery,
		bandwidth:    bandwidth,
		qps:          qps,
		genQuantile:  *genQuantile,
		streamGiBs:   streamGiBs,
		gatherGiBs:   gatherGiBs,
		blockModes:   blockModes,
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

// runRescoreSweep runs the full scan once per query, evaluating recall for
// every rescore window in a single pass. One CSV row per window (mode
// "fullsweep"); latency columns describe the shared sweep run, with the
// rescore stage covering the largest window plus per-window evaluation.
func runRescoreSweep(sc *scanner, store *bitStore, queries []float32, gt []int32, gtCols, numQueries, nq, dims, k int, windows []int, csvPath, dataset, configLabel string, rotate, center bool, retained int, seed uint64) error {
	if numQueries <= 0 || numQueries > nq {
		numQueries = nq
	}
	blocks := store.blocks
	outTopK := make([][]uint32, len(windows))
	for i := range outTopK {
		outTopK[i] = make([]uint32, k)
	}
	outFilled := make([]int, len(windows))
	hits := make([]uint64, len(windows))
	survivors := make([]int, blocks)
	gtBuf := make([]uint64, k)
	resBuf := make([]uint64, k)
	block0Lat := make([]time.Duration, numQueries)
	restLat := make([]time.Duration, numQueries)
	rescoreLat := make([]time.Duration, numQueries)
	totalLat := make([]time.Duration, numQueries)

	fmt.Fprintf(os.Stderr, "running %d queries (rescore sweep %v, %s) ...\n", numQueries, windows, configLabel)
	loopStart := time.Now()
	for qi := 0; qi < numQueries; qi++ {
		q := queries[qi*dims : (qi+1)*dims]
		res := sc.searchFullSweep(q, survivors, windows, outTopK, outFilled)
		block0Lat[qi] = res.block0
		restLat[qi] = res.restBlocks
		rescoreLat[qi] = res.rescore
		totalLat[qi] = res.block0 + res.restBlocks + res.rescore

		gtRow := gt[qi*gtCols : qi*gtCols+k]
		for i, id := range gtRow {
			gtBuf[i] = uint64(id)
		}
		for wi := range windows {
			resBuf = resBuf[:outFilled[wi]]
			for i, id := range outTopK[wi][:outFilled[wi]] {
				resBuf[i] = uint64(id)
			}
			hits[wi] += testinghelpers.MatchesInLists(gtBuf, resBuf)
		}
	}
	wall := time.Since(loopStart)
	qps := float64(numQueries) / wall.Seconds()
	bytesPerQuery := int64(blocks) * int64(store.n) * 8

	fmt.Fprintf(os.Stdout, "\n=== rescore sweep: %s retained=%d (%s, %d queries) ===\n", dataset, retained, configLabel, numQueries)
	fmt.Fprintf(os.Stdout, "window   recall@%d\n", k)
	avgSurvivors := make([]float64, blocks)
	for b := range avgSurvivors {
		avgSurvivors[b] = float64(store.n)
	}
	for wi, w := range windows {
		recall := float64(hits[wi]) / float64(uint64(k)*uint64(numQueries))
		fmt.Fprintf(os.Stdout, "%6d   %.4f\n", w, recall)
		report := runReport{
			dataset: dataset, mode: "fullsweep", n: store.n, dims: dims,
			retained: retained, rotate: rotate, center: center, config: configLabel,
			seed: seed, budgets: "", rescoreN: w, numQueries: numQueries, k: k,
			recall: recall, bytesPerQ: bytesPerQuery,
			bandwidth: float64(bytesPerQuery) * float64(numQueries) / (sumDur(block0Lat) + sumDur(restLat)).Seconds() / (1 << 30),
			qps:       qps,
			block0:    percentiles(block0Lat), rest: percentiles(restLat),
			rescore: percentiles(rescoreLat), total: percentiles(totalLat),
			avgSurvivors: avgSurvivors,
			bytesPerVec:  blocks * 8,
			storeBytes:   int64(blocks) * int64(store.n) * 8,
			floatBytes:   int64(store.n) * int64(dims) * 4,
		}
		if err := report.appendCSV(csvPath); err != nil {
			return err
		}
	}
	fmt.Fprintf(os.Stderr, "sweep results appended to %s\n", csvPath)
	return nil
}

func sumDur(ds []time.Duration) time.Duration {
	var t time.Duration
	for _, d := range ds {
		t += d
	}
	return t
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
	mode         string
	n, dims      int
	retained     int
	rotate       bool
	center       bool
	config       string
	seed         uint64
	budgets      string
	rescoreN     int
	numQueries   int
	k            int
	recall       float64
	bytesPerQ    int64
	bandwidth    float64 // GiB/s over scan time (block0 + rest)
	qps          float64
	genQuantile  float64
	streamGiBs   float64 // hybrid: streamed-block bandwidth
	gatherGiBs   float64 // hybrid: gathered-block bandwidth (measured lines)
	blockModes   string  // hybrid: S/G/M per block
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
	fmt.Fprintf(w, "\n=== bitpack-bench: %s (%s mode, %s) ===\n", r.dataset, r.mode, r.config)
	fmt.Fprintf(w, "n=%d dims=%d retained=%d rotate=%v center=%v budgets=%s rescore=%d queries=%d\n",
		r.n, r.dims, r.retained, r.rotate, r.center, r.budgets, r.rescoreN, r.numQueries)
	fmt.Fprintf(w, "recall@%d: %.4f\n", r.k, r.recall)
	fmt.Fprintf(w, "code-store reads: %.2f MiB/query, effective bandwidth %.2f GiB/s, single-threaded QPS %.1f\n",
		float64(r.bytesPerQ)/(1<<20), r.bandwidth, r.qps)
	if r.blockModes != "" {
		fmt.Fprintf(w, "hybrid blocks [S=stream G=gather]: %s; stream %.2f GiB/s, gather %.2f GiB/s\n",
			r.blockModes, r.streamGiBs, r.gatherGiBs)
	}
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
	"timestamp", "dataset", "mode", "config", "n", "dims", "retained", "rotate", "center", "seed", "budgets", "rescore", "queries", "k",
	"recall", "block0_p50_ms", "block0_p95_ms", "block0_p99_ms",
	"rest_p50_ms", "rest_p95_ms", "rest_p99_ms",
	"rescore_p50_ms", "rescore_p95_ms", "rescore_p99_ms",
	"total_p50_ms", "total_p95_ms", "total_p99_ms",
	"avg_survivors", "bytes_per_vec", "store_bytes", "float_bytes",
	"bytes_read_per_query", "bandwidth_gib_s", "qps",
	"gen_quantile", "block_modes", "stream_gib_s", "gather_gib_s",
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
	_, err = fmt.Fprintf(f, "%s,%s,%s,%s,%d,%d,%d,%v,%v,%d,%q,%d,%d,%d,%.4f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%q,%d,%d,%d,%d,%.3f,%.2f,%.2f,%s,%.2f,%.2f\n",
		time.Now().Format(time.RFC3339), r.dataset, r.mode, r.config, r.n, r.dims, r.retained, r.rotate, r.center, r.seed, r.budgets,
		r.rescoreN, r.numQueries, r.k, r.recall,
		ms(r.block0.p50), ms(r.block0.p95), ms(r.block0.p99),
		ms(r.rest.p50), ms(r.rest.p95), ms(r.rest.p99),
		ms(r.rescore.p50), ms(r.rescore.p95), ms(r.rescore.p99),
		ms(r.total.p50), ms(r.total.p95), ms(r.total.p99),
		strings.Join(surv, ";"), r.bytesPerVec, r.storeBytes, r.floatBytes,
		r.bytesPerQ, r.bandwidth, r.qps,
		r.genQuantile, r.blockModes, r.streamGiBs, r.gatherGiBs)
	return err
}

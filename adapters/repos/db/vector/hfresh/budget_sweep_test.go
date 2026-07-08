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

//go:build budgetsweep
// +build budgetsweep

// Budget Sweep Diagnostic Benchmark for HFresh+MUVERA
//
// This is a measurement-only diagnostic tool to understand the recall/latency/IO
// tradeoffs for different routing and rerank budget configurations.
//
// Run with: go test -tags=budgetsweep -run TestBudgetSweep -timeout 2h ./adapters/repos/db/vector/hfresh/...
//
// Environment variables:
//   SWEEP_NUM_DOCS=1000      Number of documents to index
//   SWEEP_TOKENS_PER_DOC=32  Tokens per document
//   SWEEP_DIMS=128           Vector dimensions
//   SWEEP_NUM_QUERIES=100    Number of queries to run
//   SWEEP_OUTPUT=results.csv Output CSV file

package hfresh

import (
	"context"
	"encoding/csv"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hfresh"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// SearchMetrics captures detailed metrics from a single search operation
type SearchMetrics struct {
	// Quality
	RecallAtK float64

	// Latency
	TotalLatencyMs    float64
	RoutingLatencyMs  float64
	PostingScanMs     float64
	MaxSimFetchMs     float64
	MaxSimComputeMs   float64

	// Routing/posting cost
	CentroidsSearched    int
	PostingsScanned      int
	VectorsScanned       int
	UniqueDocsEnumerated int

	// MaxSim/IO cost
	DocsPassedToMaxSim     int
	TotalDocTokensLoaded   int
	TotalVectorBytesLoaded int64
	MaxSimPairComparisons  int64
}

// BudgetConfig represents a single budget configuration
type BudgetConfig struct {
	K             int
	RoutingBudget int
	RerankBudget  int
}

// SweepResult contains aggregated results for a configuration
type SweepResult struct {
	Config  BudgetConfig
	Metrics SearchMetrics

	// Aggregated stats
	AvgLatencyMs float64
	P50LatencyMs float64
	P95LatencyMs float64
}

// InstrumentedHFresh wraps HFresh with metric collection
type InstrumentedHFresh struct {
	*HFresh
	mu      sync.Mutex
	metrics SearchMetrics

	// Per-search accumulators (reset before each search)
	searchStart        time.Time
	routingStart       time.Time
	postingScanStart   time.Time
	maxSimFetchStart   time.Time
	maxSimComputeStart time.Time
}

func (ih *InstrumentedHFresh) resetMetrics() {
	ih.mu.Lock()
	defer ih.mu.Unlock()
	ih.metrics = SearchMetrics{}
}

func (ih *InstrumentedHFresh) getMetrics() SearchMetrics {
	ih.mu.Lock()
	defer ih.mu.Unlock()
	return ih.metrics
}

// searchByFDEInstrumented is a copy of searchByFDE with instrumentation
func (ih *InstrumentedHFresh) searchByFDEInstrumented(
	ctx context.Context,
	queryFDE []float32,
	routingBudget int,
	rerankBudget int,
	allowList interface{},
	queryTokens int,
) ([]uint64, error) {
	h := ih.HFresh

	ih.mu.Lock()
	ih.routingStart = time.Now()
	ih.mu.Unlock()

	// Centroid search
	centroids, err := h.Centroids.Search(queryFDE, routingBudget, nil)
	if err != nil {
		return nil, err
	}

	ih.mu.Lock()
	routingDuration := time.Since(ih.routingStart)
	ih.metrics.RoutingLatencyMs = float64(routingDuration.Microseconds()) / 1000.0
	ih.metrics.CentroidsSearched = len(centroids.data)
	ih.mu.Unlock()

	if len(centroids.data) == 0 {
		return nil, nil
	}

	// Filter centroids
	maxDist := centroids.data[0].Distance * h.config.MaxDistanceRatio
	selectedCentroids := make([]uint64, 0, routingBudget)
	for i := 0; i < len(centroids.data) && len(selectedCentroids) < routingBudget; i++ {
		if maxDist > pruningMinMaxDistance && centroids.data[i].Distance > maxDist {
			continue
		}
		count, err := h.PostingSizes.Get(ctx, centroids.data[i].ID)
		if err != nil {
			return nil, err
		}
		if count == 0 {
			continue
		}
		selectedCentroids = append(selectedCentroids, centroids.data[i].ID)
	}

	ih.mu.Lock()
	ih.postingScanStart = time.Now()
	ih.mu.Unlock()

	// Read postings
	postings, err := h.PostingStore.MultiGet(ctx, selectedCentroids)
	if err != nil {
		return nil, err
	}

	ih.mu.Lock()
	ih.metrics.PostingsScanned = len(postings)
	ih.mu.Unlock()

	visited := h.visitedPool.Borrow()
	defer h.visitedPool.Return(visited)

	q := NewResultSet(rerankBudget)
	queryDistancer := h.quantizer.NewDistancer(queryFDE)
	var decompressBuf []uint64
	totalVectorsScanned := 0
	uniqueDocs := 0

	for _, p := range postings {
		if p == nil {
			continue
		}
		for _, v := range p {
			totalVectorsScanned++
			id := v.ID()

			deleted, err := h.VersionMap.IsDeleted(ctx, id)
			if err != nil {
				return nil, err
			}
			if deleted {
				continue
			}

			if visited.CheckAndVisit(id) {
				continue
			}
			uniqueDocs++

			decompressBuf = h.quantizer.FromCompressedBytesInto(v.Data(), decompressBuf)
			dist, err := queryDistancer.Distance(decompressBuf)
			if err != nil {
				return nil, err
			}
			q.Insert(id, dist)
		}
	}

	ih.mu.Lock()
	postingScanDuration := time.Since(ih.postingScanStart)
	ih.metrics.PostingScanMs = float64(postingScanDuration.Microseconds()) / 1000.0
	ih.metrics.VectorsScanned = totalVectorsScanned
	ih.metrics.UniqueDocsEnumerated = uniqueDocs
	ih.metrics.DocsPassedToMaxSim = q.Len()
	ih.mu.Unlock()

	// Extract candidate IDs
	ids := make([]uint64, 0, q.Len())
	for id := range q.Iter() {
		ids = append(ids, id)
	}

	return ids, nil
}

// computeLateInteractionInstrumented wraps MaxSim with instrumentation
func (ih *InstrumentedHFresh) computeLateInteractionInstrumented(
	ctx context.Context,
	queryVectors [][]float32,
	k int,
	candidateIDs []uint64,
) ([]uint64, []float32, error) {
	h := ih.HFresh

	ih.mu.Lock()
	ih.maxSimFetchStart = time.Now()
	ih.mu.Unlock()

	results := NewResultSet(k)
	totalDocTokens := 0
	totalBytes := int64(0)
	totalComparisons := int64(0)
	fetchDuration := time.Duration(0)
	computeDuration := time.Duration(0)

	for _, id := range candidateIDs {
		fetchStart := time.Now()
		docVectors, err := h.multivectorForIdThunk(ctx, id)
		fetchDuration += time.Since(fetchStart)

		if err != nil {
			return nil, nil, err
		}

		docTokens := len(docVectors)
		totalDocTokens += docTokens
		if docTokens > 0 && len(docVectors[0]) > 0 {
			totalBytes += int64(docTokens * len(docVectors[0]) * 4)
		}

		computeStart := time.Now()
		score, err := h.maxSimScore(queryVectors, docVectors)
		computeDuration += time.Since(computeStart)

		if err != nil {
			return nil, nil, err
		}

		totalComparisons += int64(len(queryVectors) * docTokens)
		results.Insert(id, score)
	}

	ih.mu.Lock()
	ih.metrics.MaxSimFetchMs = float64(fetchDuration.Microseconds()) / 1000.0
	ih.metrics.MaxSimComputeMs = float64(computeDuration.Microseconds()) / 1000.0
	ih.metrics.TotalDocTokensLoaded = totalDocTokens
	ih.metrics.TotalVectorBytesLoaded = totalBytes
	ih.metrics.MaxSimPairComparisons = totalComparisons
	ih.mu.Unlock()

	ids := make([]uint64, results.Len())
	dists := make([]float32, results.Len())
	i := 0
	for id, dist := range results.Iter() {
		ids[i] = id
		dists[i] = dist
		i++
	}

	return ids, dists, nil
}

// SearchByMultiVectorInstrumented performs an instrumented search
func (ih *InstrumentedHFresh) SearchByMultiVectorInstrumented(
	ctx context.Context,
	vectors [][]float32,
	k int,
	routingBudget int,
	rerankBudget int,
) ([]uint64, []float32, error) {
	ih.resetMetrics()

	ih.mu.Lock()
	ih.searchStart = time.Now()
	ih.mu.Unlock()

	h := ih.HFresh

	if !h.muvera.Load() {
		return nil, nil, ErrMuveraNotEnabled
	}

	if h.muveraEncoder == nil || h.muveraEncoder.Dimensions() == 0 {
		return nil, nil, fmt.Errorf("muvera encoder not initialized")
	}

	queryFDE := h.muveraEncoder.EncodeQuery(vectors)

	// Use the REAL production searchByFDE (not the instrumented copy)
	// to ensure we are testing actual decoupled behavior
	ih.mu.Lock()
	ih.routingStart = time.Now()
	ih.mu.Unlock()

	candidateIDs, err := h.searchByFDE(ctx, queryFDE, routingBudget, rerankBudget, nil)
	if err != nil {
		return nil, nil, err
	}

	ih.mu.Lock()
	ih.metrics.RoutingLatencyMs = float64(time.Since(ih.routingStart).Microseconds()) / 1000.0
	ih.metrics.DocsPassedToMaxSim = len(candidateIDs)
	ih.mu.Unlock()

	ids, dists, err := ih.computeLateInteractionInstrumented(ctx, vectors, k, candidateIDs)

	ih.mu.Lock()
	ih.metrics.TotalLatencyMs = float64(time.Since(ih.searchStart).Microseconds()) / 1000.0
	ih.mu.Unlock()

	return ids, dists, err
}

// computeRecall calculates recall@k
func computeRecall(results []uint64, groundTruth []uint64, k int) float64 {
	if len(groundTruth) == 0 || len(results) == 0 {
		return 0.0
	}

	truthSet := make(map[uint64]bool)
	limit := k
	if limit > len(groundTruth) {
		limit = len(groundTruth)
	}
	for i := 0; i < limit; i++ {
		truthSet[groundTruth[i]] = true
	}

	hits := 0
	resultLimit := k
	if resultLimit > len(results) {
		resultLimit = len(results)
	}
	for i := 0; i < resultLimit; i++ {
		if truthSet[results[i]] {
			hits++
		}
	}

	return float64(hits) / float64(limit)
}

// generateSyntheticMultiVectorData creates test data
func generateSyntheticMultiVectorData(
	numDocs int,
	tokensPerDoc int,
	dims int,
	numQueries int,
	queryTokens int,
	seed int64,
) (docs [][][]float32, queries [][][]float32, groundTruth [][]uint64) {
	rng := rand.New(rand.NewSource(seed))

	// Generate documents
	docs = make([][][]float32, numDocs)
	for i := 0; i < numDocs; i++ {
		doc := make([][]float32, tokensPerDoc)
		for j := 0; j < tokensPerDoc; j++ {
			vec := make([]float32, dims)
			for k := 0; k < dims; k++ {
				vec[k] = rng.Float32()
			}
			doc[j] = vec
		}
		docs[i] = doc
	}

	// Generate queries
	queries = make([][][]float32, numQueries)
	for i := 0; i < numQueries; i++ {
		query := make([][]float32, queryTokens)
		for j := 0; j < queryTokens; j++ {
			vec := make([]float32, dims)
			for k := 0; k < dims; k++ {
				vec[k] = rng.Float32()
			}
			query[j] = vec
		}
		queries[i] = query
	}

	// Compute ground truth via brute force MaxSim
	fmt.Println("Computing ground truth (brute force MaxSim)...")
	groundTruth = make([][]uint64, numQueries)
	distProvider := distancer.NewL2SquaredProvider()

	for qi, query := range queries {
		scores := make([]struct {
			id    uint64
			score float32
		}, numDocs)

		for di, doc := range docs {
			score := bruteForceMaxSim(query, doc, distProvider)
			scores[di] = struct {
				id    uint64
				score float32
			}{uint64(di), score}
		}

		// Sort by score (ascending for distance)
		sort.Slice(scores, func(i, j int) bool {
			return scores[i].score < scores[j].score
		})

		groundTruth[qi] = make([]uint64, minInt(100, numDocs))
		for i := 0; i < len(groundTruth[qi]); i++ {
			groundTruth[qi][i] = scores[i].id
		}

		if (qi+1)%10 == 0 {
			fmt.Printf("  Ground truth: %d/%d queries\n", qi+1, numQueries)
		}
	}

	return docs, queries, groundTruth
}

func bruteForceMaxSim(query [][]float32, doc [][]float32, distProvider distancer.Provider) float32 {
	var score float32
	for _, queryToken := range query {
		d := distProvider.New(queryToken)
		minDist := float32(math.MaxFloat32)
		for _, docToken := range doc {
			dist, _ := d.Distance(docToken)
			if dist < minDist {
				minDist = dist
			}
		}
		score += minDist
	}
	return score
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func getEnvInt(key string, defaultVal int) int {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return defaultVal
}

func TestBudgetSweep(t *testing.T) {
	// Configuration from environment
	numDocs := getEnvInt("SWEEP_NUM_DOCS", 500)
	tokensPerDoc := getEnvInt("SWEEP_TOKENS_PER_DOC", 32)
	dims := getEnvInt("SWEEP_DIMS", 128)
	numQueries := getEnvInt("SWEEP_NUM_QUERIES", 50)
	queryTokens := getEnvInt("SWEEP_QUERY_TOKENS", 32)
	outputFile := os.Getenv("SWEEP_OUTPUT")
	if outputFile == "" {
		outputFile = "budget_sweep_results.csv"
	}

	fmt.Printf("=== HFresh+MUVERA Budget Sweep Benchmark ===\n")
	fmt.Printf("Documents: %d, Tokens/doc: %d, Dims: %d\n", numDocs, tokensPerDoc, dims)
	fmt.Printf("Queries: %d, Query tokens: %d\n", numQueries, queryTokens)
	fmt.Printf("Output: %s\n\n", outputFile)

	// Generate synthetic data
	fmt.Println("Generating synthetic multi-vector data...")
	docs, queries, groundTruth := generateSyntheticMultiVectorData(
		numDocs, tokensPerDoc, dims, numQueries, queryTokens, 42,
	)

	// Create index
	fmt.Println("Creating HFresh+MUVERA index...")
	logger, _ := test.NewNullLogger()
	logger.SetLevel(logrus.WarnLevel)

	store := testinghelpers.NewDummyStore(t)
	cfg := DefaultConfig()
	cfg.RootPath = t.TempDir()
	cfg.ID = "budget_sweep"

	distProvider := distancer.NewL2SquaredProvider()
	cfg.DistanceProvider = distProvider

	scheduler := queue.NewScheduler(queue.SchedulerOptions{Logger: logger})
	cfg.Scheduler = scheduler
	scheduler.Start()
	defer scheduler.Close(context.Background())

	cfg.Centroids.HNSWConfig = &hnsw.Config{
		RootPath:              t.TempDir(),
		ID:                    "budget_sweep_centroids",
		MakeCommitLoggerThunk: makeNoopCommitLogger,
		GetViewThunk:          func() common.BucketView { return &noopBucketView{} },
		DistanceProvider:      distProvider,
		AllocChecker:          memwatch.NewDummyMonitor(),
		MakeBucketOptions:     lsmkv.MakeNoopBucketOptions,
	}
	cfg.TombstoneCallbacks = cyclemanager.NewCallbackGroupNoop()
	cfg.Logger = logger

	// Create multi-vector store
	mvStore := newMuveraTestStore()
	cfg.MultiVectorForIDThunk = func(ctx context.Context, id uint64) ([][]float32, error) {
		return mvStore.getMultiVector(id)
	}

	uc := ent.NewDefaultUserConfig()
	uc.Multivector.Enabled = true
	uc.Multivector.MuveraConfig.Enabled = true
	uc.Multivector.MuveraConfig.KSim = enthnsw.DefaultMultivectorKSim
	uc.Multivector.MuveraConfig.DProjections = enthnsw.DefaultMultivectorDProjections
	uc.Multivector.MuveraConfig.Repetitions = enthnsw.DefaultMultivectorRepetitions

	index, err := New(cfg, uc, store)
	require.NoError(t, err)
	index.multivectorForIdThunk = cfg.MultiVectorForIDThunk
	defer index.Shutdown(context.Background())

	// Index documents
	fmt.Println("Indexing documents...")
	var indexedCount atomic.Int32
	for i, doc := range docs {
		mvStore.storeMultiVector(uint64(i), doc)
		err := index.AddMulti(context.Background(), uint64(i), doc)
		require.NoError(t, err)
		indexedCount.Add(1)
		if (i+1)%100 == 0 {
			fmt.Printf("  Indexed %d/%d documents\n", i+1, numDocs)
		}
	}

	// Wait for background tasks
	fmt.Println("Waiting for background tasks...")
	for index.taskQueue.Size() > 0 {
		time.Sleep(100 * time.Millisecond)
	}

	// Create instrumented wrapper
	ih := &InstrumentedHFresh{HFresh: index}

	// Define sweep configurations
	kValues := []int{10, 50, 100}
	routingBudgets := []int{24, 32, 64, 96, 128, 192, 256, 350, 512, 1024}
	rerankBudgets := []int{24, 32, 64, 96, 128, 192, 256, 350, 512}

	// Open output CSV
	csvFile, err := os.Create(outputFile)
	require.NoError(t, err)
	defer csvFile.Close()

	writer := csv.NewWriter(csvFile)
	defer writer.Flush()

	// Write header
	header := []string{
		"k", "routing_budget", "rerank_budget", "is_default",
		"recall_at_k",
		"avg_latency_ms", "p50_latency_ms", "p95_latency_ms",
		"routing_latency_ms", "posting_scan_ms", "maxsim_fetch_ms", "maxsim_compute_ms",
		"centroids_searched", "postings_scanned", "vectors_scanned", "unique_docs_enumerated",
		"docs_to_maxsim", "doc_tokens_loaded", "vector_bytes_loaded", "maxsim_comparisons",
	}
	writer.Write(header)

	fmt.Println("\n=== Running Budget Sweep ===")

	for _, k := range kValues {
		fmt.Printf("\n--- k=%d ---\n", k)

		for _, routingBudget := range routingBudgets {
			for _, rerankBudget := range rerankBudgets {
				// Skip invalid configurations
				if rerankBudget < k {
					continue
				}

				// Check if this matches the default behavior
				// Default: routingBudget = max(searchProbe, rescoreLimit), rerankBudget = rescoreLimit
				// With searchProbe=256, rescoreLimit=350
				isDefault := routingBudget == 350 && rerankBudget == 350

				// Run queries
				var totalRecall float64
				var latencies []float64
				var aggMetrics SearchMetrics

				for qi, query := range queries {
					results, _, err := ih.SearchByMultiVectorInstrumented(
						context.Background(),
						query,
						k,
						routingBudget,
						rerankBudget,
					)
					require.NoError(t, err)

					recall := computeRecall(results, groundTruth[qi], k)
					totalRecall += recall

					m := ih.getMetrics()
					latencies = append(latencies, m.TotalLatencyMs)

					// Accumulate metrics
					aggMetrics.RoutingLatencyMs += m.RoutingLatencyMs
					aggMetrics.PostingScanMs += m.PostingScanMs
					aggMetrics.MaxSimFetchMs += m.MaxSimFetchMs
					aggMetrics.MaxSimComputeMs += m.MaxSimComputeMs
					aggMetrics.CentroidsSearched += m.CentroidsSearched
					aggMetrics.PostingsScanned += m.PostingsScanned
					aggMetrics.VectorsScanned += m.VectorsScanned
					aggMetrics.UniqueDocsEnumerated += m.UniqueDocsEnumerated
					aggMetrics.DocsPassedToMaxSim += m.DocsPassedToMaxSim
					aggMetrics.TotalDocTokensLoaded += m.TotalDocTokensLoaded
					aggMetrics.TotalVectorBytesLoaded += m.TotalVectorBytesLoaded
					aggMetrics.MaxSimPairComparisons += m.MaxSimPairComparisons
				}

				// Compute averages
				n := float64(numQueries)
				avgRecall := totalRecall / n
				avgLatency := 0.0
				for _, l := range latencies {
					avgLatency += l
				}
				avgLatency /= n

				// Compute percentiles
				sort.Float64s(latencies)
				p50 := latencies[len(latencies)/2]
				p95 := latencies[int(float64(len(latencies))*0.95)]

				// Average metrics
				avgMetrics := SearchMetrics{
					RoutingLatencyMs:       aggMetrics.RoutingLatencyMs / n,
					PostingScanMs:          aggMetrics.PostingScanMs / n,
					MaxSimFetchMs:          aggMetrics.MaxSimFetchMs / n,
					MaxSimComputeMs:        aggMetrics.MaxSimComputeMs / n,
					CentroidsSearched:      int(float64(aggMetrics.CentroidsSearched) / n),
					PostingsScanned:        int(float64(aggMetrics.PostingsScanned) / n),
					VectorsScanned:         int(float64(aggMetrics.VectorsScanned) / n),
					UniqueDocsEnumerated:   int(float64(aggMetrics.UniqueDocsEnumerated) / n),
					DocsPassedToMaxSim:     int(float64(aggMetrics.DocsPassedToMaxSim) / n),
					TotalDocTokensLoaded:   int(float64(aggMetrics.TotalDocTokensLoaded) / n),
					TotalVectorBytesLoaded: int64(float64(aggMetrics.TotalVectorBytesLoaded) / n),
					MaxSimPairComparisons:  int64(float64(aggMetrics.MaxSimPairComparisons) / n),
				}

				// Write CSV row
				isDefaultStr := "0"
				if isDefault {
					isDefaultStr = "1"
				}

				row := []string{
					strconv.Itoa(k),
					strconv.Itoa(routingBudget),
					strconv.Itoa(rerankBudget),
					isDefaultStr,
					fmt.Sprintf("%.4f", avgRecall),
					fmt.Sprintf("%.2f", avgLatency),
					fmt.Sprintf("%.2f", p50),
					fmt.Sprintf("%.2f", p95),
					fmt.Sprintf("%.2f", avgMetrics.RoutingLatencyMs),
					fmt.Sprintf("%.2f", avgMetrics.PostingScanMs),
					fmt.Sprintf("%.2f", avgMetrics.MaxSimFetchMs),
					fmt.Sprintf("%.2f", avgMetrics.MaxSimComputeMs),
					strconv.Itoa(avgMetrics.CentroidsSearched),
					strconv.Itoa(avgMetrics.PostingsScanned),
					strconv.Itoa(avgMetrics.VectorsScanned),
					strconv.Itoa(avgMetrics.UniqueDocsEnumerated),
					strconv.Itoa(avgMetrics.DocsPassedToMaxSim),
					strconv.Itoa(avgMetrics.TotalDocTokensLoaded),
					strconv.FormatInt(avgMetrics.TotalVectorBytesLoaded, 10),
					strconv.FormatInt(avgMetrics.MaxSimPairComparisons, 10),
				}
				writer.Write(row)

				// Print summary
				defaultMarker := ""
				if isDefault {
					defaultMarker = " [DEFAULT]"
				}
				fmt.Printf("routing=%d, rerank=%d: recall=%.3f, latency=%.1fms, maxsim_docs=%d%s\n",
					routingBudget, rerankBudget, avgRecall, avgLatency, avgMetrics.DocsPassedToMaxSim, defaultMarker)
			}
		}
	}

	writer.Flush()
	fmt.Printf("\n=== Results written to %s ===\n", outputFile)

	// Print summary analysis
	fmt.Println("\n=== Summary Analysis ===")
	printSummaryAnalysis(outputFile)
}

func printSummaryAnalysis(csvFile string) {
	file, err := os.Open(csvFile)
	if err != nil {
		fmt.Printf("Error reading results: %v\n", err)
		return
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		fmt.Printf("Error parsing CSV: %v\n", err)
		return
	}

	if len(records) < 2 {
		fmt.Println("No data to analyze")
		return
	}

	// Parse results grouped by k
	type Result struct {
		k, routing, rerank int
		recall             float64
		latency            float64
		docsToMaxSim       int
		bytesLoaded        int64
	}

	resultsByK := make(map[int][]Result)

	for i := 1; i < len(records); i++ {
		row := records[i]
		k, _ := strconv.Atoi(row[0])
		routing, _ := strconv.Atoi(row[1])
		rerank, _ := strconv.Atoi(row[2])
		recall, _ := strconv.ParseFloat(row[4], 64)
		latency, _ := strconv.ParseFloat(row[5], 64)
		docsToMaxSim, _ := strconv.Atoi(row[16])
		bytesLoaded, _ := strconv.ParseInt(row[18], 10, 64)

		resultsByK[k] = append(resultsByK[k], Result{
			k: k, routing: routing, rerank: rerank,
			recall: recall, latency: latency,
			docsToMaxSim: docsToMaxSim, bytesLoaded: bytesLoaded,
		})
	}

	for _, k := range []int{10, 50, 100} {
		results := resultsByK[k]
		if len(results) == 0 {
			continue
		}

		fmt.Printf("\n--- k=%d Analysis ---\n", k)

		// Top 5 by recall
		sort.Slice(results, func(i, j int) bool {
			return results[i].recall > results[j].recall
		})
		fmt.Println("Top 5 by recall:")
		for i := 0; i < minInt(5, len(results)); i++ {
			r := results[i]
			fmt.Printf("  routing=%d, rerank=%d: recall=%.3f, latency=%.1fms\n",
				r.routing, r.rerank, r.recall, r.latency)
		}

		// Best recall per MaxSim docs
		fmt.Println("Best recall for each MaxSim doc budget:")
		budgets := []int{24, 32, 64, 128, 256, 350}
		for _, budget := range budgets {
			best := Result{recall: -1}
			for _, r := range results {
				if r.rerank == budget && r.recall > best.recall {
					best = r
				}
			}
			if best.recall >= 0 {
				fmt.Printf("  rerank=%d: recall=%.3f (routing=%d)\n",
					best.rerank, best.recall, best.routing)
			}
		}

		// Find configurations with recall >= 0.9 * best recall
		bestRecall := results[0].recall
		threshold := bestRecall * 0.9

		var acceptable []Result
		for _, r := range results {
			if r.recall >= threshold {
				acceptable = append(acceptable, r)
			}
		}

		// Sort acceptable by MaxSim docs (lower is better)
		sort.Slice(acceptable, func(i, j int) bool {
			return acceptable[i].docsToMaxSim < acceptable[j].docsToMaxSim
		})

		if len(acceptable) > 0 {
			fmt.Printf("Most efficient with >=90%% of best recall (%.3f):\n", bestRecall)
			for i := 0; i < minInt(3, len(acceptable)); i++ {
				r := acceptable[i]
				fmt.Printf("  routing=%d, rerank=%d: recall=%.3f, maxsim_docs=%d, bytes=%dKB\n",
					r.routing, r.rerank, r.recall, r.docsToMaxSim, r.bytesLoaded/1024)
			}
		}
	}
}

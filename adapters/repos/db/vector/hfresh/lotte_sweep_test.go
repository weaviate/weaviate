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

//go:build lottesweep
// +build lottesweep

// LOTTE Budget Sweep - Real Data Benchmark for HFresh+MUVERA
//
// Uses real LOTTE lifestyle dataset to measure recall/latency/IO tradeoffs
// with decoupled routing and rerank budgets.
//
// Run with: go test -tags=lottesweep -run TestLOTTESweep -timeout 4h ./adapters/repos/db/vector/hfresh/...

package hfresh

import (
	"context"
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"io"
	"math"
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

const (
	lotteDataDir = "/tmp/lotte_sweep_data"
)

// LOTTEData holds loaded LOTTE dataset
type LOTTEData struct {
	Docs        [][][]float32 // [docID][tokenIdx][dim]
	Queries     [][][]float32 // [queryID][tokenIdx][dim]
	GroundTruth [][]uint64    // [queryID][k] - top-k doc IDs
	Dim         int
}

func loadLOTTEData(t *testing.T) *LOTTEData {
	t.Helper()

	data := &LOTTEData{}

	// Load documents
	docsFile, err := os.Open(lotteDataDir + "/docs.bin")
	require.NoError(t, err)
	defer docsFile.Close()

	var numDocs, dim uint32
	require.NoError(t, binary.Read(docsFile, binary.LittleEndian, &numDocs))
	require.NoError(t, binary.Read(docsFile, binary.LittleEndian, &dim))
	data.Dim = int(dim)

	fmt.Printf("Loading %d documents (dim=%d)...\n", numDocs, dim)
	data.Docs = make([][][]float32, numDocs)

	for i := uint32(0); i < numDocs; i++ {
		var numTokens uint32
		require.NoError(t, binary.Read(docsFile, binary.LittleEndian, &numTokens))

		doc := make([][]float32, numTokens)
		for j := uint32(0); j < numTokens; j++ {
			vec := make([]float32, dim)
			require.NoError(t, binary.Read(docsFile, binary.LittleEndian, vec))
			doc[j] = vec
		}
		data.Docs[i] = doc

		if (i+1)%5000 == 0 {
			fmt.Printf("  Loaded %d/%d docs\n", i+1, numDocs)
		}
	}

	// Load queries
	queriesFile, err := os.Open(lotteDataDir + "/queries.bin")
	require.NoError(t, err)
	defer queriesFile.Close()

	var numQueries uint32
	require.NoError(t, binary.Read(queriesFile, binary.LittleEndian, &numQueries))
	require.NoError(t, binary.Read(queriesFile, binary.LittleEndian, &dim))

	fmt.Printf("Loading %d queries...\n", numQueries)
	data.Queries = make([][][]float32, numQueries)

	for i := uint32(0); i < numQueries; i++ {
		var numTokens uint32
		require.NoError(t, binary.Read(queriesFile, binary.LittleEndian, &numTokens))

		query := make([][]float32, numTokens)
		for j := uint32(0); j < numTokens; j++ {
			vec := make([]float32, dim)
			require.NoError(t, binary.Read(queriesFile, binary.LittleEndian, vec))
			query[j] = vec
		}
		data.Queries[i] = query
	}

	// Load ground truth
	gtFile, err := os.Open(lotteDataDir + "/ground_truth.bin")
	require.NoError(t, err)
	defer gtFile.Close()

	var gtNumQueries, gtK uint32
	require.NoError(t, binary.Read(gtFile, binary.LittleEndian, &gtNumQueries))
	require.NoError(t, binary.Read(gtFile, binary.LittleEndian, &gtK))

	fmt.Printf("Loading ground truth (k=%d)...\n", gtK)
	data.GroundTruth = make([][]uint64, gtNumQueries)

	for i := uint32(0); i < gtNumQueries; i++ {
		gt := make([]uint32, gtK)
		require.NoError(t, binary.Read(gtFile, binary.LittleEndian, gt))

		// Convert to uint64, filter sentinels
		data.GroundTruth[i] = make([]uint64, 0, gtK)
		for _, id := range gt {
			if id != 0xFFFFFFFF {
				data.GroundTruth[i] = append(data.GroundTruth[i], uint64(id))
			}
		}
	}

	return data
}

// SweepMetrics captures detailed metrics for analysis
type SweepMetrics struct {
	K             int
	RoutingBudget int
	RerankBudget  int
	IsDefault     bool

	// Quality
	RecallAtK float64

	// Latency
	AvgLatencyMs float64
	P50LatencyMs float64
	P95LatencyMs float64

	// Routing metrics (from searchByFDE)
	AvgRoutingMs float64

	// MaxSim metrics
	AvgMaxSimFetchMs   float64
	AvgMaxSimComputeMs float64

	// IO metrics
	AvgCentroidsSearched int
	AvgDocsToMaxSim      int
	AvgTokensLoaded      int
	AvgBytesLoaded       int64
	AvgMaxSimComparisons int64
}

func TestLOTTESweep(t *testing.T) {
	// Check if data exists
	if _, err := os.Stat(lotteDataDir + "/docs.bin"); os.IsNotExist(err) {
		t.Skip("LOTTE data not found at " + lotteDataDir)
	}

	// Load data
	fmt.Println("=== LOTTE Budget Sweep (Real Data) ===")
	data := loadLOTTEData(t)
	fmt.Printf("Loaded: %d docs, %d queries, dim=%d\n\n", len(data.Docs), len(data.Queries), data.Dim)

	// Create index
	fmt.Println("Creating HFresh+MUVERA index...")
	logger, _ := test.NewNullLogger()
	logger.SetLevel(logrus.WarnLevel)

	store := testinghelpers.NewDummyStore(t)
	cfg := DefaultConfig()
	cfg.RootPath = t.TempDir()
	cfg.ID = "lotte_sweep"

	distProvider := distancer.NewL2SquaredProvider()
	cfg.DistanceProvider = distProvider

	scheduler := queue.NewScheduler(queue.SchedulerOptions{Logger: logger})
	cfg.Scheduler = scheduler
	scheduler.Start()
	defer scheduler.Close(context.Background())

	cfg.Centroids.HNSWConfig = &hnsw.Config{
		RootPath:              t.TempDir(),
		ID:                    "lotte_sweep_centroids",
		MakeCommitLoggerThunk: makeNoopCommitLogger,
		GetViewThunk:          func() common.BucketView { return &noopBucketView{} },
		DistanceProvider:      distProvider,
		AllocChecker:          memwatch.NewDummyMonitor(),
		MakeBucketOptions:     lsmkv.MakeNoopBucketOptions,
	}
	cfg.TombstoneCallbacks = cyclemanager.NewCallbackGroupNoop()
	cfg.Logger = logger

	// Multi-vector store
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
	startIndex := time.Now()
	for i, doc := range data.Docs {
		mvStore.storeMultiVector(uint64(i), doc)
		err := index.AddMulti(context.Background(), uint64(i), doc)
		require.NoError(t, err)
		indexedCount.Add(1)
		if (i+1)%2000 == 0 {
			fmt.Printf("  Indexed %d/%d documents\n", i+1, len(data.Docs))
		}
	}
	fmt.Printf("Indexing completed in %v\n", time.Since(startIndex))

	// Wait for background tasks
	fmt.Println("Waiting for background tasks...")
	for index.taskQueue.Size() > 0 {
		time.Sleep(100 * time.Millisecond)
	}
	fmt.Println("Index ready.")

	// === BASELINE VALIDATION ===
	fmt.Println("\n=== BASELINE VALIDATION ===")
	fmt.Println("Running default config (k=10, routing=350, rerank=350)...")

	baselineRecall := runSingleConfig(t, index, data, 10, 350, 350)
	fmt.Printf("Baseline recall@10: %.4f\n", baselineRecall)

	// Check if baseline matches expectations
	// From previous real-server experiments, we expect ~30-40% recall
	if baselineRecall < 0.1 {
		t.Fatalf("BASELINE MISMATCH: recall@10=%.4f is too low. Expected ~30-40%%. "+
			"Check index configuration or data loading.", baselineRecall)
	}
	fmt.Printf("Baseline validation PASSED (recall@10=%.4f)\n", baselineRecall)

	// === FULL SWEEP ===
	fmt.Println("\n=== RUNNING FULL BUDGET SWEEP ===")

	outputFile := "/tmp/lotte_budget_sweep.csv"
	csvFile, err := os.Create(outputFile)
	require.NoError(t, err)
	defer csvFile.Close()

	writer := csv.NewWriter(csvFile)
	defer writer.Flush()

	// Header
	header := []string{
		"k", "routing_budget", "rerank_budget", "is_default",
		"recall_at_k",
		"avg_latency_ms", "p50_latency_ms", "p95_latency_ms",
		"routing_ms", "maxsim_fetch_ms", "maxsim_compute_ms",
		"docs_to_maxsim",
		"tokens_loaded", "bytes_loaded", "maxsim_comparisons",
	}
	writer.Write(header)

	kValues := []int{10, 50, 100}
	routingBudgets := []int{24, 32, 64, 96, 128, 192, 256, 350, 512, 1024}
	rerankBudgets := []int{24, 32, 64, 96, 128, 192, 256, 350, 512, 1024}

	for _, k := range kValues {
		fmt.Printf("\n--- k=%d ---\n", k)

		for _, routingBudget := range routingBudgets {
			for _, rerankBudget := range rerankBudgets {
				// Skip invalid configs
				if rerankBudget < k {
					continue
				}

				isDefault := routingBudget == 350 && rerankBudget == 350

				metrics := runSweepConfig(t, index, data, k, routingBudget, rerankBudget)
				metrics.IsDefault = isDefault

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
					fmt.Sprintf("%.4f", metrics.RecallAtK),
					fmt.Sprintf("%.2f", metrics.AvgLatencyMs),
					fmt.Sprintf("%.2f", metrics.P50LatencyMs),
					fmt.Sprintf("%.2f", metrics.P95LatencyMs),
					fmt.Sprintf("%.2f", metrics.AvgRoutingMs),
					fmt.Sprintf("%.2f", metrics.AvgMaxSimFetchMs),
					fmt.Sprintf("%.2f", metrics.AvgMaxSimComputeMs),
					strconv.Itoa(metrics.AvgDocsToMaxSim),
					strconv.Itoa(metrics.AvgTokensLoaded),
					strconv.FormatInt(metrics.AvgBytesLoaded, 10),
					strconv.FormatInt(metrics.AvgMaxSimComparisons, 10),
				}
				writer.Write(row)

				defaultMarker := ""
				if isDefault {
					defaultMarker = " [DEFAULT]"
				}
				fmt.Printf("routing=%d, rerank=%d: recall=%.3f, latency=%.1fms, docs_to_maxsim=%d%s\n",
					routingBudget, rerankBudget, metrics.RecallAtK, metrics.AvgLatencyMs,
					metrics.AvgDocsToMaxSim, defaultMarker)
			}
		}
	}

	writer.Flush()
	fmt.Printf("\n=== Results written to %s ===\n", outputFile)
}

func runSingleConfig(t *testing.T, index *HFresh, data *LOTTEData, k, routingBudget, rerankBudget int) float64 {
	var totalRecall float64
	numQueries := len(data.Queries)

	for qi, query := range data.Queries {
		ids, err := runSearchWithBudgets(index, query, k, routingBudget, rerankBudget)
		if err != nil {
			t.Fatalf("Search failed for query %d: %v", qi, err)
		}

		recall := computeRecallLOTTE(ids, data.GroundTruth[qi], k)
		totalRecall += recall
	}

	return totalRecall / float64(numQueries)
}

func runSweepConfig(t *testing.T, index *HFresh, data *LOTTEData, k, routingBudget, rerankBudget int) SweepMetrics {
	metrics := SweepMetrics{
		K:             k,
		RoutingBudget: routingBudget,
		RerankBudget:  rerankBudget,
	}

	numQueries := len(data.Queries)
	var latencies []float64
	var totalRecall float64
	var totalRoutingMs, totalMaxSimFetchMs, totalMaxSimComputeMs float64
	var totalDocsToMaxSim, totalTokensLoaded int
	var totalBytesLoaded, totalMaxSimComparisons int64

	for qi, query := range data.Queries {
		start := time.Now()

		routingStart := time.Now()
		queryFDE := index.muveraEncoder.EncodeQuery(query)
		candidateIDs, err := index.searchByFDE(context.Background(), queryFDE, routingBudget, rerankBudget, nil)
		routingDuration := time.Since(routingStart)

		if err != nil {
			t.Fatalf("searchByFDE failed: %v", err)
		}

		// MaxSim phase
		fetchStart := time.Now()
		docVectorsMap := make(map[uint64][][]float32)
		tokensLoaded := 0
		bytesLoaded := int64(0)
		for _, id := range candidateIDs {
			docVecs, err := index.multivectorForIdThunk(context.Background(), id)
			if err != nil {
				continue
			}
			docVectorsMap[id] = docVecs
			tokensLoaded += len(docVecs)
			if len(docVecs) > 0 {
				bytesLoaded += int64(len(docVecs) * len(docVecs[0]) * 4)
			}
		}
		fetchDuration := time.Since(fetchStart)

		computeStart := time.Now()
		results := NewResultSet(k)
		maxSimComparisons := int64(0)
		for id, docVecs := range docVectorsMap {
			score, _ := index.maxSimScore(query, docVecs)
			results.Insert(id, score)
			maxSimComparisons += int64(len(query) * len(docVecs))
		}
		computeDuration := time.Since(computeStart)

		totalDuration := time.Since(start)

		// Extract results
		ids := make([]uint64, 0, results.Len())
		for id := range results.Iter() {
			ids = append(ids, id)
		}

		recall := computeRecallLOTTE(ids, data.GroundTruth[qi], k)
		totalRecall += recall

		latencies = append(latencies, float64(totalDuration.Microseconds())/1000.0)
		totalRoutingMs += float64(routingDuration.Microseconds()) / 1000.0
		totalMaxSimFetchMs += float64(fetchDuration.Microseconds()) / 1000.0
		totalMaxSimComputeMs += float64(computeDuration.Microseconds()) / 1000.0
		totalDocsToMaxSim += len(candidateIDs)
		totalTokensLoaded += tokensLoaded
		totalBytesLoaded += bytesLoaded
		totalMaxSimComparisons += maxSimComparisons
	}

	n := float64(numQueries)
	metrics.RecallAtK = totalRecall / n
	metrics.AvgRoutingMs = totalRoutingMs / n
	metrics.AvgMaxSimFetchMs = totalMaxSimFetchMs / n
	metrics.AvgMaxSimComputeMs = totalMaxSimComputeMs / n
	metrics.AvgDocsToMaxSim = int(float64(totalDocsToMaxSim) / n)
	metrics.AvgTokensLoaded = int(float64(totalTokensLoaded) / n)
	metrics.AvgBytesLoaded = int64(float64(totalBytesLoaded) / n)
	metrics.AvgMaxSimComparisons = int64(float64(totalMaxSimComparisons) / n)

	// Compute latency percentiles
	sort.Float64s(latencies)
	metrics.AvgLatencyMs = 0
	for _, l := range latencies {
		metrics.AvgLatencyMs += l
	}
	metrics.AvgLatencyMs /= n
	metrics.P50LatencyMs = latencies[len(latencies)/2]
	metrics.P95LatencyMs = latencies[int(float64(len(latencies))*0.95)]

	return metrics
}

func runSearchWithBudgets(index *HFresh, query [][]float32, k, routingBudget, rerankBudget int) ([]uint64, error) {
	queryFDE := index.muveraEncoder.EncodeQuery(query)
	candidateIDs, err := index.searchByFDE(context.Background(), queryFDE, routingBudget, rerankBudget, nil)
	if err != nil {
		return nil, err
	}

	results := NewResultSet(k)
	for _, id := range candidateIDs {
		docVecs, err := index.multivectorForIdThunk(context.Background(), id)
		if err != nil {
			continue
		}
		score, _ := index.maxSimScore(query, docVecs)
		results.Insert(id, score)
	}

	ids := make([]uint64, 0, results.Len())
	for id := range results.Iter() {
		ids = append(ids, id)
	}
	return ids, nil
}

func computeRecallLOTTE(results []uint64, groundTruth []uint64, k int) float64 {
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

// Unused import suppression
var _ = io.EOF
var _ = math.MaxFloat32
var _ sync.Mutex

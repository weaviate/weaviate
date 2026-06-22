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

package hfresh

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/bits"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// =============================================================================
// REQUIRED KEEP PROPERTY EXPERIMENT
// =============================================================================
//
// Tests whether "requiredKeep is almost independent of dataset size" holds
// across datasets, scales, and embedding families.
//
// Two independent scaling studies:
// A. DBPedia/OpenAI: 100K -> 500K -> 1M (same embedding family)
// B. Sphere/Meta: 1M -> 10M (same embedding family)
//
// Do NOT compare across families - only within each series.
//
// =============================================================================

var (
	flagRKOutputDir   = flag.String("rk-out", "/tmp/embedding_required_keep_property", "Output directory")
	flagRKMaxQueries  = flag.Int("rk-max-queries", 500, "Maximum queries per dataset")
	flagRKK           = flag.Int("rk-k", 10, "K for recall@k")
	flagRKSeed        = flag.Uint64("rk-seed", 42, "Seed for RQ1 quantizer")
	flagRKDatasetsDir = flag.String("rk-datasets", "/Users/abdel/Documents/datasets", "Datasets directory")
)

// DatasetConfig defines a dataset to process
type DatasetConfig struct {
	Name       string
	Family     string // "dbpedia_openai" or "sphere_meta"
	Path       string
	BaseCount  int    // Expected base count for identification
	Metric     string // "angular", "euclidean", "dot"
}

// DatasetResult holds results for one dataset and method
type DatasetResult struct {
	Dataset           string  `json:"dataset"`
	Family            string  `json:"family"`
	BaseCount         int     `json:"base_count"`
	QueryCount        int     `json:"query_count"`
	Dimension         int     `json:"dimension"`
	Metric            string  `json:"metric"`
	Method            string  `json:"method"` // "float", "rq1_512", "rq1_768", "rq1_1024", "rq1_full"
	DimsUsed          int     `json:"dims_used"`
	SegmentsUsed      int     `json:"segments_used"`
	AvgRequiredKeep   float64 `json:"avg_required_keep"`
	P50RequiredKeep   int     `json:"p50_required_keep"`
	P90RequiredKeep   int     `json:"p90_required_keep"`
	P95RequiredKeep   int     `json:"p95_required_keep"`
	P99RequiredKeep   int     `json:"p99_required_keep"`
	MaxRequiredKeep   int     `json:"max_required_keep"`
	AvgKeepPct        float64 `json:"avg_keep_pct"`
	P95KeepPct        float64 `json:"p95_keep_pct"`
	MeanDWorstGT      float64 `json:"mean_d_worst_gt"`
	P50DWorstGT       float64 `json:"p50_d_worst_gt"`
	P95DWorstGT       float64 `json:"p95_d_worst_gt"`
	P50Ties           int     `json:"p50_ties"` // For RQ1 only
	P95Ties           int     `json:"p95_ties"` // For RQ1 only
	ElapsedSeconds    float64 `json:"elapsed_seconds"`
}

// RequiredKeepReport is the full experiment output
type RequiredKeepReport struct {
	Config struct {
		MaxQueries int    `json:"max_queries"`
		K          int    `json:"k"`
		Seed       uint64 `json:"seed"`
	} `json:"config"`

	Results   []DatasetResult `json:"results"`
	Timestamp string          `json:"timestamp"`

	// Scaling analysis
	DBPediaScaling []ScalingRow `json:"dbpedia_scaling"`
	SphereScaling  []ScalingRow `json:"sphere_scaling"`
}

// ScalingRow holds one row of the scaling analysis table
type ScalingRow struct {
	Dataset       string  `json:"dataset"`
	BaseCount     int     `json:"base_count"`
	Method        string  `json:"method"`
	P50Keep       int     `json:"p50_keep"`
	P95Keep       int     `json:"p95_keep"`
	P99Keep       int     `json:"p99_keep"`
	RatioVsPrev   float64 `json:"ratio_vs_prev"`   // P50 ratio vs previous N
	KeepPct       float64 `json:"keep_pct"`        // P50 as percentage of N
	ScalingAlpha  float64 `json:"scaling_alpha"`   // Estimated exponent
}

// HDF5Data holds loaded dataset
type HDF5Data struct {
	Train     [][]float32
	Test      [][]float32
	Neighbors [][]int
	Distances [][]float32 // May be nil
}

// TestRequiredKeepProperty runs the main experiment
func TestRequiredKeepProperty(t *testing.T) {
	if os.Getenv("HFRESH_BENCHMARK") == "" {
		t.Skip("Skipping required keep property benchmark test. Set HFRESH_BENCHMARK=1 to run.")
	}

	// Define datasets
	datasets := []DatasetConfig{
		// DBPedia/OpenAI series
		{
			Name:      "dbpedia-100k",
			Family:    "dbpedia_openai",
			Path:      filepath.Join(*flagRKDatasetsDir, "dbpedia-100k-openai-ada002.hdf5"),
			BaseCount: 100000,
			Metric:    "angular", // OpenAI embeddings are typically normalized
		},
		{
			Name:      "dbpedia-500k",
			Family:    "dbpedia_openai",
			Path:      filepath.Join(*flagRKDatasetsDir, "dbpedia-500k-openai-ada002.hdf5"),
			BaseCount: 500000,
			Metric:    "angular",
		},
		{
			Name:      "dbpedia-1m",
			Family:    "dbpedia_openai",
			Path:      filepath.Join(*flagRKDatasetsDir, "dbpedia-openai-1000k-angular.hdf5"),
			BaseCount: 990000,
			Metric:    "angular",
		},
		// Sphere/Meta series
		{
			Name:      "sphere-1m",
			Family:    "sphere_meta",
			Path:      filepath.Join(*flagRKDatasetsDir, "sphere-1M-meta-dpr.hdf5"),
			BaseCount: 1000000,
			Metric:    "dot", // DPR typically uses dot product
		},
		{
			Name:      "sphere-10m",
			Family:    "sphere_meta",
			Path:      filepath.Join(*flagRKDatasetsDir, "sphere-10M-meta-dpr.hdf5"),
			BaseCount: 10000000,
			Metric:    "dot",
		},
	}

	// Create output directory
	err := os.MkdirAll(*flagRKOutputDir, 0755)
	require.NoError(t, err)

	var allResults []DatasetResult

	for _, ds := range datasets {
		// Check if file exists
		if _, err := os.Stat(ds.Path); os.IsNotExist(err) {
			t.Logf("SKIPPING %s: file not found at %s", ds.Name, ds.Path)
			continue
		}

		t.Log("\n" + strings.Repeat("=", 60))
		t.Logf("Processing: %s (%s)", ds.Name, ds.Family)
		t.Log(strings.Repeat("=", 60))

		// Load dataset via Python helper
		data, err := loadHDF5Dataset(t, ds.Path, *flagRKMaxQueries)
		if err != nil {
			t.Logf("ERROR loading %s: %v", ds.Name, err)
			continue
		}

		dims := len(data.Train[0])
		t.Logf("Loaded: %d docs, %d queries, %d dims", len(data.Train), len(data.Test), dims)

		// Normalize if angular/cosine metric
		if ds.Metric == "angular" {
			t.Log("Normalizing vectors for angular metric...")
			normalizeVectors(data.Train)
			normalizeVectors(data.Test)
		}

		// Run FLOAT experiment
		t.Log("Running FLOAT experiment...")
		floatResult := runFloatExperiment(t, ds, data, *flagRKK)
		allResults = append(allResults, floatResult)

		// Run RQ1 experiments at various prefix lengths
		prefixDims := []int{512, 768, 1024, dims}
		for _, prefixDim := range prefixDims {
			if prefixDim > dims {
				continue
			}
			methodName := fmt.Sprintf("rq1_%d", prefixDim)
			if prefixDim == dims {
				methodName = "rq1_full"
			}
			t.Logf("Running %s experiment...", methodName)
			rq1Result := runRQ1Experiment(t, ds, data, *flagRKK, prefixDim, methodName)
			allResults = append(allResults, rq1Result)
		}
	}

	if len(allResults) == 0 {
		t.Fatal("No datasets were processed successfully")
	}

	// Build scaling analysis tables
	dbpediaScaling := buildScalingTable(allResults, "dbpedia_openai")
	sphereScaling := buildScalingTable(allResults, "sphere_meta")

	// Build report
	report := RequiredKeepReport{
		Results:        allResults,
		Timestamp:      time.Now().Format(time.RFC3339),
		DBPediaScaling: dbpediaScaling,
		SphereScaling:  sphereScaling,
	}
	report.Config.MaxQueries = *flagRKMaxQueries
	report.Config.K = *flagRKK
	report.Config.Seed = *flagRKSeed

	// Write outputs
	writeRequiredKeepOutputs(t, *flagRKOutputDir, report)

	// Print summary
	printRequiredKeepSummary(t, report)
}

// loadHDF5Dataset loads an HDF5 file using Python helper
func loadHDF5Dataset(t *testing.T, path string, maxQueries int) (*HDF5Data, error) {
	// Create temp output file
	tmpFile, err := os.CreateTemp("", "hdf5_data_*.json")
	if err != nil {
		return nil, err
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(tmpPath)

	// Create temp script file
	scriptFile, err := os.CreateTemp("", "load_hdf5_*.py")
	if err != nil {
		return nil, err
	}
	scriptPath := scriptFile.Name()
	defer os.Remove(scriptPath)

	// Python script to load HDF5 and output JSON
	script := fmt.Sprintf(`import h5py
import json
import numpy as np

path = %q
max_queries = %d
out_path = %q

with h5py.File(path, 'r') as f:
    # Identify train/base key
    train_key = None
    for k in ['train', 'base']:
        if k in f:
            train_key = k
            break
    if train_key is None:
        raise ValueError("No train/base key found")

    # Identify test/query key
    test_key = None
    for k in ['test', 'queries', 'query']:
        if k in f:
            test_key = k
            break
    if test_key is None:
        raise ValueError("No test/query key found")

    train = np.array(f[train_key][:], dtype=np.float32)
    test = np.array(f[test_key][:max_queries], dtype=np.float32)

    neighbors = None
    if 'neighbors' in f:
        neighbors = np.array(f['neighbors'][:max_queries], dtype=np.int32)

    data = {
        'train_shape': list(train.shape),
        'test_shape': list(test.shape),
        'neighbors_shape': list(neighbors.shape) if neighbors is not None else None,
        'train': train.tolist(),
        'test': test.tolist(),
        'neighbors': neighbors.tolist() if neighbors is not None else None,
    }

    with open(out_path, 'w') as out:
        json.dump(data, out)

    print(f"Loaded train: {train.shape}, test: {test.shape}")
`, path, maxQueries, tmpPath)

	if _, err := scriptFile.WriteString(script); err != nil {
		scriptFile.Close()
		return nil, err
	}
	scriptFile.Close()

	// Run Python
	cmd := exec.Command("bash", "-c", fmt.Sprintf("source /tmp/hdf5_venv/bin/activate && python3 %s", scriptPath))
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("python error: %v\nOutput: %s", err, string(output))
	}
	t.Log(strings.TrimSpace(string(output)))

	// Read JSON output
	jsonData, err := os.ReadFile(tmpPath)
	if err != nil {
		return nil, err
	}

	var raw struct {
		TrainShape     []int       `json:"train_shape"`
		TestShape      []int       `json:"test_shape"`
		NeighborsShape []int       `json:"neighbors_shape"`
		Train          [][]float32 `json:"train"`
		Test           [][]float32 `json:"test"`
		Neighbors      [][]int     `json:"neighbors"`
	}
	if err := json.Unmarshal(jsonData, &raw); err != nil {
		return nil, err
	}

	return &HDF5Data{
		Train:     raw.Train,
		Test:      raw.Test,
		Neighbors: raw.Neighbors,
	}, nil
}

// normalizeVectors normalizes each vector to unit length
func normalizeVectors(vecs [][]float32) {
	for i := range vecs {
		var sumSq float64
		for _, v := range vecs[i] {
			sumSq += float64(v) * float64(v)
		}
		norm := float32(math.Sqrt(sumSq))
		if norm > 0 {
			for j := range vecs[i] {
				vecs[i][j] /= norm
			}
		}
	}
}

// runFloatExperiment computes requiredKeep using exact float distances
func runFloatExperiment(t *testing.T, ds DatasetConfig, data *HDF5Data, k int) DatasetResult {
	start := time.Now()

	dims := len(data.Train[0])
	numDocs := len(data.Train)
	numQueries := len(data.Test)

	var requiredKeepVals []int
	var dWorstGTVals []float64

	for qi, query := range data.Test {
		if qi > 0 && qi%100 == 0 {
			t.Logf("  Query %d/%d...", qi, numQueries)
		}

		// Get GT neighbors
		gtIDs := data.Neighbors[qi]
		if len(gtIDs) > k {
			gtIDs = gtIDs[:k]
		}

		// Compute dWorstGT: max distance to any GT neighbor
		var dWorstGT float64
		for _, gtID := range gtIDs {
			if gtID < 0 || gtID >= numDocs {
				continue
			}
			dist := floatDistance(query, data.Train[gtID], ds.Metric)
			if dist > dWorstGT {
				dWorstGT = dist
			}
		}

		// Count docs within threshold
		requiredKeep := 0
		for _, doc := range data.Train {
			dist := floatDistance(query, doc, ds.Metric)
			if dist <= dWorstGT {
				requiredKeep++
			}
		}

		requiredKeepVals = append(requiredKeepVals, requiredKeep)
		dWorstGTVals = append(dWorstGTVals, dWorstGT)
	}

	elapsed := time.Since(start)

	sort.Ints(requiredKeepVals)
	sort.Float64s(dWorstGTVals)

	return DatasetResult{
		Dataset:         ds.Name,
		Family:          ds.Family,
		BaseCount:       numDocs,
		QueryCount:      numQueries,
		Dimension:       dims,
		Metric:          ds.Metric,
		Method:          "float",
		DimsUsed:        dims,
		SegmentsUsed:    0,
		AvgRequiredKeep: floatAvgInt(requiredKeepVals),
		P50RequiredKeep: intPercentile(requiredKeepVals, 0.50),
		P90RequiredKeep: intPercentile(requiredKeepVals, 0.90),
		P95RequiredKeep: intPercentile(requiredKeepVals, 0.95),
		P99RequiredKeep: intPercentile(requiredKeepVals, 0.99),
		MaxRequiredKeep: requiredKeepVals[len(requiredKeepVals)-1],
		AvgKeepPct:      floatAvgInt(requiredKeepVals) / float64(numDocs) * 100,
		P95KeepPct:      float64(intPercentile(requiredKeepVals, 0.95)) / float64(numDocs) * 100,
		MeanDWorstGT:    floatAvgFloat(dWorstGTVals),
		P50DWorstGT:     floatPercentile(dWorstGTVals, 0.50),
		P95DWorstGT:     floatPercentile(dWorstGTVals, 0.95),
		ElapsedSeconds:  elapsed.Seconds(),
	}
}

// runRQ1Experiment computes requiredKeep using RQ1 compressed distances
func runRQ1Experiment(t *testing.T, ds DatasetConfig, data *HDF5Data, k int, prefixDims int, methodName string) DatasetResult {
	start := time.Now()

	dims := len(data.Train[0])
	numDocs := len(data.Train)
	numQueries := len(data.Test)
	numSegments := (prefixDims + 63) / 64

	// Create RQ1 quantizer
	quantizer, err := compressionhelpers.NewBinaryRotationalQuantizer(dims, *flagRKSeed, distancer.NewL2SquaredProvider())
	if err != nil {
		t.Logf("Failed to create quantizer: %v", err)
		return DatasetResult{}
	}

	// Encode all documents
	docCodes := make([][]uint64, numDocs)
	for i, doc := range data.Train {
		docCodes[i] = quantizer.Encode(doc)
	}

	// Encode queries
	queryCodes := make([][]uint64, numQueries)
	for i, query := range data.Test {
		queryCodes[i] = quantizer.Encode(query)
	}

	var requiredKeepVals []int
	var tiesVals []int
	var dWorstGTVals []float64

	for qi := range data.Test {
		if qi > 0 && qi%100 == 0 {
			t.Logf("  Query %d/%d...", qi, numQueries)
		}

		qCode := queryCodes[qi]
		if qCode == nil {
			continue
		}

		// Get GT neighbors
		gtIDs := data.Neighbors[qi]
		if len(gtIDs) > k {
			gtIDs = gtIDs[:k]
		}

		// Compute threshold: max Hamming distance to any GT neighbor
		threshold := 0
		for _, gtID := range gtIDs {
			if gtID < 0 || gtID >= numDocs {
				continue
			}
			dist := hammingDistance(qCode, docCodes[gtID], numSegments)
			if dist > threshold {
				threshold = dist
			}
		}

		// Count docs within threshold
		requiredKeep := 0
		tiesAtThreshold := 0
		for _, dCode := range docCodes {
			if dCode == nil {
				continue
			}
			dist := hammingDistance(qCode, dCode, numSegments)
			if dist <= threshold {
				requiredKeep++
			}
			if dist == threshold {
				tiesAtThreshold++
			}
		}

		requiredKeepVals = append(requiredKeepVals, requiredKeep)
		tiesVals = append(tiesVals, tiesAtThreshold)
		dWorstGTVals = append(dWorstGTVals, float64(threshold))
	}

	elapsed := time.Since(start)

	sort.Ints(requiredKeepVals)
	sort.Ints(tiesVals)
	sort.Float64s(dWorstGTVals)

	return DatasetResult{
		Dataset:         ds.Name,
		Family:          ds.Family,
		BaseCount:       numDocs,
		QueryCount:      len(requiredKeepVals),
		Dimension:       dims,
		Metric:          ds.Metric,
		Method:          methodName,
		DimsUsed:        prefixDims,
		SegmentsUsed:    numSegments,
		AvgRequiredKeep: floatAvgInt(requiredKeepVals),
		P50RequiredKeep: intPercentile(requiredKeepVals, 0.50),
		P90RequiredKeep: intPercentile(requiredKeepVals, 0.90),
		P95RequiredKeep: intPercentile(requiredKeepVals, 0.95),
		P99RequiredKeep: intPercentile(requiredKeepVals, 0.99),
		MaxRequiredKeep: requiredKeepVals[len(requiredKeepVals)-1],
		AvgKeepPct:      floatAvgInt(requiredKeepVals) / float64(numDocs) * 100,
		P95KeepPct:      float64(intPercentile(requiredKeepVals, 0.95)) / float64(numDocs) * 100,
		MeanDWorstGT:    floatAvgFloat(dWorstGTVals),
		P50DWorstGT:     floatPercentile(dWorstGTVals, 0.50),
		P95DWorstGT:     floatPercentile(dWorstGTVals, 0.95),
		P50Ties:         intPercentile(tiesVals, 0.50),
		P95Ties:         intPercentile(tiesVals, 0.95),
		ElapsedSeconds:  elapsed.Seconds(),
	}
}

// floatDistance computes distance based on metric
func floatDistance(a, b []float32, metric string) float64 {
	switch metric {
	case "angular", "cosine":
		// For normalized vectors, angular distance = 1 - dot_product
		var dot float64
		for i := range a {
			dot += float64(a[i]) * float64(b[i])
		}
		return 1.0 - dot
	case "dot":
		// Negative dot product (so smaller = better)
		var dot float64
		for i := range a {
			dot += float64(a[i]) * float64(b[i])
		}
		return -dot
	default: // euclidean
		var sumSq float64
		for i := range a {
			diff := float64(a[i]) - float64(b[i])
			sumSq += diff * diff
		}
		return math.Sqrt(sumSq)
	}
}

// hammingDistance computes Hamming distance using first numSegments
func hammingDistance(qCode, dCode []uint64, numSegments int) int {
	// Bits start at index 1 (after header)
	qBits := qCode[1:]
	dBits := dCode[1:]

	if numSegments > len(qBits) {
		numSegments = len(qBits)
	}
	if numSegments > len(dBits) {
		numSegments = len(dBits)
	}

	dist := 0
	for i := 0; i < numSegments; i++ {
		dist += bits.OnesCount64(qBits[i] ^ dBits[i])
	}
	return dist
}

func floatAvgInt(vals []int) float64 {
	if len(vals) == 0 {
		return 0
	}
	sum := 0
	for _, v := range vals {
		sum += v
	}
	return float64(sum) / float64(len(vals))
}

func floatAvgFloat(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range vals {
		sum += v
	}
	return sum / float64(len(vals))
}

func floatPercentile(sortedVals []float64, p float64) float64 {
	if len(sortedVals) == 0 {
		return 0
	}
	idx := int(math.Ceil(p*float64(len(sortedVals)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sortedVals) {
		idx = len(sortedVals) - 1
	}
	return sortedVals[idx]
}

// buildScalingTable builds the scaling analysis table for a family
func buildScalingTable(results []DatasetResult, family string) []ScalingRow {
	// Filter and group by method
	byMethod := make(map[string][]DatasetResult)
	for _, r := range results {
		if r.Family == family {
			byMethod[r.Method] = append(byMethod[r.Method], r)
		}
	}

	var rows []ScalingRow

	for method, methodResults := range byMethod {
		// Sort by base count
		sort.Slice(methodResults, func(i, j int) bool {
			return methodResults[i].BaseCount < methodResults[j].BaseCount
		})

		var prevP50 int
		var prevN int
		for i, r := range methodResults {
			row := ScalingRow{
				Dataset:   r.Dataset,
				BaseCount: r.BaseCount,
				Method:    method,
				P50Keep:   r.P50RequiredKeep,
				P95Keep:   r.P95RequiredKeep,
				P99Keep:   r.P99RequiredKeep,
				KeepPct:   float64(r.P50RequiredKeep) / float64(r.BaseCount) * 100,
			}

			if i > 0 && prevP50 > 0 {
				row.RatioVsPrev = float64(r.P50RequiredKeep) / float64(prevP50)
				// Estimate alpha: keep ~ N^alpha
				// log(keep2/keep1) = alpha * log(N2/N1)
				// alpha = log(keep2/keep1) / log(N2/N1)
				if prevN > 0 {
					logKeepRatio := math.Log(float64(r.P50RequiredKeep) / float64(prevP50))
					logNRatio := math.Log(float64(r.BaseCount) / float64(prevN))
					if logNRatio > 0 {
						row.ScalingAlpha = logKeepRatio / logNRatio
					}
				}
			}

			rows = append(rows, row)
			prevP50 = r.P50RequiredKeep
			prevN = r.BaseCount
		}
	}

	return rows
}

func writeRequiredKeepOutputs(t *testing.T, outDir string, report RequiredKeepReport) {
	// Write float CSV
	csvPath := filepath.Join(outDir, "required_keep_float.csv")
	writeFloatCSV(t, csvPath, report.Results)

	// Write RQ1 CSV
	csvPath = filepath.Join(outDir, "required_keep_rq1.csv")
	writeRQ1CSV(t, csvPath, report.Results)

	// Write JSON
	jsonPath := filepath.Join(outDir, "required_keep_summary.json")
	jsonData, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Errorf("Failed to marshal JSON: %v", err)
	} else {
		if err := os.WriteFile(jsonPath, jsonData, 0644); err != nil {
			t.Errorf("Failed to write JSON: %v", err)
		} else {
			t.Logf("JSON written to: %s", jsonPath)
		}
	}

	// Write Markdown
	mdPath := filepath.Join(outDir, "required_keep_summary.md")
	writeRequiredKeepMarkdown(t, mdPath, report)
}

func writeFloatCSV(t *testing.T, path string, results []DatasetResult) {
	f, err := os.Create(path)
	if err != nil {
		t.Errorf("Failed to create CSV: %v", err)
		return
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	header := []string{"dataset", "family", "base_count", "query_count", "dimension", "metric",
		"avg_keep", "p50_keep", "p90_keep", "p95_keep", "p99_keep", "max_keep",
		"avg_keep_pct", "p95_keep_pct", "mean_d_worst", "p50_d_worst", "p95_d_worst", "elapsed_sec"}
	w.Write(header)

	for _, r := range results {
		if r.Method != "float" {
			continue
		}
		row := []string{
			r.Dataset, r.Family, strconv.Itoa(r.BaseCount), strconv.Itoa(r.QueryCount),
			strconv.Itoa(r.Dimension), r.Metric,
			fmt.Sprintf("%.1f", r.AvgRequiredKeep),
			strconv.Itoa(r.P50RequiredKeep), strconv.Itoa(r.P90RequiredKeep),
			strconv.Itoa(r.P95RequiredKeep), strconv.Itoa(r.P99RequiredKeep),
			strconv.Itoa(r.MaxRequiredKeep),
			fmt.Sprintf("%.4f", r.AvgKeepPct), fmt.Sprintf("%.4f", r.P95KeepPct),
			fmt.Sprintf("%.6f", r.MeanDWorstGT), fmt.Sprintf("%.6f", r.P50DWorstGT),
			fmt.Sprintf("%.6f", r.P95DWorstGT), fmt.Sprintf("%.1f", r.ElapsedSeconds),
		}
		w.Write(row)
	}

	t.Logf("Float CSV written to: %s", path)
}

func writeRQ1CSV(t *testing.T, path string, results []DatasetResult) {
	f, err := os.Create(path)
	if err != nil {
		t.Errorf("Failed to create CSV: %v", err)
		return
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	header := []string{"dataset", "family", "base_count", "query_count", "dimension", "metric",
		"method", "dims_used", "segments_used",
		"avg_keep", "p50_keep", "p90_keep", "p95_keep", "p99_keep", "max_keep",
		"avg_keep_pct", "p95_keep_pct", "p50_ties", "p95_ties", "elapsed_sec"}
	w.Write(header)

	for _, r := range results {
		if r.Method == "float" {
			continue
		}
		row := []string{
			r.Dataset, r.Family, strconv.Itoa(r.BaseCount), strconv.Itoa(r.QueryCount),
			strconv.Itoa(r.Dimension), r.Metric, r.Method,
			strconv.Itoa(r.DimsUsed), strconv.Itoa(r.SegmentsUsed),
			fmt.Sprintf("%.1f", r.AvgRequiredKeep),
			strconv.Itoa(r.P50RequiredKeep), strconv.Itoa(r.P90RequiredKeep),
			strconv.Itoa(r.P95RequiredKeep), strconv.Itoa(r.P99RequiredKeep),
			strconv.Itoa(r.MaxRequiredKeep),
			fmt.Sprintf("%.4f", r.AvgKeepPct), fmt.Sprintf("%.4f", r.P95KeepPct),
			strconv.Itoa(r.P50Ties), strconv.Itoa(r.P95Ties),
			fmt.Sprintf("%.1f", r.ElapsedSeconds),
		}
		w.Write(row)
	}

	t.Logf("RQ1 CSV written to: %s", path)
}

func writeRequiredKeepMarkdown(t *testing.T, path string, report RequiredKeepReport) {
	f, err := os.Create(path)
	if err != nil {
		t.Errorf("Failed to create markdown: %v", err)
		return
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	defer w.Flush()

	fmt.Fprintf(w, "# Required Keep Property Experiment\n\n")
	fmt.Fprintf(w, "**Date**: %s\n\n", report.Timestamp)
	fmt.Fprintf(w, "## Overview\n\n")
	fmt.Fprintf(w, "Tests whether `requiredKeep` is almost independent of dataset size.\n\n")
	fmt.Fprintf(w, "**Two independent scaling studies:**\n")
	fmt.Fprintf(w, "- A. DBPedia/OpenAI: 100K → 500K → 1M (same embedding family)\n")
	fmt.Fprintf(w, "- B. Sphere/Meta: 1M → 10M (same embedding family)\n\n")
	fmt.Fprintf(w, "**Do NOT compare across families** - only within each series.\n\n")

	fmt.Fprintf(w, "## Configuration\n\n")
	fmt.Fprintf(w, "| Parameter | Value |\n")
	fmt.Fprintf(w, "|-----------|-------|\n")
	fmt.Fprintf(w, "| Max Queries | %d |\n", report.Config.MaxQueries)
	fmt.Fprintf(w, "| K | %d |\n", report.Config.K)
	fmt.Fprintf(w, "| RQ1 Seed | %d |\n\n", report.Config.Seed)

	// Results by family
	fmt.Fprintf(w, "## Results by Family\n\n")

	families := []string{"dbpedia_openai", "sphere_meta"}
	familyNames := map[string]string{"dbpedia_openai": "DBPedia/OpenAI", "sphere_meta": "Sphere/Meta"}

	for _, family := range families {
		fmt.Fprintf(w, "### %s\n\n", familyNames[family])

		// Float results
		fmt.Fprintf(w, "#### FLOAT (Exact Distance)\n\n")
		fmt.Fprintf(w, "| Dataset | N | P50 Keep | P95 Keep | P99 Keep | Max | Keep%% |\n")
		fmt.Fprintf(w, "|---------|---|----------|----------|----------|-----|-------|\n")
		for _, r := range report.Results {
			if r.Family == family && r.Method == "float" {
				fmt.Fprintf(w, "| %s | %d | %d | %d | %d | %d | %.4f%% |\n",
					r.Dataset, r.BaseCount, r.P50RequiredKeep, r.P95RequiredKeep,
					r.P99RequiredKeep, r.MaxRequiredKeep, r.AvgKeepPct)
			}
		}

		// RQ1 results
		fmt.Fprintf(w, "\n#### RQ1 (Compressed)\n\n")
		fmt.Fprintf(w, "| Dataset | N | Method | Dims | P50 Keep | P95 Keep | P99 Keep | P95 Ties |\n")
		fmt.Fprintf(w, "|---------|---|--------|------|----------|----------|----------|----------|\n")
		for _, r := range report.Results {
			if r.Family == family && r.Method != "float" {
				fmt.Fprintf(w, "| %s | %d | %s | %d | %d | %d | %d | %d |\n",
					r.Dataset, r.BaseCount, r.Method, r.DimsUsed,
					r.P50RequiredKeep, r.P95RequiredKeep, r.P99RequiredKeep, r.P95Ties)
			}
		}
		fmt.Fprintf(w, "\n")
	}

	// Scaling analysis
	fmt.Fprintf(w, "## Scaling Analysis\n\n")

	if len(report.DBPediaScaling) > 0 {
		fmt.Fprintf(w, "### A. DBPedia/OpenAI Scaling (100K → 500K → 1M)\n\n")
		fmt.Fprintf(w, "*This is scale-only within DBPedia/OpenAI.*\n\n")
		fmt.Fprintf(w, "| Dataset | N | Method | P50 Keep | P95 Keep | Ratio vs Prev | Keep%% | Alpha |\n")
		fmt.Fprintf(w, "|---------|---|--------|----------|----------|---------------|-------|-------|\n")
		for _, row := range report.DBPediaScaling {
			ratioStr := "-"
			alphaStr := "-"
			if row.RatioVsPrev > 0 {
				ratioStr = fmt.Sprintf("%.2fx", row.RatioVsPrev)
			}
			if row.ScalingAlpha != 0 {
				alphaStr = fmt.Sprintf("%.3f", row.ScalingAlpha)
			}
			fmt.Fprintf(w, "| %s | %d | %s | %d | %d | %s | %.4f%% | %s |\n",
				row.Dataset, row.BaseCount, row.Method, row.P50Keep, row.P95Keep,
				ratioStr, row.KeepPct, alphaStr)
		}
		fmt.Fprintf(w, "\n")
	}

	if len(report.SphereScaling) > 0 {
		fmt.Fprintf(w, "### B. Sphere/Meta Scaling (1M → 10M)\n\n")
		fmt.Fprintf(w, "*This is scale-only within Sphere/Meta.*\n\n")
		fmt.Fprintf(w, "| Dataset | N | Method | P50 Keep | P95 Keep | Ratio vs Prev | Keep%% | Alpha |\n")
		fmt.Fprintf(w, "|---------|---|--------|----------|----------|---------------|-------|-------|\n")
		for _, row := range report.SphereScaling {
			ratioStr := "-"
			alphaStr := "-"
			if row.RatioVsPrev > 0 {
				ratioStr = fmt.Sprintf("%.2fx", row.RatioVsPrev)
			}
			if row.ScalingAlpha != 0 {
				alphaStr = fmt.Sprintf("%.3f", row.ScalingAlpha)
			}
			fmt.Fprintf(w, "| %s | %d | %s | %d | %d | %s | %.4f%% | %s |\n",
				row.Dataset, row.BaseCount, row.Method, row.P50Keep, row.P95Keep,
				ratioStr, row.KeepPct, alphaStr)
		}
		fmt.Fprintf(w, "\n")
	}

	// Decision summary
	fmt.Fprintf(w, "## Decision Summary\n\n")
	fmt.Fprintf(w, "**Interpretation of Alpha (scaling exponent):**\n")
	fmt.Fprintf(w, "- α ≈ 0: constant (requiredKeep independent of N)\n")
	fmt.Fprintf(w, "- α < 0.3: strongly sublinear\n")
	fmt.Fprintf(w, "- α ≈ 0.5: sqrt-like\n")
	fmt.Fprintf(w, "- α ≈ 1: linear\n\n")

	fmt.Fprintf(w, "**Key Questions:**\n")
	fmt.Fprintf(w, "1. Does FLOAT requiredKeep scale sublinearly within DBPedia/OpenAI?\n")
	fmt.Fprintf(w, "2. Does FLOAT requiredKeep scale sublinearly within Sphere/Meta?\n")
	fmt.Fprintf(w, "3. Does RQ1 preserve the same scaling shape as FLOAT?\n")
	fmt.Fprintf(w, "4. Is the property geometric (FLOAT shows it) or compression-induced (only RQ1 shows it)?\n\n")

	t.Logf("Markdown written to: %s", path)
}

func printRequiredKeepSummary(t *testing.T, report RequiredKeepReport) {
	t.Log("\n" + strings.Repeat("=", 80))
	t.Log("REQUIRED KEEP PROPERTY SUMMARY")
	t.Log(strings.Repeat("=", 80))

	// Group results by family
	for _, family := range []string{"dbpedia_openai", "sphere_meta"} {
		familyName := map[string]string{"dbpedia_openai": "DBPedia/OpenAI", "sphere_meta": "Sphere/Meta"}[family]

		hasData := false
		for _, r := range report.Results {
			if r.Family == family {
				hasData = true
				break
			}
		}
		if !hasData {
			t.Logf("\n--- %s: NO DATA ---", familyName)
			continue
		}

		t.Logf("\n--- %s ---", familyName)
		t.Log("Dataset        | N        | Method    | P50 Keep  | P95 Keep  | Keep%")
		t.Log("---------------|----------|-----------|-----------|-----------|------")

		for _, r := range report.Results {
			if r.Family == family {
				t.Logf("%-14s | %8d | %-9s | %9d | %9d | %.4f%%",
					r.Dataset, r.BaseCount, r.Method, r.P50RequiredKeep, r.P95RequiredKeep, r.AvgKeepPct)
			}
		}
	}

	t.Log("\n" + strings.Repeat("=", 80))
}

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
// NEIGHBORHOOD DENSITY EXPERIMENT
// =============================================================================
//
// Corrects the previous RequiredKeepProperty experiment.
//
// The previous FLOAT experiment measured:
//   requiredKeep = count(v where dist(q,v) <= dWorstGT)
// which is trivially K (modulo ties) because GT is defined in float space.
//
// This experiment measures neighborhood DENSITY:
//   "How many vectors live near the GT neighborhood as N grows?"
//
// Experiment A: Radius expansion in FLOAT space
//   - Measure keep at 1.00x, 1.01x, 1.02x, 1.05x, 1.10x of dWorstGT
//
// Experiment B: RQ1 compressed distances
//   - Measure requiredKeepRQ1 at various prefix lengths
//
// Experiment C: Scaling law
//   - Fit keep = C * N^alpha for each radius/method
//
// =============================================================================

var (
	flagNDOutputDir   = flag.String("nd-out", "/tmp/neighborhood_density", "Output directory")
	flagNDMaxQueries  = flag.Int("nd-max-queries", 500, "Maximum queries per dataset")
	flagNDK           = flag.Int("nd-k", 10, "K for recall@k")
	flagNDSeed        = flag.Uint64("nd-seed", 42, "Seed for RQ1 quantizer")
	flagNDDatasetsDir = flag.String("nd-datasets", "/Users/abdel/Documents/datasets", "Datasets directory")
)

// Radius multipliers for Experiment A
var radiusMultipliers = []float64{1.00, 1.01, 1.02, 1.05, 1.10}

// NDDatasetConfig defines a dataset to process
type NDDatasetConfig struct {
	Name      string
	Family    string // "dbpedia_openai" or "sphere_meta"
	Path      string
	BaseCount int
	Metric    string // "angular", "euclidean", "dot"
}

// RadiusResult holds results for one radius multiplier
type RadiusResult struct {
	Dataset    string  `json:"dataset"`
	Family     string  `json:"family"`
	BaseCount  int     `json:"base_count"`
	QueryCount int     `json:"query_count"`
	Dimension  int     `json:"dimension"`
	Metric     string  `json:"metric"`
	Radius     float64 `json:"radius"` // 1.00, 1.01, 1.02, 1.05, 1.10
	Method     string  `json:"method"` // "float" or "rq1_xxx"
	DimsUsed   int     `json:"dims_used"`

	AvgKeep float64 `json:"avg_keep"`
	P50Keep int     `json:"p50_keep"`
	P90Keep int     `json:"p90_keep"`
	P95Keep int     `json:"p95_keep"`
	P99Keep int     `json:"p99_keep"`
	MaxKeep int     `json:"max_keep"`

	AvgKeepPct float64 `json:"avg_keep_pct"`
	P95KeepPct float64 `json:"p95_keep_pct"`

	ElapsedSeconds float64 `json:"elapsed_seconds"`
}

// ScalingFit holds the scaling law fit results
type ScalingFit struct {
	Family string  `json:"family"`
	Method string  `json:"method"` // "float_1.01x", "float_1.02x", etc. or "rq1_full"
	Radius float64 `json:"radius"` // 0 for RQ1
	Alpha  float64 `json:"alpha"`  // Scaling exponent
	C      float64 `json:"c"`      // Coefficient
	R2     float64 `json:"r2"`     // Goodness of fit
}

// NDReport is the full experiment output
type NDReport struct {
	Config struct {
		MaxQueries int     `json:"max_queries"`
		K          int     `json:"k"`
		Seed       uint64  `json:"seed"`
		Radii      []float64 `json:"radii"`
	} `json:"config"`

	Results      []RadiusResult `json:"results"`
	ScalingFits  []ScalingFit   `json:"scaling_fits"`
	Timestamp    string         `json:"timestamp"`
}

// NDHDFData holds loaded dataset
type NDHDFData struct {
	Train     [][]float32
	Test      [][]float32
	Neighbors [][]int
}

func TestNeighborhoodDensity(t *testing.T) {
	// Define datasets
	datasets := []NDDatasetConfig{
		// DBPedia/OpenAI series
		{
			Name:      "dbpedia-100k",
			Family:    "dbpedia_openai",
			Path:      filepath.Join(*flagNDDatasetsDir, "dbpedia-100k-openai-ada002.hdf5"),
			BaseCount: 100000,
			Metric:    "angular",
		},
		{
			Name:      "dbpedia-500k",
			Family:    "dbpedia_openai",
			Path:      filepath.Join(*flagNDDatasetsDir, "dbpedia-500k-openai-ada002.hdf5"),
			BaseCount: 500000,
			Metric:    "angular",
		},
		{
			Name:      "dbpedia-1m",
			Family:    "dbpedia_openai",
			Path:      filepath.Join(*flagNDDatasetsDir, "dbpedia-openai-1000k-angular.hdf5"),
			BaseCount: 990000,
			Metric:    "angular",
		},
		// Sphere/Meta series
		{
			Name:      "sphere-1m",
			Family:    "sphere_meta",
			Path:      filepath.Join(*flagNDDatasetsDir, "sphere-1M-meta-dpr.hdf5"),
			BaseCount: 1000000,
			Metric:    "angular", // Use angular (cosine) for consistency
		},
		// sphere-10m skipped by default due to memory
	}

	// Create output directory
	err := os.MkdirAll(*flagNDOutputDir, 0755)
	require.NoError(t, err)

	var allResults []RadiusResult

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
		data, err := loadNDHDF5Dataset(t, ds.Path, *flagNDMaxQueries)
		if err != nil {
			t.Logf("ERROR loading %s: %v", ds.Name, err)
			continue
		}

		dims := len(data.Train[0])
		t.Logf("Loaded: %d docs, %d queries, %d dims", len(data.Train), len(data.Test), dims)

		// Normalize vectors for angular/cosine metric
		t.Log("Normalizing vectors...")
		ndNormalizeVectors(data.Train)
		ndNormalizeVectors(data.Test)

		// Experiment A: Radius expansion in FLOAT space
		t.Log("Running Experiment A: Radius expansion...")
		floatResults := runRadiusExpansionExperiment(t, ds, data, *flagNDK)
		allResults = append(allResults, floatResults...)

		// Experiment B: RQ1 compressed distances
		t.Log("Running Experiment B: RQ1 compressed...")
		rq1Results := runRQ1NeighborhoodExperiment(t, ds, data, *flagNDK)
		allResults = append(allResults, rq1Results...)
	}

	if len(allResults) == 0 {
		t.Fatal("No datasets were processed successfully")
	}

	// Experiment C: Scaling law fits
	t.Log("Running Experiment C: Scaling law fits...")
	scalingFits := computeScalingFits(allResults)

	// Build report
	report := NDReport{
		Results:     allResults,
		ScalingFits: scalingFits,
		Timestamp:   time.Now().Format(time.RFC3339),
	}
	report.Config.MaxQueries = *flagNDMaxQueries
	report.Config.K = *flagNDK
	report.Config.Seed = *flagNDSeed
	report.Config.Radii = radiusMultipliers

	// Write outputs
	writeNDOutputs(t, *flagNDOutputDir, report)

	// Print summary
	printNDSummary(t, report)
}

// loadNDHDF5Dataset loads an HDF5 file using Python helper
func loadNDHDF5Dataset(t *testing.T, path string, maxQueries int) (*NDHDFData, error) {
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

	return &NDHDFData{
		Train:     raw.Train,
		Test:      raw.Test,
		Neighbors: raw.Neighbors,
	}, nil
}

// ndNormalizeVectors normalizes each vector to unit length
func ndNormalizeVectors(vecs [][]float32) {
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

// runRadiusExpansionExperiment runs Experiment A
func runRadiusExpansionExperiment(t *testing.T, ds NDDatasetConfig, data *NDHDFData, k int) []RadiusResult {
	start := time.Now()

	dims := len(data.Train[0])
	numDocs := len(data.Train)
	numQueries := len(data.Test)

	// Results per radius multiplier
	keepVals := make(map[float64][]int)
	for _, r := range radiusMultipliers {
		keepVals[r] = make([]int, 0, numQueries)
	}

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
			dist := ndFloatDistance(query, data.Train[gtID])
			if dist > dWorstGT {
				dWorstGT = dist
			}
		}

		// Count vectors within each radius
		for _, radius := range radiusMultipliers {
			threshold := dWorstGT * radius
			count := 0
			for _, doc := range data.Train {
				dist := ndFloatDistance(query, doc)
				if dist <= threshold {
					count++
				}
			}
			keepVals[radius] = append(keepVals[radius], count)
		}
	}

	elapsed := time.Since(start)

	// Build results
	var results []RadiusResult
	for _, radius := range radiusMultipliers {
		vals := keepVals[radius]
		sort.Ints(vals)

		result := RadiusResult{
			Dataset:        ds.Name,
			Family:         ds.Family,
			BaseCount:      numDocs,
			QueryCount:     numQueries,
			Dimension:      dims,
			Metric:         ds.Metric,
			Radius:         radius,
			Method:         "float",
			DimsUsed:       dims,
			AvgKeep:        ndFloatAvgInt(vals),
			P50Keep:        ndIntPercentile(vals, 0.50),
			P90Keep:        ndIntPercentile(vals, 0.90),
			P95Keep:        ndIntPercentile(vals, 0.95),
			P99Keep:        ndIntPercentile(vals, 0.99),
			MaxKeep:        vals[len(vals)-1],
			AvgKeepPct:     ndFloatAvgInt(vals) / float64(numDocs) * 100,
			P95KeepPct:     float64(ndIntPercentile(vals, 0.95)) / float64(numDocs) * 100,
			ElapsedSeconds: elapsed.Seconds() / float64(len(radiusMultipliers)),
		}
		results = append(results, result)

		t.Logf("  Radius %.2fx: P50=%d, P95=%d, Keep%%=%.4f%%",
			radius, result.P50Keep, result.P95Keep, result.AvgKeepPct)
	}

	return results
}

// runRQ1NeighborhoodExperiment runs Experiment B
func runRQ1NeighborhoodExperiment(t *testing.T, ds NDDatasetConfig, data *NDHDFData, k int) []RadiusResult {
	dims := len(data.Train[0])
	numDocs := len(data.Train)
	numQueries := len(data.Test)

	// Create RQ1 quantizer
	quantizer, err := compressionhelpers.NewBinaryRotationalQuantizer(dims, *flagNDSeed, distancer.NewL2SquaredProvider())
	if err != nil {
		t.Logf("Failed to create quantizer: %v", err)
		return nil
	}

	// Encode all documents
	t.Log("  Encoding documents...")
	docCodes := make([][]uint64, numDocs)
	for i, doc := range data.Train {
		docCodes[i] = quantizer.Encode(doc)
	}

	// Encode queries
	queryCodes := make([][]uint64, numQueries)
	for i, query := range data.Test {
		queryCodes[i] = quantizer.Encode(query)
	}

	// Prefix lengths to test
	prefixDims := []int{512, 768, 1024, dims}
	var results []RadiusResult

	for _, prefixDim := range prefixDims {
		if prefixDim > dims {
			continue
		}

		start := time.Now()
		methodName := fmt.Sprintf("rq1_%d", prefixDim)
		if prefixDim == dims {
			methodName = "rq1_full"
		}

		numSegments := (prefixDim + 63) / 64

		var keepVals []int

		for qi := range data.Test {
			if qi > 0 && qi%100 == 0 {
				t.Logf("  %s Query %d/%d...", methodName, qi, numQueries)
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
				dist := ndHammingDistance(qCode, docCodes[gtID], numSegments)
				if dist > threshold {
					threshold = dist
				}
			}

			// Count docs within threshold
			count := 0
			for _, dCode := range docCodes {
				if dCode == nil {
					continue
				}
				dist := ndHammingDistance(qCode, dCode, numSegments)
				if dist <= threshold {
					count++
				}
			}

			keepVals = append(keepVals, count)
		}

		elapsed := time.Since(start)
		sort.Ints(keepVals)

		result := RadiusResult{
			Dataset:        ds.Name,
			Family:         ds.Family,
			BaseCount:      numDocs,
			QueryCount:     len(keepVals),
			Dimension:      dims,
			Metric:         ds.Metric,
			Radius:         1.0, // Not applicable for RQ1
			Method:         methodName,
			DimsUsed:       prefixDim,
			AvgKeep:        ndFloatAvgInt(keepVals),
			P50Keep:        ndIntPercentile(keepVals, 0.50),
			P90Keep:        ndIntPercentile(keepVals, 0.90),
			P95Keep:        ndIntPercentile(keepVals, 0.95),
			P99Keep:        ndIntPercentile(keepVals, 0.99),
			MaxKeep:        keepVals[len(keepVals)-1],
			AvgKeepPct:     ndFloatAvgInt(keepVals) / float64(numDocs) * 100,
			P95KeepPct:     float64(ndIntPercentile(keepVals, 0.95)) / float64(numDocs) * 100,
			ElapsedSeconds: elapsed.Seconds(),
		}
		results = append(results, result)

		t.Logf("  %s: P50=%d, P95=%d, Keep%%=%.4f%%",
			methodName, result.P50Keep, result.P95Keep, result.AvgKeepPct)
	}

	return results
}

// ndFloatDistance computes angular distance (1 - cos_sim) for normalized vectors
func ndFloatDistance(a, b []float32) float64 {
	var dot float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
	}
	return 1.0 - dot
}

// ndHammingDistance computes Hamming distance using first numSegments
func ndHammingDistance(qCode, dCode []uint64, numSegments int) int {
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

// computeScalingFits computes scaling law fits for Experiment C
func computeScalingFits(results []RadiusResult) []ScalingFit {
	var fits []ScalingFit

	// Group by family
	families := []string{"dbpedia_openai", "sphere_meta"}

	for _, family := range families {
		// Get all results for this family
		familyResults := make([]RadiusResult, 0)
		for _, r := range results {
			if r.Family == family {
				familyResults = append(familyResults, r)
			}
		}
		if len(familyResults) == 0 {
			continue
		}

		// Fit for each FLOAT radius (except 1.00x which is trivially K)
		for _, radius := range radiusMultipliers {
			if radius == 1.00 {
				continue // Skip 1.00x as it's trivially K
			}

			// Collect (N, keep) pairs for this radius
			var nVals, keepVals []float64
			for _, r := range familyResults {
				if r.Method == "float" && r.Radius == radius {
					nVals = append(nVals, float64(r.BaseCount))
					keepVals = append(keepVals, float64(r.P50Keep))
				}
			}

			if len(nVals) >= 2 {
				alpha, c, r2 := fitPowerLaw(nVals, keepVals)
				fits = append(fits, ScalingFit{
					Family: family,
					Method: fmt.Sprintf("float_%.2fx", radius),
					Radius: radius,
					Alpha:  alpha,
					C:      c,
					R2:     r2,
				})
			}
		}

		// Fit for RQ1_full
		var nVals, keepVals []float64
		for _, r := range familyResults {
			if r.Method == "rq1_full" {
				nVals = append(nVals, float64(r.BaseCount))
				keepVals = append(keepVals, float64(r.P50Keep))
			}
		}

		if len(nVals) >= 2 {
			alpha, c, r2 := fitPowerLaw(nVals, keepVals)
			fits = append(fits, ScalingFit{
				Family: family,
				Method: "rq1_full",
				Radius: 0,
				Alpha:  alpha,
				C:      c,
				R2:     r2,
			})
		}
	}

	return fits
}

// fitPowerLaw fits keep = C * N^alpha using log-linear regression
func fitPowerLaw(n, keep []float64) (alpha, c, r2 float64) {
	if len(n) < 2 {
		return 0, 0, 0
	}

	// Transform to log space
	logN := make([]float64, len(n))
	logKeep := make([]float64, len(keep))
	for i := range n {
		logN[i] = math.Log(n[i])
		if keep[i] > 0 {
			logKeep[i] = math.Log(keep[i])
		} else {
			logKeep[i] = 0
		}
	}

	// Linear regression: log(keep) = log(C) + alpha * log(N)
	// y = a + b*x where y=log(keep), x=log(N), b=alpha, a=log(C)
	meanX := mean(logN)
	meanY := mean(logKeep)

	var ssXY, ssXX, ssYY float64
	for i := range logN {
		dx := logN[i] - meanX
		dy := logKeep[i] - meanY
		ssXY += dx * dy
		ssXX += dx * dx
		ssYY += dy * dy
	}

	if ssXX == 0 {
		return 0, 0, 0
	}

	alpha = ssXY / ssXX
	logC := meanY - alpha*meanX
	c = math.Exp(logC)

	// R-squared
	if ssYY > 0 {
		r2 = (ssXY * ssXY) / (ssXX * ssYY)
	}

	return alpha, c, r2
}

func mean(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range vals {
		sum += v
	}
	return sum / float64(len(vals))
}

func ndFloatAvgInt(vals []int) float64 {
	if len(vals) == 0 {
		return 0
	}
	sum := 0
	for _, v := range vals {
		sum += v
	}
	return float64(sum) / float64(len(vals))
}

func ndIntPercentile(sortedVals []int, p float64) int {
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

func writeNDOutputs(t *testing.T, outDir string, report NDReport) {
	// Write CSV
	csvPath := filepath.Join(outDir, "neighborhood_density.csv")
	writeNDCSV(t, csvPath, report.Results)

	// Write JSON
	jsonPath := filepath.Join(outDir, "neighborhood_density.json")
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
	mdPath := filepath.Join(outDir, "neighborhood_density.md")
	writeNDMarkdown(t, mdPath, report)
}

func writeNDCSV(t *testing.T, path string, results []RadiusResult) {
	f, err := os.Create(path)
	if err != nil {
		t.Errorf("Failed to create CSV: %v", err)
		return
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	header := []string{"dataset", "family", "base_count", "query_count", "dimension", "metric",
		"method", "radius", "dims_used",
		"avg_keep", "p50_keep", "p90_keep", "p95_keep", "p99_keep", "max_keep",
		"avg_keep_pct", "p95_keep_pct", "elapsed_sec"}
	w.Write(header)

	for _, r := range results {
		row := []string{
			r.Dataset, r.Family, strconv.Itoa(r.BaseCount), strconv.Itoa(r.QueryCount),
			strconv.Itoa(r.Dimension), r.Metric, r.Method,
			fmt.Sprintf("%.2f", r.Radius), strconv.Itoa(r.DimsUsed),
			fmt.Sprintf("%.1f", r.AvgKeep),
			strconv.Itoa(r.P50Keep), strconv.Itoa(r.P90Keep),
			strconv.Itoa(r.P95Keep), strconv.Itoa(r.P99Keep),
			strconv.Itoa(r.MaxKeep),
			fmt.Sprintf("%.4f", r.AvgKeepPct), fmt.Sprintf("%.4f", r.P95KeepPct),
			fmt.Sprintf("%.1f", r.ElapsedSeconds),
		}
		w.Write(row)
	}

	t.Logf("CSV written to: %s", path)
}

func writeNDMarkdown(t *testing.T, path string, report NDReport) {
	f, err := os.Create(path)
	if err != nil {
		t.Errorf("Failed to create markdown: %v", err)
		return
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	defer w.Flush()

	fmt.Fprintf(w, "# Neighborhood Density Experiment\n\n")
	fmt.Fprintf(w, "**Date**: %s\n\n", report.Timestamp)

	fmt.Fprintf(w, "## Overview\n\n")
	fmt.Fprintf(w, "Measures how many vectors live near the GT neighborhood as N grows.\n\n")
	fmt.Fprintf(w, "**Key Question**: Does the neighborhood size remain stable as N grows?\n\n")

	fmt.Fprintf(w, "## Configuration\n\n")
	fmt.Fprintf(w, "| Parameter | Value |\n")
	fmt.Fprintf(w, "|-----------|-------|\n")
	fmt.Fprintf(w, "| Max Queries | %d |\n", report.Config.MaxQueries)
	fmt.Fprintf(w, "| K | %d |\n", report.Config.K)
	fmt.Fprintf(w, "| RQ1 Seed | %d |\n", report.Config.Seed)
	fmt.Fprintf(w, "| Radii | %.2f, %.2f, %.2f, %.2f, %.2f |\n\n",
		report.Config.Radii[0], report.Config.Radii[1], report.Config.Radii[2],
		report.Config.Radii[3], report.Config.Radii[4])

	// Results by family
	families := []string{"dbpedia_openai", "sphere_meta"}
	familyNames := map[string]string{"dbpedia_openai": "DBPedia/OpenAI", "sphere_meta": "Sphere/Meta"}

	for _, family := range families {
		fmt.Fprintf(w, "## %s\n\n", familyNames[family])

		// FLOAT results
		fmt.Fprintf(w, "### Experiment A: Radius Expansion (FLOAT)\n\n")
		fmt.Fprintf(w, "| Dataset | N | Radius | P50 Keep | P95 Keep | P99 Keep | Keep%% |\n")
		fmt.Fprintf(w, "|---------|---|--------|----------|----------|----------|-------|\n")
		for _, r := range report.Results {
			if r.Family == family && r.Method == "float" {
				fmt.Fprintf(w, "| %s | %d | %.2fx | %d | %d | %d | %.4f%% |\n",
					r.Dataset, r.BaseCount, r.Radius,
					r.P50Keep, r.P95Keep, r.P99Keep, r.AvgKeepPct)
			}
		}
		fmt.Fprintf(w, "\n")

		// RQ1 results
		fmt.Fprintf(w, "### Experiment B: RQ1 Compressed\n\n")
		fmt.Fprintf(w, "| Dataset | N | Method | Dims | P50 Keep | P95 Keep | P99 Keep | Keep%% |\n")
		fmt.Fprintf(w, "|---------|---|--------|------|----------|----------|----------|-------|\n")
		for _, r := range report.Results {
			if r.Family == family && strings.HasPrefix(r.Method, "rq1") {
				fmt.Fprintf(w, "| %s | %d | %s | %d | %d | %d | %d | %.4f%% |\n",
					r.Dataset, r.BaseCount, r.Method, r.DimsUsed,
					r.P50Keep, r.P95Keep, r.P99Keep, r.AvgKeepPct)
			}
		}
		fmt.Fprintf(w, "\n")
	}

	// Scaling fits
	fmt.Fprintf(w, "## Experiment C: Scaling Law Fits\n\n")
	fmt.Fprintf(w, "Fit: `keep = C * N^alpha`\n\n")
	fmt.Fprintf(w, "| Family | Method | Alpha | C | R² | Interpretation |\n")
	fmt.Fprintf(w, "|--------|--------|-------|---|-----|----------------|\n")
	for _, fit := range report.ScalingFits {
		interp := interpretAlpha(fit.Alpha)
		fmt.Fprintf(w, "| %s | %s | %.3f | %.2e | %.3f | %s |\n",
			fit.Family, fit.Method, fit.Alpha, fit.C, fit.R2, interp)
	}
	fmt.Fprintf(w, "\n")

	// Decision summary
	fmt.Fprintf(w, "## Decision Summary\n\n")
	fmt.Fprintf(w, "**Interpretation of Alpha (scaling exponent):**\n")
	fmt.Fprintf(w, "- α ≈ 0: constant (neighborhood size independent of N)\n")
	fmt.Fprintf(w, "- α < 0.3: strongly sublinear\n")
	fmt.Fprintf(w, "- α ≈ 0.5: sqrt-like scaling\n")
	fmt.Fprintf(w, "- α ≈ 1: linear scaling\n\n")

	fmt.Fprintf(w, "**Key Questions Answered:**\n\n")
	fmt.Fprintf(w, "1. **Does the GT neighborhood grow with N?**\n")
	fmt.Fprintf(w, "   - At radius 1.00x: trivially K (by definition)\n")
	fmt.Fprintf(w, "   - At radius 1.01x-1.10x: check alpha values above\n\n")
	fmt.Fprintf(w, "2. **Does it remain approximately constant?**\n")
	fmt.Fprintf(w, "   - If alpha ≈ 0, yes\n")
	fmt.Fprintf(w, "   - If alpha > 0.5, neighborhood grows significantly with N\n\n")
	fmt.Fprintf(w, "3. **Does RQ1 preserve the same scaling law?**\n")
	fmt.Fprintf(w, "   - Compare rq1_full alpha with float_1.0Xx alphas\n\n")
	fmt.Fprintf(w, "4. **Is the property embedding-dependent or compression-dependent?**\n")
	fmt.Fprintf(w, "   - If FLOAT shows constant/sublinear scaling, it's geometric\n")
	fmt.Fprintf(w, "   - If only RQ1 shows it, it's compression-induced\n\n")

	t.Logf("Markdown written to: %s", path)
}

func interpretAlpha(alpha float64) string {
	switch {
	case alpha < 0.1:
		return "constant"
	case alpha < 0.3:
		return "strongly sublinear"
	case alpha < 0.6:
		return "sublinear (sqrt-like)"
	case alpha < 0.9:
		return "weakly sublinear"
	default:
		return "linear"
	}
}

func printNDSummary(t *testing.T, report NDReport) {
	t.Log("\n" + strings.Repeat("=", 80))
	t.Log("NEIGHBORHOOD DENSITY SUMMARY")
	t.Log(strings.Repeat("=", 80))

	// Group by family
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

		// FLOAT results
		t.Log("\nFLOAT Radius Expansion:")
		t.Log("Dataset        | N        | 1.00x  | 1.01x  | 1.02x  | 1.05x  | 1.10x")
		t.Log("---------------|----------|--------|--------|--------|--------|-------")

		// Group by dataset
		datasets := make(map[string]map[float64]int)
		baseCount := make(map[string]int)
		for _, r := range report.Results {
			if r.Family == family && r.Method == "float" {
				if datasets[r.Dataset] == nil {
					datasets[r.Dataset] = make(map[float64]int)
				}
				datasets[r.Dataset][r.Radius] = r.P50Keep
				baseCount[r.Dataset] = r.BaseCount
			}
		}

		for _, ds := range []string{"dbpedia-100k", "dbpedia-500k", "dbpedia-1m", "sphere-1m", "sphere-10m"} {
			if vals, ok := datasets[ds]; ok {
				t.Logf("%-14s | %8d | %6d | %6d | %6d | %6d | %6d",
					ds, baseCount[ds],
					vals[1.00], vals[1.01], vals[1.02], vals[1.05], vals[1.10])
			}
		}

		// RQ1 results
		t.Log("\nRQ1 Compressed:")
		t.Log("Dataset        | N        | rq1_512 | rq1_768 | rq1_1024 | rq1_full")
		t.Log("---------------|----------|---------|---------|----------|----------")

		rq1Datasets := make(map[string]map[string]int)
		for _, r := range report.Results {
			if r.Family == family && strings.HasPrefix(r.Method, "rq1") {
				if rq1Datasets[r.Dataset] == nil {
					rq1Datasets[r.Dataset] = make(map[string]int)
				}
				rq1Datasets[r.Dataset][r.Method] = r.P50Keep
			}
		}

		for _, ds := range []string{"dbpedia-100k", "dbpedia-500k", "dbpedia-1m", "sphere-1m", "sphere-10m"} {
			if vals, ok := rq1Datasets[ds]; ok {
				t.Logf("%-14s | %8d | %7d | %7d | %8d | %8d",
					ds, baseCount[ds],
					vals["rq1_512"], vals["rq1_768"], vals["rq1_1024"], vals["rq1_full"])
			}
		}
	}

	// Scaling fits
	t.Log("\n--- Scaling Law Fits ---")
	t.Log("Family         | Method      | Alpha  | Interpretation")
	t.Log("---------------|-------------|--------|---------------")
	for _, fit := range report.ScalingFits {
		t.Logf("%-14s | %-11s | %6.3f | %s",
			fit.Family, fit.Method, fit.Alpha, interpretAlpha(fit.Alpha))
	}

	t.Log("\n" + strings.Repeat("=", 80))
}

# Technical Investigation: HFresh + MUVERA Recall Analysis

## Executive Summary

This document investigates why HFresh+MUVERA may exhibit lower recall than HNSW+MUVERA, based on:
1. Analysis of the MUVERA, PLAID, IGP, and ColBERTv2 papers
2. Deep examination of the current codebase implementation
3. Identification of architectural differences between HNSW and HFresh for multi-vector retrieval

---

## 1. Technical Summary of Relevant Papers

### 1.1 MUVERA: Multi-Vector Retrieval via Fixed Dimensional Encodings

**Source**: [arXiv:2405.19504](https://arxiv.org/abs/2405.19504) (NeurIPS 2024)

**Core Mechanism**:
- FDEs transform multi-vector documents into single vectors via:
  1. **SimHash Clustering**: Random Gaussian vectors `g₁...gₖₛᵢₘ` partition space into `2^kSim` clusters
  2. **Accumulation**: Tokens accumulate in cluster buckets (sums for queries, averages for documents)
  3. **Projection**: Random ±1 matrix `S` projects each cluster from `d → dProjections` dimensions
  4. **Repetition**: `Repetitions` independent encodings concatenated

**Key Asymmetry**:
- **Queries**: Sum tokens per cluster (no normalization)
- **Documents**: Average tokens per cluster, fill empty clusters via nearest-neighbor

**Theoretical Guarantee** (Theorem 2.1):
```
NChamfer(Q,P) - ε ≤ (1/|Q|)⟨F_q(Q), F_doc(P)⟩ ≤ NChamfer(Q,P) + ε
```

**Critical Assumptions**:
1. FDEs are **data-oblivious** — any MIPS solver works unchanged
2. **No specific index requirements** — "off-the-shelf MIPS solvers" compatible
3. Works with graph-based (DiskANN), LSH, or tree-based indexes

**Compression Compatibility**:
- PQ-256-8 achieves 32× compression with minimal recall loss
- Higher-dimensional FDEs (10K-20K) retrieve 5-20× fewer candidates at fixed recall

---

### 1.2 PLAID: An Efficient Engine for Late Interaction Retrieval

**Source**: [arXiv:2205.09707](https://arxiv.org/abs/2205.09707)

**Key Innovations**:
1. **Centroid Interaction**: Treats passages as "bags of centroids" for fast pruning
2. **Centroid Pruning**: Sparsifies centroid representation to reduce computation
3. **Staged Scoring Pipeline**:
   - Stage 1: Centroid-level interaction (cheapest)
   - Stage 2: Residual decompression for promising candidates
   - Stage 3: Full token scoring for top candidates

**Performance**: 7× GPU / 45× CPU latency reduction vs ColBERTv2

**Key Insight for HFresh**: PLAID's centroid-based approach is conceptually similar to HFresh's cluster-based routing, but PLAID uses **token-level centroids** trained via k-means on the token embedding space, not document-level clustering.

---

### 1.3 IGP: Efficient Multi-Vector Retrieval via Proximity Graph Index

**Source**: [SIGIR 2025](https://dl.acm.org/doi/10.1145/3726302.3730004)

**Key Insight**:
> "Vectors of a document contributing to the final score are likely to be within a few clusters nearest to vectors of the query"

**Approach**:
1. Incremental next-similar retrieval on proximity graph
2. Bounds candidate count by incrementally computing top vector scores during generation
3. Produces only hundreds of candidates with high recall

**Relevance to HFresh**: IGP validates that cluster-based candidate generation can work for multi-vector, but requires **query-aware cluster selection** — selecting clusters based on individual query token proximity, not just the FDE vector.

---

### 1.4 ColBERTv2: Residual Compression

**Source**: [arXiv:2112.01488](https://arxiv.org/abs/2112.01488)

**Residual Compression**:
- Cluster all token vectors via k-means
- Store: `centroid_id + quantized_residual` (1-2 bit per dimension)
- 6-10× space reduction with minimal quality loss

**Key Difference from Current Implementation**:
ColBERTv2 compresses **token vectors**, not FDEs. The current HFresh implementation applies RQ1 to the **already-encoded FDE**, which is a fundamentally different compression target.

---

### 1.5 Recent Advances: Token-Aware Clustering (TAC/Tachiom)

**Source**: [arXiv:2604.28142](https://arxiv.org/html/2604.28142)

**Key Innovation**: Allocate centroids based on token frequency and semantic variance, not uniform k-means.

**Insight for HFresh**: Standard k-means favors high-frequency tokens despite their low retrieval value. Domain-specific or rare tokens may be poorly represented.

---

## 2. HNSW+MUVERA vs HFresh+MUVERA: Architectural Differences

### 2.1 Search Pipeline Comparison

| Stage | HNSW+MUVERA | HFresh+MUVERA |
|-------|-------------|---------------|
| **FDE Encoding** | `EncodeQuery()` → 2560-dim | `EncodeQuery()` → 2560-dim |
| **Candidate Selection** | HNSW graph traversal, EF-controlled | Centroid HNSW search → posting retrieval |
| **Compression** | Optional RQ8 (8-bit) | **Mandatory RQ1 (1-bit)** |
| **Overfetch Factor** | `2k` (hardcoded, line 106) | `rescoreLimit` (default 350) |
| **Rescore** | Late interaction on `2k` candidates | Late interaction on `rescoreLimit` candidates |

### 2.2 Critical Code Differences

**HNSW Search** (`adapters/repos/db/vector/hnsw/search.go:99-116`):
```go
muvera_query := h.muveraEncoder.EncodeQuery(vectors)
overfetch := 2
docIDs, _, err := h.SearchByVector(ctx, muvera_query, overfetch*k, allowList)
```
- Simple `2×k` overfetch
- Direct graph traversal, no intermediate quantization loss

**HFresh Search** (`adapters/repos/db/vector/hfresh/search.go:336-348`):
```go
queryFlat := h.muveraEncoder.EncodeQuery(vectors)
candidateIDs, _, err := h.SearchByVector(ctx, queryFlat, int(h.rescoreLimit), allow)
return h.computeLateInteraction(ctx, vectors, k, candidateIDs)
```
- Fixed `rescoreLimit` (default 350), not proportional to `k`
- Query goes through RQ1 compression before centroid matching

### 2.3 Compression Architecture

**HNSW with MUVERA** (`entities/vectorindex/hnsw/config.go:358-362`):
- Supports RQ1 or RQ8
- When using RQ8: 8 bits per dimension = 2560 bytes per FDE
- When using RQ1: 1 bit per dimension = 320 bytes per FDE

**HFresh with MUVERA** (`entities/vectorindex/hfresh/config.go:166-168`):
```go
if bits > 1 {
    return fmt.Errorf("rq only supports 1 bit, got %d", bits)
}
```
- **Only RQ1 supported** — no RQ2/RQ4/RQ8 options
- FDE compressed from 10,240 bytes → 320 bytes (32× compression)

---

## 3. Prioritized Recall-Loss Hypotheses

### Hypothesis 1: RQ1 Compression Loss on FDEs (HIGH PRIORITY)

**Evidence**:
- FDEs are 2560-dimensional vectors with specific statistical properties
- RQ1 reduces each dimension to 1 bit (sign only)
- MUVERA paper shows PQ-256-8 works well, but doesn't test extreme 1-bit quantization
- FDEs have **high variance across dimensions** due to random projection, making binary quantization potentially more lossy

**Expected Impact**: HIGH — binary quantization may destroy fine-grained similarity information

**Current Code** (`adapters/repos/db/vector/compressionhelpers/binary_rotational_quantization.go:180-200`):
```go
func (rq *BinaryRotationalQuantizer) Encode(x []float32) []uint64 {
    // Rotates, then takes sign bit
    for b := 0; b < numBlocks; b++ {
        bits |= uint64(mask) << bit
    }
    code.Bits()[b] = bits
}
```

---

### Hypothesis 2: Centroid Routing Incompatibility with FDE Similarity (HIGH PRIORITY)

**Evidence**:
- HFresh centroids are computed by k-means on **original vectors** (or FDEs for MUVERA)
- Centroid search uses RQ1-compressed distances
- FDE similarity structure may not align with centroid-based routing

**Key Question**: Are centroids trained on FDEs, or on original multi-vectors?

**Current Code** (`adapters/repos/db/vector/hfresh/split.go:166-191`):
```go
func (h *HFresh) computeNewCentroids(posting Posting) ([]Centroid, [][]Vector) {
    encoder := kmeans.NewKMeansEncoder(2, 0, h.dims, h.distancer.distancer)
    vectors := make([][]float32, 0, len(posting))
    for _, v := range posting {
        vectors = append(vectors, h.quantizer.Restore(compressed))  // Decompressed FDEs!
    }
```

**Insight**: Centroids ARE computed on FDEs for MUVERA mode, but via k-means on **restored** (decompressed) FDEs — this introduces quantization noise into centroid computation.

---

### Hypothesis 3: Insufficient Candidate Generation (MEDIUM PRIORITY)

**Evidence**:
- HNSW: `2×k` overfetch is proportional to requested results
- HFresh: Fixed `rescoreLimit=350` regardless of `k`
- For `k=100`, HNSW overfetches 200, HFresh overfetches 350 — but HFresh candidates come from posting scans, not graph traversal

**Calculation**:
- With `searchProbe=64` centroids and `maxPostingSize=458` vectors per posting
- Maximum candidates scanned: `64 × 458 = 29,312`
- But `rescoreLimit=350` caps to 350 for late interaction
- If true positives are spread across many postings, 350 may be insufficient

---

### Hypothesis 4: FDE Dimensionality vs Posting Size Mismatch (MEDIUM PRIORITY)

**Evidence**:
- Default FDE dimensions: `10 × 16 × 16 = 2560`
- Default posting size: 48KB / (2560÷8 + metadata) ≈ 140 vectors per posting
- This is significantly smaller than typical IVF posting lists (thousands of vectors)

**Current Code** (`adapters/repos/db/vector/hfresh/config.go:155-192` equivalent):
```go
// vectorBytes = dims * 0.125 + 9 + RQMetadataSize
// For 2560 dims: vectorBytes ≈ 320 + 9 + 40 = 369 bytes
// maxPostingSize = 48KB / 369 ≈ 133 vectors
```

**Impact**: Small postings mean more centroids, more routing decisions, more opportunities for routing failure.

---

### Hypothesis 5: Empty Cluster Handling in FDE Encoding (LOW-MEDIUM PRIORITY)

**Evidence**:
- Recent fix (commit `a2d9964b6e`) addressed division-by-zero for empty clusters
- Empty clusters are filled via nearest-neighbor fallback
- This fallback may introduce distortion, especially for sparse multi-vectors

**Current Code** (`adapters/repos/db/vector/multivector/muvera.go:144-164`):
```go
if repetitionClusterCounts[cluster] == 0 {
    // Find nearest non-empty cluster
    for docIdx, clusterMapped := range clusterMappings {
        hamming, _ := distancer.HammingBitwise([]uint64{cluster}, []uint64{clusterMapped})
        if hamming < minHamming {
            nearestPoint = uint64(docIdx)
        }
    }
    // Copy vector from nearest point
    tmpVec[startIdx:] = fullVec[nearestPoint][:]
}
```

---

### Hypothesis 6: Replication Strategy Not FDE-Aware (LOW PRIORITY)

**Evidence**:
- RNG selection uses centroid distances to select replica postings
- RNG factor is fixed (`10.0`) regardless of FDE characteristics
- No consideration of FDE-space "ambiguity" or confidence

**Current Code** (`adapters/repos/db/vector/hfresh/rng.go:48`):
```go
if centerDist <= (1.0/h.rngFactor)*cDistance {
    tooClose = true
}
```

---

### Hypothesis 7: Query Normalization Mismatch (LOW PRIORITY)

**Evidence**:
- FDE queries use sums (no normalization per MUVERA spec)
- HFresh normalizes query vectors for cosine distance
- Potential conflict between MUVERA encoding and HFresh search

**Current Code** (`adapters/repos/db/vector/hfresh/search.go:341`):
```go
queryFlat := h.muveraEncoder.EncodeQuery(vectors)  // NOT normalized
candidateIDs, _, err := h.SearchByVector(ctx, queryFlat, int(h.rescoreLimit), allow)
// SearchByVector normalizes: vector = h.normalizeVec(vector)
```

**Note**: This appears correct — the FDE is normalized before search, matching document FDEs which are also normalized before storage.

---

## 4. Minimal Experiments to Validate/Reject Hypotheses

### Experiment 1: RQ1 vs RQ8 Compression Impact

**Goal**: Isolate compression loss from routing loss

**Method**:
1. Create two HFresh indexes with identical data
2. Index A: RQ1 (current)
3. Index B: RQ8 (requires implementation)
4. Compare recall@10, recall@100 on benchmark dataset

**Expected Outcome**: If RQ8 significantly improves recall, compression is a major factor.

**Implementation Effort**: LOW — add RQ8 support to HFresh config validation

---

### Experiment 2: Centroid Routing Quality Measurement

**Goal**: Measure how often true positives are missed during centroid selection

**Method**:
1. For each query, compute ground-truth top-100 via brute force
2. Record which centroids contain each ground-truth result
3. Measure: What % of ground-truth results are in selected centroids?

**Metrics**:
- `centroid_recall@k`: Fraction of true positives whose posting was selected
- `centroid_miss_distance`: Average distance from nearest selected centroid to missed result's centroid

**Implementation**: Add instrumentation to `SearchByVector`

---

### Experiment 3: Candidate Generation Quality

**Goal**: Isolate posting selection + scoring from late interaction

**Method**:
1. Compute ground-truth top-k via brute force MaxSim
2. After `SearchByVector` returns candidates, measure recall before late interaction
3. Vary `rescoreLimit` from 100 to 2000

**Metrics**:
- `pre_rescore_recall@k`: Recall within candidate set before MaxSim
- `post_rescore_recall@k`: Final recall after MaxSim

---

### Experiment 4: HNSW Baseline Comparison

**Goal**: Establish recall ceiling for MUVERA on this dataset

**Method**:
1. Index same data with HNSW+MUVERA (RQ8)
2. Index with HFresh+MUVERA (RQ1)
3. Compare recall curves

**Expected Outcome**: Quantify the recall gap

---

### Experiment 5: FDE Dimensionality Sweep

**Goal**: Test if current FDE parameters are optimal for HFresh

**Method**:
1. Vary MUVERA parameters:
   - `kSim`: 3, 4, 5, 6
   - `dProjections`: 8, 16, 32
   - `repetitions`: 5, 10, 20
2. Measure recall and latency for each configuration

**Expected Outcome**: Identify if default parameters (4, 16, 10) are suboptimal for cluster-based retrieval

---

## 5. Proposed Code Changes

### Phase 1: Instrumentation (No Algorithmic Changes)

#### 5.1 Add Recall Instrumentation Metrics

**File**: `adapters/repos/db/vector/hfresh/metrics.go`

```go
type SearchMetrics struct {
    CentroidsSearched      int
    CentroidsWithResults   int
    CandidatesScored       int
    CandidatesPassedFilter int
    RescoreCandidates      int
}
```

#### 5.2 Add Centroid Routing Logger

**File**: `adapters/repos/db/vector/hfresh/search.go`

Add optional debug logging to track:
- Selected centroid IDs and distances
- Posting sizes for each selected centroid
- Number of candidates after deduplication

#### 5.3 Add Compression Quality Metrics

**File**: `adapters/repos/db/vector/compressionhelpers/binary_rotational_quantization.go`

Add method to compute reconstruction error:
```go
func (rq *BinaryRotationalQuantizer) ReconstructionError(original, compressed []float32) float32 {
    // Compute L2 or cosine distance between original and restored
}
```

### Phase 2: RQ2/RQ4 Implementation (If Justified by Experiments)

#### 5.4 Multi-Bit RQ Support

**Files to Modify**:
- `entities/vectorindex/hfresh/config.go`: Allow bits > 1
- `adapters/repos/db/vector/compressionhelpers/`: Add multi-bit encoding

**Proposed Interface**:
```go
type MultibitRQ struct {
    bits int  // 1, 2, 4, or 8
    // ... existing BinaryRotationalQuantizer fields
}
```

### Phase 3: Algorithmic Improvements (After Diagnosis)

Based on experiment results, potential changes:
- **If centroid routing fails**: FDE-specific centroid training
- **If compression is the issue**: RQ2/RQ4/RQ8 support
- **If candidate generation fails**: Query-token-aware posting selection (PLAID-style)

---

## 6. RQ2/RQ4/RQ8 Recommendation

### 6.1 Should We Implement RQ2/RQ4?

**Recommendation**: **Yes, implement RQ2 experimentally**

**Rationale**:
1. MUVERA paper validates PQ-256-8 (equivalent to ~8 bits) works well
2. RQ1 may be too aggressive for 2560-dim FDEs
3. RQ2 provides 2× memory vs RQ1, potentially significant recall improvement
4. Implementation cost is low — existing RQ infrastructure supports multi-bit

### 6.2 Expected Tradeoffs

| Quantization | Memory/Vector | Expected Recall Impact | I/O per Query |
|--------------|---------------|------------------------|---------------|
| RQ1 (current) | 320 bytes | Baseline | 320 × candidates |
| RQ2 | 640 bytes | +5-15% recall | 640 × candidates |
| RQ4 | 1280 bytes | +10-20% recall | 1280 × candidates |
| RQ8 | 2560 bytes | +15-25% recall | 2560 × candidates |
| Uncompressed | 10240 bytes | Maximum | 10KB × candidates |

### 6.3 Benchmark Criteria for Keeping RQ2/RQ4

**Keep RQ2 if**:
- Recall@10 improves by ≥5% on BEIR benchmark
- Latency increase is ≤20%

**Keep RQ4 if**:
- Recall@10 improves by ≥10% over RQ2
- Memory is still ≤50% of uncompressed

---

## 7. Key Findings Summary

### Confirmed Issues:
1. **RQ1-only compression**: HFresh forces 1-bit quantization, unlike HNSW which supports 8-bit
2. **Fixed rescoreLimit**: Not proportional to `k`, may under-fetch for large k
3. **Centroid computation on decompressed FDEs**: Introduces quantization noise

### Open Questions Requiring Experimentation:
1. How much recall is lost to RQ1 vs routing vs candidate generation?
2. Are current MUVERA parameters optimal for cluster-based retrieval?
3. Would PLAID-style centroid interaction help HFresh?

### Recommended Next Steps:
1. Implement instrumentation (Phase 1)
2. Run Experiment 1 (RQ1 vs RQ8 comparison)
3. Run Experiment 2 (Centroid routing quality)
4. Based on results, implement RQ2/RQ4 if compression is the primary issue

---

## References

1. [MUVERA: Multi-Vector Retrieval via Fixed Dimensional Encodings](https://arxiv.org/abs/2405.19504)
2. [PLAID: An Efficient Engine for Late Interaction Retrieval](https://arxiv.org/abs/2205.09707)
3. [IGP: Efficient Multi-Vector Retrieval via Proximity Graph Index](https://dl.acm.org/doi/10.1145/3726302.3730004)
4. [ColBERTv2: Effective and Efficient Retrieval via Lightweight Late Interaction](https://arxiv.org/abs/2112.01488)
5. [Efficient Multivector Retrieval with Token-Aware Clustering](https://arxiv.org/html/2604.28142)
6. [Weaviate 8-bit Rotational Quantization Blog](https://weaviate.io/blog/8-bit-rotational-quantization)
7. [Google Research: MUVERA Blog](https://research.google/blog/muvera-making-multi-vector-retrieval-as-fast-as-single-vector-search/)

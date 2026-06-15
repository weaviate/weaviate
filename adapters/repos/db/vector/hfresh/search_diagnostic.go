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
	"context"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/storobj"
)

// DecoupledBudgetTrace captures detailed metrics for the decoupled budget experiment.
// This allows us to measure the impact of routing vs reranking budgets separately.
type DecoupledBudgetTrace struct {
	// Input parameters
	RoutingBudget int `json:"routing_budget"`
	RerankBudget  int `json:"rerank_budget"`
	K             int `json:"k"`

	// Stage 1: Centroid selection (controlled by routingBudget)
	SelectedCentroids int `json:"selected_centroids"`

	// Stage 2: Posting scan (consequence of routingBudget)
	PostingsScanned     int `json:"postings_scanned"`
	TotalVectorsScanned int `json:"total_vectors_scanned"`
	UniqueDocsEnumerated int `json:"unique_docs_enumerated"`

	// Stage 3: Approximate ranking (controlled by rerankBudget)
	ApproxRankedCandidateCount int `json:"approx_ranked_candidate_count"`

	// Stage 4: MaxSim reranking (controlled by rerankBudget)
	CandidatesPassedToMaxSim int `json:"candidates_passed_to_maxsim"`

	// Output
	ReturnedCount int `json:"returned_count"`
}

// SearchByMultiVectorDiagnostic performs a MUVERA search with decoupled budgets.
//
// This is a DIAGNOSTIC-ONLY function for measuring the separate effects of:
//   - routingBudget: controls how many centroids are selected and postings scanned
//   - rerankBudget: controls how many candidates are kept after RQ1 ranking and passed to MaxSim
//
// The current production code couples these via rescoreLimit, which controls both.
// This function allows us to measure which budget matters more for recall.
//
// Semantics:
//  1. Encode query with MUVERA/FDE
//  2. Select centroids using routingBudget (NOT rerankBudget)
//  3. Scan postings from selected centroids
//  4. Rank candidates with RQ1 approximate scorer
//  5. Keep top rerankBudget approximate candidates
//  6. Run full MaxSim on those rerankBudget candidates
//  7. Return final top-k plus trace
func (h *HFresh) SearchByMultiVectorDiagnostic(
	ctx context.Context,
	vectors [][]float32,
	k int,
	routingBudget int,
	rerankBudget int,
	allow helpers.AllowList,
) ([]uint64, []float32, *DecoupledBudgetTrace, error) {
	if !h.muvera.Load() {
		return nil, nil, nil, ErrMuveraNotEnabled
	}

	trace := &DecoupledBudgetTrace{
		RoutingBudget: routingBudget,
		RerankBudget:  rerankBudget,
		K:             k,
	}

	// Step 1: Encode query with MUVERA/FDE
	queryFlat := h.muveraEncoder.EncodeQuery(vectors)

	// Step 2-5: Search with decoupled budgets
	candidateIDs, err := h.searchByVectorDiagnostic(ctx, queryFlat, routingBudget, rerankBudget, allow, trace)
	if err != nil {
		return nil, nil, trace, errors.Wrap(err, "diagnostic candidate search")
	}

	trace.CandidatesPassedToMaxSim = len(candidateIDs)

	// Step 6: Run full MaxSim on rerankBudget candidates
	ids, dists, err := h.computeLateInteractionDiagnostic(ctx, vectors, k, candidateIDs)
	if err != nil {
		return nil, nil, trace, errors.Wrap(err, "diagnostic late interaction")
	}

	trace.ReturnedCount = len(ids)

	return ids, dists, trace, nil
}

// searchByVectorDiagnostic is the internal search with decoupled budgets.
// routingBudget controls centroid selection.
// rerankBudget controls the RQ1 result set size.
func (h *HFresh) searchByVectorDiagnostic(
	ctx context.Context,
	vector []float32,
	routingBudget int,
	rerankBudget int,
	allowList helpers.AllowList,
	trace *DecoupledBudgetTrace,
) ([]uint64, error) {
	vector = h.normalizeVec(vector)
	if h.quantizer == nil {
		return nil, errors.New("quantizer not initialized")
	}
	queryDistancer := h.quantizer.NewDistancer(vector)

	// Step 2: Select centroids using ONLY routingBudget
	// This is the key difference from production code which uses max(rescoreLimit, searchProbe)
	candidateCentroidNum := routingBudget

	nAllowList := allowList
	if allowList != nil {
		nAllowList = h.wrapAllowList(ctx, allowList)
		defer nAllowList.Close()
	}

	centroids, err := h.Centroids.Search(vector, candidateCentroidNum, nAllowList)
	if err != nil {
		return nil, err
	}
	if len(centroids.data) == 0 {
		return nil, nil
	}

	// Step 3: Build list of selected centroids (applying pruning)
	maxDist := centroids.data[0].Distance * h.config.MaxDistanceRatio
	selectedCentroids := make([]uint64, 0, candidateCentroidNum)
	for i := 0; i < len(centroids.data) && len(selectedCentroids) < candidateCentroidNum; i++ {
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

	trace.SelectedCentroids = len(selectedCentroids)
	trace.PostingsScanned = len(selectedCentroids)

	// Step 4: Scan postings from selected centroids
	postings, err := h.PostingStore.MultiGet(ctx, selectedCentroids)
	if err != nil {
		return nil, err
	}

	visited := h.visitedPool.Borrow()
	defer h.visitedPool.Return(visited)

	// Use rerankBudget for the RQ1 result set size
	// This is the key difference: routing doesn't affect ranking depth
	q := NewResultSet(rerankBudget)

	var decompressBuf []uint64
	var totalVectorsScanned int
	var uniqueDocsEnumerated int

	for _, p := range postings {
		if p == nil {
			continue
		}

		totalVectorsScanned += len(p)

		for _, v := range p {
			id := v.ID()

			// Skip deleted vectors
			deleted, err := h.VersionMap.IsDeleted(context.Background(), id)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to check if vector %d is deleted", id)
			}
			if deleted {
				continue
			}

			// Skip duplicates
			if visited.CheckAndVisit(id) {
				continue
			}

			// Count unique docs enumerated
			uniqueDocsEnumerated++

			// Skip vectors not in allow list
			if allowList != nil && !allowList.Contains(id) {
				continue
			}

			decompressBuf = h.quantizer.FromCompressedBytesInto(v.Data(), decompressBuf)
			dist, err := queryDistancer.Distance(decompressBuf)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to compute distance for vector %d", id)
			}

			q.Insert(id, dist)
		}
	}

	trace.TotalVectorsScanned = totalVectorsScanned
	trace.UniqueDocsEnumerated = uniqueDocsEnumerated
	trace.ApproxRankedCandidateCount = q.Len()

	// Step 5: Return top rerankBudget candidates (already limited by ResultSet)
	ids := make([]uint64, 0, q.Len())
	for id := range q.Iter() {
		ids = append(ids, id)
	}

	return ids, nil
}

// computeLateInteractionDiagnostic computes MaxSim scores for candidates.
// This is identical to computeLateInteraction but used in the diagnostic path.
func (h *HFresh) computeLateInteractionDiagnostic(
	ctx context.Context,
	queryVectors [][]float32,
	k int,
	candidateIDs []uint64,
) ([]uint64, []float32, error) {
	results := NewResultSet(k)

	for _, id := range candidateIDs {
		docVectors, err := h.multivectorForIdThunk(ctx, id)
		if err != nil {
			// The object may have been deleted between search and rescore.
			var notFound storobj.ErrNotFound
			if errors.As(err, &notFound) {
				continue
			}
			return nil, nil, errors.Wrapf(err, "get multi-vector for id %d", id)
		}
		score, err := h.maxSimScore(queryVectors, docVectors)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "compute maxsim for id %d", id)
		}
		results.Insert(id, score)
	}

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

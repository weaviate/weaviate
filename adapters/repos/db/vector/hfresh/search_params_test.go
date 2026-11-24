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
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/searchparams"
)

const (
	testConfiguredProbe   = 64
	testConfiguredRescore = 350
)

// searchProbeCases is shared by the single-vector and MUVERA budget tests:
// both must apply the same per-request routing rule.
var searchProbeCases = []struct {
	name   string
	k      int
	params *searchparams.VectorSearchParams
	want   int
}{
	{name: "nil params uses configured probe", k: 10, params: nil, want: testConfiguredProbe},
	{name: "nil params, k above configured probe", k: 100, params: nil, want: 100},
	{name: "zero probe uses configured probe", k: 10, params: &searchparams.VectorSearchParams{}, want: testConfiguredProbe},
	{name: "unrelated knob set uses configured probe", k: 10, params: &searchparams.VectorSearchParams{EF: 500}, want: testConfiguredProbe},
	{name: "requested probe below k is floored by k", k: 10, params: &searchparams.VectorSearchParams{SearchProbe: 5}, want: 10},
	{name: "requested probe above k, below configured, is respected", k: 10, params: &searchparams.VectorSearchParams{SearchProbe: 20}, want: 20},
	{name: "requested probe above configured", k: 10, params: &searchparams.VectorSearchParams{SearchProbe: 512}, want: 512},
	{name: "k above requested probe", k: 600, params: &searchparams.VectorSearchParams{SearchProbe: 512}, want: 600},
}

func newBudgetOnlyHFresh() *HFresh {
	h := &HFresh{}
	atomic.StoreUint32(&h.searchProbe, testConfiguredProbe)
	atomic.StoreUint32(&h.rescoreLimit, testConfiguredRescore)
	return h
}

func Test_searchProbeFor(t *testing.T) {
	h := newBudgetOnlyHFresh()
	for _, tt := range searchProbeCases {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, h.searchProbeFor(tt.k, tt.params))
		})
	}
}

func Test_muveraSearchBudgets_Override(t *testing.T) {
	h := newBudgetOnlyHFresh()
	for _, tt := range searchProbeCases {
		t.Run(tt.name, func(t *testing.T) {
			routing, rerank := h.muveraSearchBudgets(tt.k, tt.params)
			require.Equal(t, tt.want, routing, "routingBudget")
			// the request probe never changes the rerank depth
			require.Equal(t, max(tt.k, testConfiguredRescore), rerank, "rerankBudget")
		})
	}
}

// Test_SearchByVector_SearchProbeOverride checks the slow-log annotations of
// the single-vector paths and that a probe override on a tiny index (where
// every probe covers all postings) returns the same results as the default.
func Test_SearchByVector_SearchProbeOverride(t *testing.T) {
	const (
		dim   = 16
		nDocs = 50
		k     = 10
	)
	rng := rand.New(rand.NewSource(7))
	vectors := make([][]float32, nDocs)
	for i := range vectors {
		v := make([]float32, dim)
		for j := range v {
			v[j] = rng.Float32()
		}
		vectors[i] = v
	}
	tf := createHFreshIndexWithVectorStore(t, vectors)
	for i, v := range vectors {
		addVectorToIndex(t, &tf, uint64(i), v)
	}
	require.Eventually(t, func() bool {
		return tf.Index.taskQueue.Size() == 0
	}, 30*time.Second, 50*time.Millisecond, "background tasks did not drain")

	query := vectors[3]
	baseline, _, err := tf.Index.SearchByVector(t.Context(), query, k, nil, nil)
	require.NoError(t, err)
	require.NotEmpty(t, baseline)

	smallAllow := helpers.NewAllowList()
	for i := 0; i < nDocs; i++ {
		smallAllow.Insert(uint64(i))
	}

	configured := int(atomic.LoadUint32(&tf.Index.searchProbe))

	tests := []struct {
		name          string
		params        *searchparams.VectorSearchParams
		allow         helpers.AllowList
		wantFlat      bool
		wantRequested any // nil = key absent
		wantEffective any // nil = key absent
	}{
		{name: "no override, graph path", params: nil, wantFlat: false},
		{name: "zero probe, graph path", params: &searchparams.VectorSearchParams{}, wantFlat: false},
		{
			name:          "probe above configured, graph path",
			params:        &searchparams.VectorSearchParams{SearchProbe: configured + 100},
			wantFlat:      false,
			wantRequested: configured + 100,
			wantEffective: configured + 100,
		},
		{
			name:          "probe below k, graph path, floored by k",
			params:        &searchparams.VectorSearchParams{SearchProbe: 3},
			wantFlat:      false,
			wantRequested: 3,
			wantEffective: k,
		},
		{
			name:          "probe on brute-force path reports requested only",
			params:        &searchparams.VectorSearchParams{SearchProbe: 512},
			allow:         smallAllow,
			wantFlat:      true,
			wantRequested: 512,
		},
		{name: "no override, brute-force path", params: nil, allow: smallAllow, wantFlat: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := helpers.InitSlowQueryDetails(t.Context())
			ids, _, err := tf.Index.SearchByVector(ctx, query, k, tt.allow, tt.params)
			require.NoError(t, err)
			require.Equal(t, baseline, ids)

			details := helpers.ExtractSlowQueryDetails(ctx)
			require.Equal(t, tt.wantFlat, details["hfresh_flat_search"], "hfresh_flat_search")

			requested, ok := details["hfresh_search_probe_requested"]
			if tt.wantRequested == nil {
				require.False(t, ok, "hfresh_search_probe_requested must be absent, got %v", requested)
			} else {
				require.Equal(t, tt.wantRequested, requested, "hfresh_search_probe_requested")
			}

			effective, ok := details["hfresh_search_probe_effective"]
			if tt.wantEffective == nil {
				require.False(t, ok, "hfresh_search_probe_effective must be absent, got %v", effective)
			} else {
				require.Equal(t, tt.wantEffective, effective, "hfresh_search_probe_effective")
			}
		})
	}
	t.Run("per-target probes on one context are all reported", func(t *testing.T) {
		ctx := helpers.InitSlowQueryDetails(t.Context())
		for _, probe := range []int{configured + 100, configured + 50} {
			_, _, err := tf.Index.SearchByVector(ctx, query, k, nil, &searchparams.VectorSearchParams{SearchProbe: probe})
			require.NoError(t, err)
		}
		details := helpers.ExtractSlowQueryDetails(ctx)
		require.Equal(t, []int{configured + 50, configured + 100}, details["hfresh_search_probe_requested"])
		require.Equal(t, []int{configured + 50, configured + 100}, details["hfresh_search_probe_effective"])
	})
}

// Test_SearchByMultiVector_SearchProbeOverride checks the MUVERA path reports
// the per-request routing budget and leaves results unchanged on a tiny index.
func Test_SearchByMultiVector_SearchProbeOverride(t *testing.T) {
	const (
		nDocs  = 50
		tokens = 2
		dim    = 16
		k      = 5
	)
	tf := createMuveraHFreshIndex(t)
	rng := rand.New(rand.NewSource(11))
	for i := 0; i < nDocs; i++ {
		addMultiVectorToIndex(t, &tf, uint64(i), randomMultiVector(rng, tokens, dim))
	}
	require.Eventually(t, func() bool {
		return tf.Index.taskQueue.Size() == 0
	}, 30*time.Second, 50*time.Millisecond, "background tasks did not drain")

	query := randomMultiVector(rng, tokens, dim)

	ctx := helpers.InitSlowQueryDetails(t.Context())
	baseline, _, err := tf.Index.SearchByMultiVector(ctx, query, k, nil, nil)
	require.NoError(t, err)
	require.NotEmpty(t, baseline)
	details := helpers.ExtractSlowQueryDetails(ctx)
	require.NotContains(t, details, "hfresh_search_probe_requested")
	require.NotContains(t, details, "hfresh_search_probe_effective")

	ctx = helpers.InitSlowQueryDetails(t.Context())
	ids, _, err := tf.Index.SearchByMultiVector(ctx, query, k, nil, &searchparams.VectorSearchParams{SearchProbe: 1000})
	require.NoError(t, err)
	require.Equal(t, baseline, ids)
	details = helpers.ExtractSlowQueryDetails(ctx)
	require.Equal(t, 1000, details["hfresh_search_probe_requested"])
	require.Equal(t, 1000, details["hfresh_search_probe_effective"])
}

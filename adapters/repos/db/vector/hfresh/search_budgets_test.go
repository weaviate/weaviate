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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hfresh"
)

func updateSearchProbe(t *testing.T, tf *TestHFresh, probe uint32) {
	t.Helper()
	uc := ent.NewDefaultUserConfig()
	uc.SearchProbe = probe
	require.NoError(t, tf.Index.UpdateUserConfig(uc, func() {}))
}

// TestMuveraSearchBudgets pins the budget semantics of the decoupled
// routing/rerank search: searchProbe steers routing in BOTH directions (a
// low explicit probe is not floored by rescoreLimit), rescoreLimit floors
// only the rerank depth, and the user-requested k floors both (issue #277).
//
// An earlier version used routingBudget = max(k, searchProbe, rescoreLimit),
// which silently ignored any searchProbe below rescoreLimit (350) and broke
// the low-probe latency/recall trade-off.
func TestMuveraSearchBudgets(t *testing.T) {
	tests := []struct {
		name        string
		searchProbe uint32 // 0 = leave the parse-time default (256)
		k           int
		wantRouting int
		wantRerank  int
	}{
		{name: "defaults", k: 10, wantRouting: 256, wantRerank: 350},
		{name: "low probe respected below rescoreLimit", searchProbe: 16, k: 10, wantRouting: 16, wantRerank: 350},
		{name: "high probe respected above rescoreLimit", searchProbe: 512, k: 10, wantRouting: 512, wantRerank: 350},
		{name: "k wins over low probe", searchProbe: 16, k: 600, wantRouting: 600, wantRerank: 600},
		{name: "k wins over defaults", k: 400, wantRouting: 400, wantRerank: 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tf := createMuveraHFreshIndex(t)
			if tt.searchProbe > 0 {
				updateSearchProbe(t, &tf, tt.searchProbe)
			}
			routing, rerank := tf.Index.muveraSearchBudgets(tt.k)
			require.Equal(t, tt.wantRouting, routing, "routingBudget")
			require.Equal(t, tt.wantRerank, rerank, "rerankBudget")
		})
	}
}

// TestSearchProbeChangesResults is the behavioral half: an explicit low
// searchProbe must actually reduce routing work. This is the test that
// would have caught the rescoreLimit floor silently swallowing low probes:
// with the floor, probe 16 scanned exactly the same postings as the
// default, so the candidate coverage — and every downstream result — was
// bit-identical.
//
// The observable is candidate COVERAGE (rerank budget above the corpus
// size), not the rescored top-k: on IVF-style routing the true top-k
// concentrates in the postings nearest the query, so a lower probe often
// returns the same top-k even though it scans a fraction of the data —
// top-k equality is a property of the dataset, coverage is a property of
// the budget.
func TestSearchProbeChangesResults(t *testing.T) {
	const (
		nDocs  = 2500
		tokens = 2
		dim    = 16
	)
	tf := createMuveraHFreshIndex(t, withDistanceProvider(distancer.NewCosineDistanceProvider()))
	rng := rand.New(rand.NewSource(21))
	for i := 0; i < nDocs; i++ {
		addMultiVectorToIndex(t, &tf, uint64(i), randomMultiVector(rng, tokens, dim))
	}

	// wait for background splits so the posting layout is stable and wide
	// enough (>16 postings) for the routing budget to matter
	require.Eventually(t, func() bool {
		return tf.Index.taskQueue.Size() == 0
	}, 60*time.Second, 100*time.Millisecond, "background tasks did not drain")

	// disable posting expansion: its purpose is to recover candidates a
	// narrow probe missed, which would mask the routing budget under test
	tf.Index.docToPostings = nil

	probe := randomMultiVector(rng, tokens, dim)
	queryFDE := tf.Index.muveraEncoder.EncodeQuery(tf.Index.normalizeMultiVec(probe))

	// rerank budget above the corpus size so the candidate set reflects
	// coverage, not the RQ1 top-N cut
	wideOpen := nDocs * 2

	fullCandidates, err := tf.Index.searchByFDE(t.Context(), queryFDE, 1000, wideOpen, nil)
	require.NoError(t, err)
	require.Equal(t, nDocs, len(fullCandidates),
		"a routing budget above the posting count must cover the whole corpus")

	lowProbeCandidates, err := tf.Index.searchByFDE(t.Context(), queryFDE, 16, wideOpen, nil)
	require.NoError(t, err)
	require.NotEmpty(t, lowProbeCandidates)
	require.Less(t, len(lowProbeCandidates), len(fullCandidates),
		"routingBudget=16 must scan fewer postings than full routing; "+
			"equal coverage means the probe is being silently floored")

	// end to end: the collection-level searchProbe drives the same budgets
	updateSearchProbe(t, &tf, 16)
	routing, rerank := tf.Index.muveraSearchBudgets(10)
	require.Equal(t, 16, routing)
	require.Equal(t, 350, rerank)
	ids, _, err := tf.Index.SearchByMultiVector(t.Context(), probe, 10, nil)
	require.NoError(t, err)
	require.Len(t, ids, 10)
}

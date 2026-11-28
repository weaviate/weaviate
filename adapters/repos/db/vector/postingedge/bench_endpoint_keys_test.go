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

//go:build postingedgebench

package postingedge

import (
	"context"
	"encoding/json"
	"flag"
	"os"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var (
	benchEndpointKeys = flag.Bool("bench.endpointkeys", false, "postingedge: instead of the plan, record the searches of the "+
		"first sweep at each of its efs (untimed) and time their endpoint scoring replayed over the endpoint side keyed by "+
		"slot, by doc id and by shuffled doc id (endpoint_keys_test.go), into <name>-endpointkeys.json next to -bench.out")
	benchEndpointKeyRounds = flag.Int("bench.endpointkeyrounds", 7, "postingedge, with -bench.endpointkeys: timed rounds, "+
		"each replaying every keying once, in an order that rotates from round to round")
)

// endpointKeysPoint is the replay at one ef.
type endpointKeysPoint struct {
	EF       int     `json:"ef"`
	Recall   float64 `json:"recall"`
	Searches int     `json:"searches"`
	Loads    int     `json:"loads"`
	// Followed is the number of edges of sources within the cut, Scored the
	// endpoints scored of them.
	Followed int                  `json:"followed"`
	Scored   int                  `json:"scored"`
	Keyings  []endpointKeysTiming `json:"keyings"`
}

// endpointKeysTiming is one keying at one ef: ns per scored endpoint of every
// round, in round order, and their median.
type endpointKeysTiming struct {
	Name          string    `json:"name"`
	NsPerEndpoint []float64 `json:"nsPerEndpoint"`
	Median        float64   `json:"median"`
}

// measureEndpointKeys records the searches of the first sweep at each of its
// efs, with its rescoreLimit, batch and margin, one thread and untimed, and
// times their endpoint scoring replayed over three keyings of the endpoint
// side: by slot, as the index is; by doc id, the order the vectors were
// added in; and by doc id through a random permutation, the order a long
// history of updates could leave. Every replay must score the endpoints the
// searches scored, with the same estimates.
func measureEndpointKeys(t *testing.T, index *Index, data *benchData, uc UserConfig, plan []sweep) {
	require.NotEmpty(t, *benchOut, "-bench.endpointkeys writes its result next to -bench.out")
	ctx := context.Background()
	st := index.state.Load()
	first := plan[0]
	require.NotEqual(t, autoMargin, first.margins[0], "-bench.endpointkeys needs a first sweep with a margin given")

	slots := slotKeys(st)
	docIDs, err := docIDKeys(st)
	require.NoError(t, err)
	shuffled, err := shuffledKeys(st, 1)
	require.NoError(t, err)
	keyings := []struct {
		name  string
		keyOf []uint32
		side  *sidecar
	}{
		{name: "slots", keyOf: slots},
		{name: "doc ids", keyOf: docIDs},
		{name: "doc ids, shuffled", keyOf: shuffled},
	}
	for i := range keyings {
		keyings[i].side = keyedSidecar(st, keyings[i].keyOf)
	}

	var points []endpointKeysPoint
	for _, ef := range first.efs {
		uc.EF, uc.RescoreLimit, uc.LoadBatch, uc.SourceMargin = ef, first.rescores[0], first.batches[0], float32(first.margins[0])
		require.NoError(t, index.UpdateUserConfig(uc, func() {}))

		point := endpointKeysPoint{EF: ef, Searches: len(data.queries)}
		searches := make([]recordedSearch, len(data.queries))
		hits, want := 0, 0
		for i, query := range data.queries {
			rec, ids, err := recordSearch(ctx, index, query, *benchK)
			require.NoError(t, err)
			searches[i] = rec
			want += rec.scored
			point.Loads += len(rec.loads)
			for _, load := range rec.loads {
				point.Followed += len(load.followed)
			}
			for _, id := range data.truth[i] {
				if slices.Contains(ids, id) {
					hits++
				}
			}
		}
		point.Recall = float64(hits) / float64(len(data.queries)*len(data.truth[0]))
		point.Scored = want

		keyed := make([]*endpointKeys, len(keyings))
		for i, k := range keyings {
			keyed[i] = &endpointKeys{name: k.name, side: k.side, searches: keyedSearches(searches, k.keyOf)}
		}
		replay := newEndpointReplay(st, searches)
		var sum0 float64
		// A first pass of every keying, untimed, that also checks it.
		for i, keys := range keyed {
			scored, sum, err := replay.run(keys)
			require.NoError(t, err)
			require.Equal(t, want, scored, "%s: the replay scores the endpoints the searches scored", keys.name)
			if i == 0 {
				sum0 = sum
			}
			require.Equal(t, sum0, sum, "%s: the same estimates", keys.name)
		}

		timings := make([]endpointKeysTiming, len(keyed))
		runtime.LockOSThread()
		for round := range *benchEndpointKeyRounds {
			for j := range keyed {
				i := (round + j) % len(keyed)
				runtime.GC()
				begin := time.Now()
				scored, _, err := replay.run(keyed[i])
				took := time.Since(begin)
				require.NoError(t, err)
				timings[i].NsPerEndpoint = append(timings[i].NsPerEndpoint, float64(took.Nanoseconds())/float64(scored))
			}
		}
		runtime.UnlockOSThread()
		for i := range timings {
			timings[i].Name = keyed[i].name
			sorted := slices.Sorted(slices.Values(timings[i].NsPerEndpoint))
			timings[i].Median = sorted[len(sorted)/2]
			t.Logf("endpoint keys: ef=%d recall=%.4f %s: %.1f ns per endpoint (median of %d rounds, %.1f to %.1f), %.3fx the slots; "+
				"%d searches, %.1f loads, %.0f followed and %.0f scored per search",
				ef, point.Recall, timings[i].Name, timings[i].Median, len(sorted), sorted[0], sorted[len(sorted)-1],
				timings[i].Median/timings[0].Median, len(data.queries),
				float64(point.Loads)/float64(len(data.queries)), float64(point.Followed)/float64(len(data.queries)),
				float64(want)/float64(len(data.queries)))
		}
		point.Keyings = timings
		points = append(points, point)
	}

	raw, err := json.MarshalIndent(points, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(strings.TrimSuffix(*benchOut, ".json")+"-endpointkeys.json", raw, 0o644))
}

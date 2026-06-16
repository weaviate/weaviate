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

package reindex_multinode

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/docker"
)

// TestMultiNode_ChangeTokenization_RoundTrip pins the bug reported in
// https://github.com/weaviate/weaviate/issues/10675: after a word→field→word round-trip on a 3-node
// RF=3 cluster, only one replica ends up with a populated inverted bucket.
// The other replicas report task FINISHED and the schema flag flipped, but
// their on-disk buckets are empty, so a BM25 query routed to them returns
// zero matches.
//
// Frontend Claude observed in production:
//
//	token        weaviate-0    weaviate-1    weaviate-2
//	gopro                 0             0        195077
//	hero                  0             0         58594
//	...
//
// Objects themselves replicate correctly (Aggregate { count } = 1M on every
// replica), so this is per-replica inverted-bucket state, not per-shard
// routing.
//
// Each node is hit directly via compose.GetWeaviateNode(i).URI() so an
// empty-bucket replica cannot be hidden behind a load balancer's
// round-robin landing on a healthy one. If any single replica returns a
// count different from baseline, the test fails with a per-node
// breakdown.
func TestMultiNode_ChangeTokenization_RoundTrip(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	const className = "RoundTripTokenize"
	restURI := compose.GetWeaviateNode(1).URI()

	createCollection(t, restURI, className, 3, 3, []*models.Property{
		{Name: "text", DataType: []string{"text"}, Tokenization: "word"},
	})
	defer deleteCollection(t, restURI, className)

	importObjects(t, restURI, className, testDocuments)

	// Baselines must already be consistent across nodes before any
	// migration runs. If they're not, the rest of the test is meaningless.
	baselines := make(map[string][]int)
	for _, q := range testBM25Queries {
		counts := perNodeBM25Counts(t, compose, className, q)
		t.Logf("baseline %q: %v", q, counts)
		require.Equalf(t, counts[0], counts[1],
			"baseline inconsistent for %q: node1=%d node2=%d", q, counts[0], counts[1])
		require.Equalf(t, counts[0], counts[2],
			"baseline inconsistent for %q: node1=%d node3=%d", q, counts[0], counts[2])
		require.Greaterf(t, counts[0], 0,
			"baseline %q must match at least one doc", q)
		baselines[q] = counts
	}

	// word → field.
	taskID := reindexhelpers.SubmitIndexUpdate(t, restURI, className, "text",
		`{"searchable":{"tokenization":"field"}}`)
	reindexhelpers.AwaitReindexFinished(t, restURI, taskID, reindexhelpers.WithTimeout(180*time.Second))
	awaitTokenizationOnAllNodes(t, compose, className, "text", "field")

	// field → word.
	taskID = reindexhelpers.SubmitIndexUpdate(t, restURI, className, "text",
		`{"searchable":{"tokenization":"word"}}`)
	reindexhelpers.AwaitReindexFinished(t, restURI, taskID, reindexhelpers.WithTimeout(180*time.Second))
	awaitTokenizationOnAllNodes(t, compose, className, "text", "word")

	// Per-node assertion: every replica must return the baseline count for
	// every query. A replica with an empty inverted bucket returns 0 for
	// everything; that is the failure mode Frontend Claude observed.
	var failures []string
	for _, q := range testBM25Queries {
		actual := perNodeBM25Counts(t, compose, className, q)
		expected := baselines[q]
		t.Logf("post-round-trip %q: baseline=%v actual=%v", q, expected, actual)
		for i := 0; i < 3; i++ {
			if actual[i] != expected[i] {
				failures = append(failures,
					fmt.Sprintf("query=%q node%d expected=%d actual=%d",
						q, i+1, expected[i], actual[i]))
			}
		}
	}
	if len(failures) > 0 {
		sort.Strings(failures)
		t.Fatalf(
			"per-replica inverted-bucket mismatch after word→field→word "+
				"round-trip (see https://github.com/weaviate/weaviate/issues/10675); %d mismatches:\n  %s",
			len(failures), strings.Join(failures, "\n  "))
	}
}

// perNodeBM25Counts runs a BM25 query against each node directly and
// returns the per-node match counts. Direct per-node queries (not via LB)
// are essential here — Frontend Claude's bug pattern is per-replica
// bucket state, so a round-robin'd query could mask it.
func perNodeBM25Counts(t *testing.T, compose *docker.DockerCompose, className, query string) []int {
	t.Helper()

	counts := make([]int, 3)
	for i := 0; i < 3; i++ {
		uri := compose.GetWeaviateNode(i + 1).URI()
		ids, err := runBM25QueryOnNode(t, uri, className, query)
		require.NoErrorf(t, err, "BM25 query %q on node %d", query, i+1)
		counts[i] = len(ids)
	}
	return counts
}

// awaitTokenizationOnAllNodes polls each node's /v1/schema/{class} until
// the named property's tokenization matches the target. The schema flip is
// RAFT-replicated from the OnTaskCompleted commit, so it lands on every
// node within RAFT propagation latency — typically tens of ms.
func awaitTokenizationOnAllNodes(
	t *testing.T, compose *docker.DockerCompose, className, propName, target string,
) {
	t.Helper()

	for i := 0; i < 3; i++ {
		uri := compose.GetWeaviateNode(i + 1).URI()
		require.Eventuallyf(t, func() bool {
			return tryGetPropertyTokenization(uri, className, propName) == target
		}, 30*time.Second, 50*time.Millisecond,
			"node %d: property %q tokenization should be %q", i+1, propName, target)
	}
}

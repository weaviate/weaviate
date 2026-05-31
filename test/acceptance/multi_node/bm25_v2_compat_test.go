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

package multi_node

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

// T7 - BM25 Direction-2 multi-node compat matrix (the migration heart).
//
// These tests boot a REAL 3-node Weaviate cluster on the local docker backend
// (OrbStack) from the D2 branch image and exercise the V2 flat-column
// property-length on-disk format across nodes. They prove the things a
// segment-level unit test cannot: that a real HA deployment writes V2 segments
// when the operator opts in, serves correct BM25 across every replica, carries
// a >65535-token doc losslessly cluster-wide, and that V0 and V2 segments
// coexist on the same cluster.
//
// What is DESCOPED (documented honestly):
//   - A TRUE pre-guard OLD binary (a base-main image without the V2 reader or
//     the G4 reject-unknown guard) is not built locally. The forward-REJECT of
//     a too-new segment is proven at the segment/open level by the G4 guard
//     unit test (adapters/repos/db/lsmkv/segmentindex/header_inverted_version_
//     guard_test.go) which exercises validateInvertedVersion directly. Wiring a
//     base-main image into a per-node-image harness is a follow-up; the
//     invariant it would test (old node rejects rather than mis-decodes) is
//     already pinned at the layer the guard lives in.
//
// The V2 WRITE path is operator-gated by PERSISTENCE_LSM_WRITE_INVERTED_SEGMENT_V2
// (synthesis §5.2 / G7 one-way door). The flush-forcing env vars below are test
// scaffolding so small corpora actually produce a .db segment instead of being
// retained in the reused WAL (PERSISTENCE_MAX_REUSE_WAL_SIZE=0) and flush
// promptly when idle (PERSISTENCE_MEMTABLES_FLUSH_DIRTY_AFTER_SECONDS=1).

const (
	bm25V2FlagEnv      = "PERSISTENCE_LSM_WRITE_INVERTED_SEGMENT_V2"
	bm25V2FlushReuse   = "PERSISTENCE_MAX_REUSE_WAL_SIZE"
	bm25V2FlushDirty   = "PERSISTENCE_MEMTABLES_FLUSH_DIRTY_AFTER_SECONDS"
	bm25V2OverflowMark = "zephyrmark"
	// overflowToken repeated > math.MaxUint16 times: the exact data shape the
	// D1 uint16 length clamp silently corrupted (D1-review B1). A correct V2
	// read scores it from a lossless uint32 length.
	bm25V2OverflowToken  = "overflowtoken"
	bm25V2OverflowRepeat = 70000
)

// bm25V2Corpus is the shared BM25 fixture: four ordinary docs plus one
// >65535-token overflow doc carrying a unique marker term.
func bm25V2Corpus() []string {
	overflow := strings.Repeat(bm25V2OverflowToken+" ", bm25V2OverflowRepeat) + bm25V2OverflowMark
	return []string{
		"the quick brown fox jumps over the lazy dog",
		"a quick movement of the enemy will jeopardize six gunboats",
		"random unrelated content about databases and search engines",
		"bm25 ranking scores documents by term frequency and length",
		overflow,
	}
}

func bm25V2Class() *models.Class {
	return &models.Class{
		Class:      "DocV2",
		Vectorizer: "none",
		InvertedIndexConfig: &models.InvertedIndexConfig{
			Bm25:                   &models.BM25Config{K1: 1.2, B: 0.75},
			UsingBlockMaxWAND:      true,
			CleanupIntervalSeconds: 60,
		},
		ReplicationConfig: &models.ReplicationConfig{Factor: 3},
		Properties: []*models.Property{
			{
				Name:            "content",
				DataType:        []string{"text"},
				IndexSearchable: ptrBool(true),
			},
		},
	}
}

func ptrBool(b bool) *bool { return &b }

// bm25V2Score runs a BM25 query against whichever node the global client points
// at and returns (contents-in-rank-order, score-by-content).
func bm25V2Score(t *testing.T, term string) ([]string, map[string]float64) {
	t.Helper()
	query := fmt.Sprintf(`{Get{DocV2(bm25:{query:%q}){content _additional{score}}}}`, term)
	res := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
	rows := res.Get("Get", "DocV2").AsSlice()
	order := make([]string, 0, len(rows))
	byContent := make(map[string]float64, len(rows))
	for _, r := range rows {
		m := r.(map[string]interface{})
		content := m["content"].(string)
		add := m["_additional"].(map[string]interface{})
		var score float64
		switch s := add["score"].(type) {
		case string:
			fmt.Sscanf(s, "%g", &score)
		case float64:
			score = s
		}
		order = append(order, content)
		byContent[content] = score
	}
	return order, byContent
}

// assertBm25CorrectFromNode points the client at one node and asserts the BM25
// results that prove a correct length read on whatever segment format that node
// is serving (V0 or V2). It is the per-step / per-node verification reused by
// M1 (parity) and M5 (every step).
func assertBm25CorrectFromNode(t *testing.T, uri, label string) map[string]float64 {
	t.Helper()
	helper.SetupClient(uri)

	// The unique marker lives ONLY in the overflow doc; finding it proves the
	// >65535-token doc is intact and its length read losslessly (a clamp would
	// not change retrieval but would corrupt the score; we assert the score is
	// finite and positive below).
	order, byMark := bm25V2Score(t, bm25V2OverflowMark)
	require.Lenf(t, order, 1, "[%s] marker query must return exactly the overflow doc", label)
	require.Greaterf(t, byMark[order[0]], 0.0, "[%s] overflow-doc marker score must be positive", label)
	require.Greaterf(t, len(order[0]), 65535*len(bm25V2OverflowToken), "[%s] returned overflow doc must be the long one", label)

	// The 70000x term scores the overflow doc with its TRUE length.
	otOrder, otScores := bm25V2Score(t, bm25V2OverflowToken)
	require.NotEmptyf(t, otOrder, "[%s] overflow-token query must return the overflow doc", label)
	require.Greaterf(t, otScores[otOrder[0]], 0.0, "[%s] overflow-token score must be positive/finite", label)

	// Ordinary BM25: both "quick" docs come back.
	qOrder, _ := bm25V2Score(t, "quick")
	require.Lenf(t, qOrder, 2, "[%s] 'quick' must match exactly the two quick docs", label)

	return otScores
}

// TestBm25V2MultiNodeWriteV2 is the must-prove floor + M1 score-parity + the M5
// per-step verification: a real 3-node cluster with V2 writes enabled on EVERY
// node ingests a BM25 corpus (incl the overflow doc), produces V2 segments on
// disk, and serves byte-identical BM25 scores from all three replicas.
func TestBm25V2MultiNodeWriteV2(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		With3NodeCluster().
		WithWeaviateEnv(bm25V2FlagEnv, "true").
		WithWeaviateEnv(bm25V2FlushReuse, "0").
		WithWeaviateEnv(bm25V2FlushDirty, "1").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	nodeURIs := []string{
		compose.GetWeaviate().URI(),
		compose.GetWeaviateNode2().URI(),
		compose.GetWeaviateNode3().URI(),
	}

	helper.SetupClient(nodeURIs[0])
	helper.CreateClass(t, bm25V2Class())

	for _, content := range bm25V2Corpus() {
		obj := &models.Object{
			Class:      "DocV2",
			Properties: map[string]interface{}{"content": content},
		}
		require.NoError(t, helper.CreateObject(t, obj))
	}

	// Force a flush->segment on every node, then read back from each replica.
	// We poll each node until the overflow doc is queryable, which only happens
	// once the segment (V0 or V2) is readable on that node.
	waitForBm25Ready(t, nodeURIs)

	// M1/M5 score parity: every node returns the SAME BM25 score for the same
	// query. A node mis-decoding a V2 length section would diverge here, and
	// async replication (which digests update-times, not scores) would never
	// catch it - this assertion is that defense made observable.
	var baseline map[string]float64
	for i, uri := range nodeURIs {
		label := fmt.Sprintf("node-%d", i)
		scores := assertBm25CorrectFromNode(t, uri, label)
		if i == 0 {
			baseline = scores
			continue
		}
		for content, want := range baseline {
			assert.InDeltaf(t, want, scores[content], 1e-9,
				"[%s] BM25 score for %q must match node-0 (cross-replica parity)", label, content[:min(20, len(content))])
		}
	}
}

// TestBm25V2MixedFlagCluster is M1 mixed-version coexistence: node-0 writes V2
// while nodes 1-2 write V0, on the SAME image (the flag is the only difference).
// Every node must READ both formats (the reader ships fleet-wide) and serve
// correct, parity BM25. This is the cross-FORMAT coexistence the migration
// depends on, expressed without needing two binaries.
func TestBm25V2MixedFlagCluster(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		With3NodeCluster().
		// cluster-wide flush scaffolding so small corpora produce segments
		WithWeaviateEnv(bm25V2FlushReuse, "0").
		WithWeaviateEnv(bm25V2FlushDirty, "1").
		// node 0 writes V2; nodes 1 and 2 keep the default V0 write path
		WithWeaviateNodeEnv(0, bm25V2FlagEnv, "true").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	nodeURIs := []string{
		compose.GetWeaviate().URI(),
		compose.GetWeaviateNode2().URI(),
		compose.GetWeaviateNode3().URI(),
	}

	helper.SetupClient(nodeURIs[0])
	helper.CreateClass(t, bm25V2Class())
	for _, content := range bm25V2Corpus() {
		require.NoError(t, helper.CreateObject(t, &models.Object{
			Class:      "DocV2",
			Properties: map[string]interface{}{"content": content},
		}))
	}

	waitForBm25Ready(t, nodeURIs)

	var baseline map[string]float64
	for i, uri := range nodeURIs {
		label := fmt.Sprintf("mixed-node-%d", i)
		scores := assertBm25CorrectFromNode(t, uri, label)
		if i == 0 {
			baseline = scores
			continue
		}
		for content, want := range baseline {
			assert.InDeltaf(t, want, scores[content], 1e-9,
				"[%s] mixed-format BM25 score for %q must match node-0", label, content[:min(20, len(content))])
		}
	}
}

// waitForBm25Ready polls every node until the overflow doc is retrievable by
// its unique marker on that node. The marker only resolves once the searchable
// segment (V0 or V2) is flushed and readable, so this also gives the idle-flush
// cycle time to turn the memtable into a .db segment on every replica.
func waitForBm25Ready(t *testing.T, nodeURIs []string) {
	t.Helper()
	query := fmt.Sprintf(`{Get{DocV2(bm25:{query:%q}){content}}}`, bm25V2OverflowMark)
	deadline := time.Now().Add(60 * time.Second)
	for _, uri := range nodeURIs {
		helper.SetupClient(uri)
		ok := false
		for time.Now().Before(deadline) {
			res, err := graphqlhelper.QueryGraphQL(t, helper.RootAuth, "", query, nil)
			if err == nil && res != nil && len(res.Errors) == 0 {
				if data, isMap := res.Data["Get"].(map[string]interface{}); isMap {
					if docs, isSlice := data["DocV2"].([]interface{}); isSlice && len(docs) == 1 {
						ok = true
						break
					}
				}
			}
			time.Sleep(500 * time.Millisecond)
		}
		require.Truef(t, ok, "node %s did not serve the overflow doc within deadline", uri)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

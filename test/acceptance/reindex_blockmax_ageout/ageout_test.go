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

// Package reindex_blockmax_ageout is the regression for weaviate/weaviate#12252:
// a searchable property migrated to blockmax in a permanently-partial class
// reads back as WAND once its FINISHED task ages out of the DTM list. Runs
// with DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS=0 so assertions hit a
// genuinely empty task list, where only the durable per-property stamp keeps
// reads correct.
package reindex_blockmax_ageout

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

// shortTitle/longTitle have equal token counts but different distinct-term
// counts. BM25 doc length is token count on blockmax but distinct-term count
// on WAND, so for query term "banana" (tf=1 in both): correct blockmax gives
// equal doc length → equal scores; a WAND downgrade gives shortTitle a
// shorter length → a strictly higher score. Score equality is the test.
const (
	shortTitle = "banana apple apple apple apple" // 5 tokens, 2 distinct
	longTitle  = "banana cherry mango date fig"   // 5 tokens, 5 distinct
)

func TestBlockmaxAgeOut(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		WithWeaviate().
		WithWeaviateEnv("USE_INVERTED_SEARCHABLE", "false"). // classes start on WAND
		WithWeaviateEnv("DISTRIBUTED_TASKS_SCHEDULER_TICK_INTERVAL_SECONDS", "1").
		WithWeaviateEnv("DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS", "0"). // GC finished tasks on the next tick
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("terminate containers: %s", err)
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	restURI := compose.GetWeaviate().URI()
	container := compose.GetWeaviate().Container()
	defer func() {
		if t.Failed() {
			dumpLogs(ctx, t, container)
		}
	}()

	t.Run("AgedOutPartialClass", func(t *testing.T) {
		testAgedOutPartialClass(t, restURI)
	})
	t.Run("NewTenantShardBlockmax", func(t *testing.T) {
		testNewTenantShardBlockmax(t, restURI)
	})
}

// testAgedOutPartialClass covers assertions (i)-(iv): after the migrated
// property's task ages out, GET/PUT/rebuild reflect blockmax, and a
// change-tokenization keeps the bucket blockmax (BM25 length model intact).
func testAgedOutPartialClass(t *testing.T, restURI string) {
	const class = "AgeOutPartial"
	bm25Class(t, class, false)
	defer helper.DeleteClass(t, class)

	require.False(t, helper.GetClass(t, class).InvertedIndexConfig.UsingBlockMaxWAND)
	createDoc(t, class, shortTitle, "")
	createDoc(t, class, longTitle, "")

	// Migrate ONLY title; body stays WAND so the class is permanently partial.
	// Under TTL=0 the FINISHED task is GC'd almost immediately, so wait for it
	// to drain (awaitTaskGone) — that's exactly the aged-out empty-list state.
	migrate := reindexhelpers.SubmitIndexUpsert(t, restURI, class, "title", "searchable", `{"algorithm":"blockmax"}`)
	awaitTaskGone(t, restURI, migrate)
	require.Empty(t, reindexTasksFor(t, restURI, class), "task list must be empty (aged out)")

	// (i) GET reports blockmax for the migrated property against an empty list.
	require.Equal(t, "blockmax", searchableAlgorithm(t, restURI, class, "title"))
	require.Equal(t, "wand", searchableAlgorithm(t, restURI, class, "body"))
	require.False(t, helper.GetClass(t, class).InvertedIndexConfig.UsingBlockMaxWAND,
		"class flip must stay deferred while body is still WAND")

	// Baseline: title migrated to blockmax → equal doc lengths.
	assertEqualBananaScores(t, class, "", "after change-algorithm, task aged out (blockmax baseline)")

	// (ii) PUT {algorithm:blockmax} on the already-migrated property → 200 NO_OP.
	putResp := reindexhelpers.SubmitIndexUpsertRaw(t, restURI, class, "title", "searchable", `{"algorithm":"blockmax"}`)
	require.Equal(t, http.StatusOK, putResp.StatusCode, "repeat blockmax PUT must be NO_OP: %s", putResp.Body)
	require.Contains(t, putResp.Body, "NO_OP")

	// (iii) rebuild is accepted (not 400 "cannot rebuild a WAND searchable
	// index"). RebuildIndex asserts the 202; drain it before the next submit.
	rebuild := reindexhelpers.RebuildIndex(t, restURI, class, "title", "searchable")
	awaitTaskGone(t, restURI, rebuild)

	// (iv) THE corrupting hop: change-tokenization submitted against the empty
	// task list must derive StrategyInverted from the stamp and keep the bucket
	// blockmax. Assert the BM25 length model, not just a 200.
	retok := reindexhelpers.SubmitIndexUpsert(t, restURI, class, "title", "searchable", `{"tokenization":"whitespace"}`)
	awaitTaskGone(t, restURI, retok)
	reindexhelpers.AwaitTokenizationVisible(t, restURI, class, "title", "whitespace")
	require.Equal(t, "blockmax", searchableAlgorithm(t, restURI, class, "title"),
		"change-tokenization on a stamped-blockmax property must not downgrade it to WAND")
	assertEqualBananaScores(t, class, "", "after change-tokenization on the aged-out blockmax property")
}

// testNewTenantShardBlockmax covers assertion (v): a tenant shard created
// after the property was stamped must build a blockmax bucket even though the
// class flag is still false, verified via the BM25 length model.
func testNewTenantShardBlockmax(t *testing.T, restURI string) {
	const class = "AgeOutMT"
	bm25Class(t, class, true)
	defer helper.DeleteClass(t, class)

	helper.CreateTenants(t, class, []*models.Tenant{{Name: "t1", ActivityStatus: models.TenantActivityStatusHOT}})
	createDoc(t, class, shortTitle, "t1")
	createDoc(t, class, longTitle, "t1")

	migrate := reindexhelpers.SubmitIndexUpsert(t, restURI, class, "title", "searchable", `{"algorithm":"blockmax"}`)
	awaitTaskGone(t, restURI, migrate) // completes + drains under TTL=0
	require.Equal(t, "blockmax", searchableAlgorithm(t, restURI, class, "title"),
		"title must be stamped blockmax after the migration")
	require.False(t, helper.GetClass(t, class).InvertedIndexConfig.UsingBlockMaxWAND,
		"class flip must stay deferred while body is still WAND")

	// New tenant → new shard, created after the stamp is durable.
	helper.CreateTenants(t, class, []*models.Tenant{{Name: "t2", ActivityStatus: models.TenantActivityStatusHOT}})
	createDoc(t, class, shortTitle, "t2")
	createDoc(t, class, longTitle, "t2")

	// t2's brand-new searchable bucket must be blockmax (per-prop stamp overrides
	// the still-false class flag at shard init), so its BM25 lengths are
	// term-frequency-based.
	assertEqualBananaScores(t, class, "t2", "new tenant shard on a partial class")
}

// --- helpers ---------------------------------------------------------------

func bm25Class(t *testing.T, class string, mt bool) {
	t.Helper()
	c := &models.Class{
		Class: class,
		Properties: []*models.Property{
			{Name: "title", DataType: []string{"text"}, Tokenization: "word"},
			{Name: "body", DataType: []string{"text"}, Tokenization: "word"}, // stays WAND → partial class
		},
		InvertedIndexConfig: &models.InvertedIndexConfig{Bm25: &models.BM25Config{K1: 1.2, B: 0.75}},
		Vectorizer:          "none",
	}
	if mt {
		c.MultiTenancyConfig = &models.MultiTenancyConfig{Enabled: true}
	}
	helper.CreateClass(t, c)
}

func createDoc(t *testing.T, class, title, tenant string) {
	t.Helper()
	obj := &models.Object{
		Class:      class,
		Properties: map[string]interface{}{"title": title, "body": "filler"},
	}
	if tenant != "" {
		obj.Tenant = tenant
	}
	require.NoError(t, helper.CreateObject(t, obj))
}

func searchableAlgorithm(t *testing.T, restURI, class, prop string) string {
	t.Helper()
	resp := reindexhelpers.GetIndexes(t, restURI, class)
	for _, p := range resp.Properties {
		if p.Name != prop {
			continue
		}
		for _, idx := range p.Indexes {
			if idx.Type == "searchable" {
				return idx.Algorithm
			}
		}
	}
	t.Fatalf("no searchable index for %q in GET /indexes", prop)
	return ""
}

// reindexTasksFor returns the reindex tasks currently visible for a collection.
func reindexTasksFor(t *testing.T, restURI, class string) []models.DistributedTask {
	t.Helper()
	tasks, ok := reindexhelpers.TryFetchTasks(restURI)
	require.True(t, ok, "GET /v1/tasks failed")
	var out []models.DistributedTask
	for _, task := range tasks["reindex"] {
		if strings.HasPrefix(task.ID, class+":") {
			out = append(out, task)
		}
	}
	return out
}

// awaitTaskGone blocks until the task has been GC'd out of the DTM list (the
// FINISHED task has aged out under TTL=0).
func awaitTaskGone(t *testing.T, restURI, taskID string) {
	t.Helper()
	require.Eventually(t, func() bool {
		tasks, ok := reindexhelpers.TryFetchTasks(restURI)
		if !ok {
			return false
		}
		for _, task := range tasks["reindex"] {
			if task.ID == taskID {
				return false
			}
		}
		return true
	}, 60*time.Second, 250*time.Millisecond, "task %s never aged out of the DTM list", taskID)
}

// assertEqualBananaScores fails if the "banana" BM25 scores of shortTitle and
// longTitle diverge — the observable signature of a WAND (item-count) doc-length
// model where blockmax (token-count) is required. tenant is "" for non-MT.
func assertEqualBananaScores(t *testing.T, class, tenant, when string) {
	t.Helper()
	scores := bananaScores(t, class, tenant)
	short, okS := scores[shortTitle]
	long, okL := scores[longTitle]
	require.Truef(t, okS && okL, "both docs must match 'banana' (%s): got %+v", when, scores)
	require.Positive(t, short, "score must be positive (%s)", when)
	assert.InDeltaf(t, long, short, 1e-4,
		"blockmax doc lengths must be term-frequency-based, so equal-token-count docs score equally (%s); "+
			"a strictly higher short-title score is the WAND-downgrade signature", when)
}

// bananaScores runs a BM25 "banana" query over the title property and returns
// title→score.
func bananaScores(t *testing.T, class, tenant string) map[string]float64 {
	t.Helper()
	tenantClause := ""
	if tenant != "" {
		tenantClause = fmt.Sprintf(", tenant: %q", tenant)
	}
	gql := fmt.Sprintf(`{
		Get {
			%s(bm25: {query: "banana", properties: ["title"]}%s) {
				title
				_additional { score }
			}
		}
	}`, class, tenantClause)

	resp, err := graphqlhelper.QueryGraphQL(t, nil, "", gql, nil)
	require.NoError(t, err)
	require.Empty(t, resp.Errors, "graphql errors: %+v", resp.Errors)

	getMap, ok := resp.Data["Get"].(map[string]interface{})
	require.True(t, ok, "missing Get in response")
	items, ok := getMap[class].([]interface{})
	require.True(t, ok, "missing class results")

	out := make(map[string]float64, len(items))
	for _, it := range items {
		m := it.(map[string]interface{})
		title := m["title"].(string)
		add := m["_additional"].(map[string]interface{})
		out[title] = toFloat(t, add["score"])
	}
	return out
}

func toFloat(t *testing.T, v interface{}) float64 {
	t.Helper()
	switch n := v.(type) {
	case float64:
		return n
	case string:
		f, err := strconv.ParseFloat(n, 64)
		require.NoError(t, err, "parse score %q", n)
		return f
	default:
		t.Fatalf("unexpected score type %T", v)
		return 0
	}
}

func dumpLogs(ctx context.Context, t *testing.T, c testcontainers.Container) {
	t.Helper()
	reader, err := c.Logs(ctx)
	if err != nil {
		t.Logf("get container logs: %v", err)
		return
	}
	defer reader.Close()
	logs, _ := io.ReadAll(reader)
	lines := strings.Split(string(logs), "\n")
	if len(lines) > 200 {
		lines = lines[len(lines)-200:]
	}
	t.Logf("=== container logs (last 200 lines) ===\n%s", strings.Join(lines, "\n"))
}

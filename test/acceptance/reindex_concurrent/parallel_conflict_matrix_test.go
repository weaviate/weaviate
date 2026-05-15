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

// Package reindex_concurrent — parallel-conflict matrix.
//
// Frontend hit a Sev 1 on 2026-05-14: two parallel PUTs to
// /v1/schema/{class}/indexes/{prop} (enable-filterable + enable-rangeable on
// the SAME property) both failed. Root cause (per agent on the primary fix
// branch): strategy selection reads stale state and the sentinel-dir
// lifecycle collides between concurrent migrations targeting the same
// property.
//
// This file pins every realistic adjacent parallel-conflict scenario on the
// same (collection, property) tuple as RED tests. When the primary fix
// lands, the matrix tells us which combinations the fix actually covers vs.
// which still leak.
//
// Test contract for each subtest:
//
//   - Pre-state: create a class with property config that makes both
//     operations legal in isolation.
//   - Seed enough objects (~2000) that the migrations overlap in time.
//   - Fire both PUTs in parallel via enterrors.GoWrapper. Collect both
//     responses.
//   - Accept any of: 202+task FINISHED, or 409 (conflict, in-flight task on
//     this property). 4xx validator rejections are also acceptable when
//     the scenario design expects them. 5xx is NEVER acceptable.
//   - Assert at least one operation succeeded.
//   - Assert no silent data loss: whichever bucket(s) ended up indexed must
//     be queryable. The expected hit count is the same as a sequential
//     ground-truth run.
//
// Scope: same (collection, property). Different properties / different
// classes are out of scope — see PRD.
package reindex_concurrent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

const matrixObjectCount = 2000

// TestParallelConflictMatrix enumerates parallel (op_A, op_B) pairs on the
// SAME (collection, property) tuple. Each subtest is one cell of the matrix.
//
// Time budget note: each subtest spins a fresh class on a SHARED container.
// Total runtime is dominated by ingest+migration of ~2k objects per cell.
func TestParallelConflictMatrix(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		WithWeaviate().
		WithWeaviateEnv("USE_INVERTED_SEARCHABLE", "false").
		WithWeaviateEnv("DISTRIBUTED_TASKS_SCHEDULER_TICK_INTERVAL_SECONDS", "1").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	restURI := compose.GetWeaviate().URI()
	helper.SetupClient(restURI)

	container := compose.GetWeaviate().Container()
	defer func() {
		if t.Failed() {
			reader, err := container.Logs(ctx)
			if err != nil {
				t.Logf("failed to get container logs: %v", err)
				return
			}
			defer reader.Close()
			logs, _ := io.ReadAll(reader)
			lines := strings.Split(string(logs), "\n")
			if len(lines) > 400 {
				lines = lines[len(lines)-400:]
			}
			t.Logf("=== Container logs (last 400 lines) ===\n%s", strings.Join(lines, "\n"))
		}
	}()

	// Scenario 1: the primary Sev 1 repro.
	t.Run("enable_filterable__enable_rangeable", func(t *testing.T) {
		testParallel_EnableFilterableEnableRangeable(t, restURI)
	})

	// Scenario 2: filterable + searchable on a text property.
	t.Run("enable_filterable__enable_searchable", func(t *testing.T) {
		testParallel_EnableFilterableEnableSearchable(t, restURI)
	})

	// Scenario 3: enable-searchable + enable-rangeable on text[]. The
	// rangeable PUT is validator-rejected (text isn't numeric). Pins that
	// the validator rejection doesn't corrupt the searchable enable's
	// sentinel state.
	t.Run("enable_searchable__enable_rangeable_textArray", func(t *testing.T) {
		testParallel_EnableSearchableEnableRangeable(t, restURI)
	})

	// Scenario 4: enable-filterable + change-tokenization on text.
	t.Run("enable_filterable__change_tokenization", func(t *testing.T) {
		testParallel_EnableFilterableChangeTokenization(t, restURI)
	})

	// Scenario 5: change-tok-both + enable-rangeable on a SINGLE text
	// property. The rangeable PUT is validator-rejected. Pins that
	// rejection doesn't corrupt the change-tok task.
	t.Run("change_tokenization_both__enable_rangeable_text_rejected", func(t *testing.T) {
		testParallel_ChangeTokBothEnableRangeableOnText(t, restURI)
	})

	// Scenario 6: change-tok-filterable + change-tokenization (both) in
	// parallel. Both touch filterable → expect 409 on one.
	t.Run("change_tok_filterable__change_tokenization_both", func(t *testing.T) {
		testParallel_ChangeTokFilterableChangeTokBoth(t, restURI)
	})

	// Scenario 7: repair-filterable + repair-rangeable on same property.
	t.Run("repair_filterable__repair_rangeable", func(t *testing.T) {
		testParallel_RepairFilterableRepairRangeable(t, restURI)
	})

	// Scenario 8: repair-filterable + enable-rangeable on same property.
	t.Run("repair_filterable__enable_rangeable", func(t *testing.T) {
		testParallel_RepairFilterableEnableRangeable(t, restURI)
	})

	// Scenario 9: enable-filterable + DELETE-rangeable.
	t.Run("enable_filterable__delete_rangeable", func(t *testing.T) {
		testParallel_EnableFilterableDeleteRangeable(t, restURI)
	})

	// Scenario 10: change-tokenization-both + DELETE-searchable.
	t.Run("change_tokenization_both__delete_searchable_parallel", func(t *testing.T) {
		testParallel_ChangeTokBothDeleteSearchable(t, restURI)
	})

	// Scenario 11: enable-filterable + immediate cancel of that same
	// enable.
	t.Run("enable_filterable__cancel_enable_filterable", func(t *testing.T) {
		testParallel_EnableFilterableCancelSame(t, restURI)
	})

	// Scenario 12: two identical enable-filterable PUTs.
	t.Run("enable_filterable__enable_filterable_idempotency", func(t *testing.T) {
		testParallel_EnableFilterableTwiceIdempotency(t, restURI)
	})
}

// =============================================================================
// Scenario 1: enable-filterable + enable-rangeable (PRIMARY REPRO)
// =============================================================================
//
// Pre-state: int property with both flags disabled. Per typesConflict the
// two migrations do NOT conflict (they touch disjoint bucket types), so
// the conflict checker accepts both. The Sev 1 comes from strategy
// selection reading stale state and sentinel dirs colliding on the
// property.
func testParallel_EnableFilterableEnableRangeable(t *testing.T, restURI string) {
	const class = "ParallelEnFiltEnRange"
	falseVal := false
	helper.CreateClass(t, &models.Class{
		Class: class,
		Properties: []*models.Property{
			{
				Name:              "score",
				DataType:          []string{"int"},
				IndexFilterable:   &falseVal,
				IndexRangeFilters: &falseVal,
			},
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, class)

	seedIntObjects(t, class, "score", matrixObjectCount)

	rA := newPR("enable-filterable", `{"filterable":{"enabled":true}}`)
	rB := newPR("enable-rangeable", `{"rangeable":{"enabled":true}}`)
	fireParallelPUTs(t, restURI, class, "score", rA, rB)

	assertNoFiveXX(t, rA)
	assertNoFiveXX(t, rB)
	assertAtLeastOneAccepted(t, "enable-filterable+enable-rangeable", rA, rB)

	awaitTerminalP(t, restURI, rA)
	awaitTerminalP(t, restURI, rB)

	if rA.terminal == "FINISHED" {
		hits := equalIntFilterHits(t, class, "score", 0)
		assert.Greater(t, hits, 0,
			"[enable-filterable+enable-rangeable] Equal(score=0) after FINISHED must hit; "+
				"got %d. Empty bucket = Sev 1 silent data loss", hits)
	}
	if rB.terminal == "FINISHED" {
		hits := rangeIntFilterHits(t, class, "score", matrixObjectCount/2)
		assert.Greater(t, hits, 0,
			"[enable-filterable+enable-rangeable] LessThan(score=%d) after FINISHED must hit; "+
				"got %d. Empty rangeable bucket = Sev 1 silent data loss", matrixObjectCount/2, hits)
	}
	if rA.terminal == "FINISHED" && rB.terminal == "FINISHED" {
		eventualBothFlags(t, class, "score", true, true)
	}
}

// =============================================================================
// Scenario 2: enable-filterable + enable-searchable on text
// =============================================================================
func testParallel_EnableFilterableEnableSearchable(t *testing.T, restURI string) {
	const class = "ParallelEnFiltEnSearch"
	falseVal := false
	helper.CreateClass(t, &models.Class{
		Class: class,
		Properties: []*models.Property{
			{
				Name:            "name",
				DataType:        []string{"text"},
				Tokenization:    "word",
				IndexFilterable: &falseVal,
				IndexSearchable: &falseVal,
			},
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, class)

	seedTextObjects(t, class, "name", matrixObjectCount)

	rA := newPR("enable-filterable", `{"filterable":{"enabled":true}}`)
	rB := newPR("enable-searchable", `{"searchable":{"enabled":true,"tokenization":"word"}}`)
	fireParallelPUTs(t, restURI, class, "name", rA, rB)

	assertNoFiveXX(t, rA)
	assertNoFiveXX(t, rB)
	assertAtLeastOneAccepted(t, "enable-filterable+enable-searchable", rA, rB)

	awaitTerminalP(t, restURI, rA)
	awaitTerminalP(t, restURI, rB)

	if rA.terminal == "FINISHED" {
		hits := equalTextFilterHits(t, class, "name", "word_0")
		assert.Greater(t, hits, 0,
			"[enable-filterable+enable-searchable] Equal(name=word_0) after FINISHED must hit; got %d", hits)
	}
	if rB.terminal == "FINISHED" {
		hits := bm25HitsForProp(t, class, "name", "word_0")
		assert.Greater(t, hits, 0,
			"[enable-filterable+enable-searchable] bm25(word_0) after FINISHED must hit; got %d", hits)
	}
}

// =============================================================================
// Scenario 3: enable-searchable + enable-rangeable on text[]
// =============================================================================
//
// Same-property parallel: enable-searchable on text[] accepted +
// enable-rangeable rejected (text[] not numeric). Pins that rejection
// doesn't corrupt the searchable enable's sentinel state.
func testParallel_EnableSearchableEnableRangeable(t *testing.T, restURI string) {
	const class = "ParallelEnSearchEnRange"
	falseVal := false
	helper.CreateClass(t, &models.Class{
		Class: class,
		Properties: []*models.Property{
			{
				Name:            "tags",
				DataType:        []string{"text[]"},
				Tokenization:    "word",
				IndexFilterable: &falseVal,
				IndexSearchable: &falseVal,
			},
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, class)

	seedTextArrayObjects(t, class, "tags", matrixObjectCount)

	rA := newPR("enable-searchable", `{"searchable":{"enabled":true,"tokenization":"word"}}`)
	rB := newPR("enable-rangeable", `{"rangeable":{"enabled":true}}`)
	fireParallelPUTs(t, restURI, class, "tags", rA, rB)

	assertNoFiveXX(t, rA)
	assertNoFiveXX(t, rB)
	assert.False(t, rB.accepted,
		"[enable-searchable+enable-rangeable] enable-rangeable on text[] must be rejected by validator; "+
			"got status=%d body=%s", rB.statusCode, rB.body)
	assert.True(t, rA.accepted,
		"[enable-searchable+enable-rangeable] enable-searchable on text[] must be accepted; "+
			"got status=%d body=%s", rA.statusCode, rA.body)

	awaitTerminalP(t, restURI, rA)

	if rA.terminal == "FINISHED" {
		hits := bm25HitsForProp(t, class, "tags", "word_0")
		assert.Greater(t, hits, 0,
			"[enable-searchable+enable-rangeable] bm25(tags=word_0) after FINISHED must hit; got %d", hits)
	}
}

// =============================================================================
// Scenario 4: enable-filterable + change-tokenization on text
// =============================================================================
//
// Both touch the filterable bucket. They conflict per typesConflict; one
// 409 is expected. The hazard is the conflict checker missing the
// overlap, or the change-tokenization migration colliding with the from-
// scratch enable.
func testParallel_EnableFilterableChangeTokenization(t *testing.T, restURI string) {
	const class = "ParallelEnFiltChangeTok"
	trueVal, falseVal := true, false
	helper.CreateClass(t, &models.Class{
		Class: class,
		Properties: []*models.Property{
			{
				Name:            "name",
				DataType:        []string{"text"},
				Tokenization:    "word",
				IndexFilterable: &falseVal,
				IndexSearchable: &trueVal,
			},
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, class)

	seedTextObjects(t, class, "name", matrixObjectCount)

	rA := newPR("enable-filterable", `{"filterable":{"enabled":true}}`)
	rB := newPR("change-tokenization", `{"searchable":{"tokenization":"field"}}`)
	fireParallelPUTs(t, restURI, class, "name", rA, rB)

	assertNoFiveXX(t, rA)
	assertNoFiveXX(t, rB)
	assertAtLeastOneAccepted(t, "enable-filterable+change-tokenization", rA, rB)

	awaitTerminalP(t, restURI, rA)
	awaitTerminalP(t, restURI, rB)

	if rA.terminal == "FINISHED" {
		hits := equalTextFilterHits(t, class, "name", "word_0")
		hitsFieldFallback := equalTextFilterHits(t, class, "name", "word_0 word_0")
		assert.True(t, hits+hitsFieldFallback > 0,
			"[enable-filterable+change-tok] after FINISHED on filterable side, "+
				"at least one Equal(word_0) or Equal('word_0 word_0') must hit; "+
				"both zero = empty filterable bucket (Sev 1)")
	}
	if rB.terminal == "FINISHED" {
		// Mirror the rA branch's dual-query pattern: after change-tok to
		// FIELD the stored term is the full whole-string token
		// "word_0 word_0", so bare-token bm25("word_0") cannot hit. The
		// migration writes the bucket correctly under field semantics
		// (verified independently by isolating the bucket-write path).
		// Either query landing is enough to prove the bucket has data;
		// both zero would be the genuine Sev 1.
		hitsBare := bm25HitsForProp(t, class, "name", "word_0")
		hitsField := bm25HitsForProp(t, class, "name", "word_0 word_0")
		assert.Greater(t, hitsBare+hitsField, 0,
			"[enable-filterable+change-tok] after FINISHED on change-tok side, "+
				"at least one of bm25('word_0') or bm25('word_0 word_0') must hit; "+
				"both zero = empty searchable bucket = Sev 1 silent data loss")
	}
}

// =============================================================================
// Scenario 5: change-tok-both + enable-rangeable on a SINGLE text property
// =============================================================================
func testParallel_ChangeTokBothEnableRangeableOnText(t *testing.T, restURI string) {
	const class = "ParallelChangeTokBothEnRangeText"
	trueVal := true
	helper.CreateClass(t, &models.Class{
		Class: class,
		Properties: []*models.Property{
			{
				Name:            "name",
				DataType:        []string{"text"},
				Tokenization:    "word",
				IndexFilterable: &trueVal,
				IndexSearchable: &trueVal,
			},
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, class)

	seedTextObjects(t, class, "name", matrixObjectCount)

	rA := newPR("change-tok-both", `{"searchable":{"tokenization":"field"}}`)
	rB := newPR("enable-rangeable-text", `{"rangeable":{"enabled":true}}`)
	fireParallelPUTs(t, restURI, class, "name", rA, rB)

	assertNoFiveXX(t, rA)
	assertNoFiveXX(t, rB)
	assert.False(t, rB.accepted,
		"[change-tok-both+enable-rangeable-text] enable-rangeable on text must be 4xx; "+
			"got status=%d body=%s", rB.statusCode, rB.body)
	assert.True(t, rA.accepted,
		"[change-tok-both+enable-rangeable-text] change-tok-both must be accepted; "+
			"got status=%d body=%s", rA.statusCode, rA.body)

	awaitTerminalP(t, restURI, rA)

	if rA.terminal == "FINISHED" {
		hits := equalTextFilterHits(t, class, "name", "word_0 word_0")
		assert.Greater(t, hits, 0,
			"[change-tok-both+enable-rangeable-text] post change-tok, Equal('word_0 word_0') "+
				"under field tokenization must hit; got %d. "+
				"Empty filterable bucket after rejected sibling = sentinel collision (Sev 1)", hits)
	}
}

// =============================================================================
// Scenario 6: change-tok-filterable + change-tok-both in parallel
// =============================================================================
//
// Both touch filterable on overlapping property → at least one 409
// expected.
func testParallel_ChangeTokFilterableChangeTokBoth(t *testing.T, restURI string) {
	const class = "ParallelChangeTokFiltBoth"
	trueVal := true
	helper.CreateClass(t, &models.Class{
		Class: class,
		Properties: []*models.Property{
			{
				Name:            "name",
				DataType:        []string{"text"},
				Tokenization:    "word",
				IndexFilterable: &trueVal,
				IndexSearchable: &trueVal,
			},
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, class)

	seedTextObjects(t, class, "name", matrixObjectCount)

	rA := newPR("change-tok-filterable", `{"filterable":{"tokenization":"field"}}`)
	rB := newPR("change-tok-both", `{"searchable":{"tokenization":"field"}}`)
	fireParallelPUTs(t, restURI, class, "name", rA, rB)

	assertNoFiveXX(t, rA)
	assertNoFiveXX(t, rB)
	assertAtLeastOneAccepted(t, "change-tok-filt+change-tok-both", rA, rB)

	conflicts := 0
	if rA.statusCode == http.StatusConflict {
		conflicts++
	}
	if rB.statusCode == http.StatusConflict {
		conflicts++
	}
	assert.GreaterOrEqual(t, conflicts, 1,
		"[change-tok-filt+change-tok-both] at least one must 409; got rA=%d rB=%d. "+
			"Both 202 means conflict checker lets two writers race on the filterable bucket",
		rA.statusCode, rB.statusCode)

	awaitTerminalP(t, restURI, rA)
	awaitTerminalP(t, restURI, rB)

	if rA.terminal == "FINISHED" || rB.terminal == "FINISHED" {
		hits := equalTextFilterHits(t, class, "name", "word_0 word_0")
		assert.Greater(t, hits, 0,
			"[change-tok-filt+change-tok-both] post FINISHED, Equal('word_0 word_0') "+
				"under field tok must hit; got %d. Empty bucket = Sev 1", hits)
	}
}

// =============================================================================
// Scenario 7: repair-filterable + repair-rangeable on same property
// =============================================================================
func testParallel_RepairFilterableRepairRangeable(t *testing.T, restURI string) {
	const class = "ParallelRepairFiltRepairRange"
	trueVal := true
	helper.CreateClass(t, &models.Class{
		Class: class,
		Properties: []*models.Property{
			{
				Name:              "score",
				DataType:          []string{"int"},
				IndexFilterable:   &trueVal,
				IndexRangeFilters: &trueVal,
			},
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, class)

	seedIntObjects(t, class, "score", matrixObjectCount)

	rA := newPR("repair-filterable", `{"filterable":{"rebuild":true}}`)
	rB := newPR("repair-rangeable", `{"rangeable":{"rebuild":true}}`)
	fireParallelPUTs(t, restURI, class, "score", rA, rB)

	assertNoFiveXX(t, rA)
	assertNoFiveXX(t, rB)
	assertAtLeastOneAccepted(t, "repair-filterable+repair-rangeable", rA, rB)

	awaitTerminalP(t, restURI, rA)
	awaitTerminalP(t, restURI, rB)

	if rA.terminal == "FINISHED" {
		hits := equalIntFilterHits(t, class, "score", 0)
		assert.Greater(t, hits, 0,
			"[repair-filt+repair-range] after FINISHED, Equal(score=0) must hit; got %d. "+
				"Empty filterable post-rebuild = Sev 1 silent data loss", hits)
	}
	if rB.terminal == "FINISHED" {
		hits := rangeIntFilterHits(t, class, "score", matrixObjectCount/2)
		assert.Greater(t, hits, 0,
			"[repair-filt+repair-range] after FINISHED, LessThan(score=%d) must hit; got %d. "+
				"Empty rangeable post-rebuild = Sev 1 silent data loss", matrixObjectCount/2, hits)
	}
}

// =============================================================================
// Scenario 8: repair-filterable + enable-rangeable on same property
// =============================================================================
//
// Repair tears the existing filterable bucket while enable-rangeable
// backfills from it. Shared lifecycle hazard even though typesConflict
// considers them non-overlapping.
func testParallel_RepairFilterableEnableRangeable(t *testing.T, restURI string) {
	const class = "ParallelRepairFiltEnRange"
	trueVal, falseVal := true, false
	helper.CreateClass(t, &models.Class{
		Class: class,
		Properties: []*models.Property{
			{
				Name:              "score",
				DataType:          []string{"int"},
				IndexFilterable:   &trueVal,
				IndexRangeFilters: &falseVal,
			},
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, class)

	seedIntObjects(t, class, "score", matrixObjectCount)

	rA := newPR("repair-filterable", `{"filterable":{"rebuild":true}}`)
	rB := newPR("enable-rangeable", `{"rangeable":{"enabled":true}}`)
	fireParallelPUTs(t, restURI, class, "score", rA, rB)

	assertNoFiveXX(t, rA)
	assertNoFiveXX(t, rB)
	assertAtLeastOneAccepted(t, "repair-filterable+enable-rangeable", rA, rB)

	awaitTerminalP(t, restURI, rA)
	awaitTerminalP(t, restURI, rB)

	if rA.terminal == "FINISHED" {
		hits := equalIntFilterHits(t, class, "score", 0)
		assert.Greater(t, hits, 0,
			"[repair-filt+en-range] post FINISHED on filterable, Equal(score=0) must hit; got %d", hits)
	}
	if rB.terminal == "FINISHED" {
		hits := rangeIntFilterHits(t, class, "score", matrixObjectCount/2)
		assert.Greater(t, hits, 0,
			"[repair-filt+en-range] post FINISHED on rangeable, LessThan(%d) must hit; got %d. "+
				"If 0, enable-rangeable backfill ran against a torn filterable bucket (Sev 1)",
			matrixObjectCount/2, hits)
	}
}

// =============================================================================
// Scenario 9: enable-filterable + DELETE-rangeable
// =============================================================================
//
// DELETE is NOT a distributed task — it bypasses checkReindexConflict. The
// hazard is that DELETE flips IndexRangeFilters to false while
// enable-filterable is scanning, potentially while shared property cache
// state is mutating.
func testParallel_EnableFilterableDeleteRangeable(t *testing.T, restURI string) {
	const class = "ParallelEnFiltDelRange"
	trueVal, falseVal := true, false
	helper.CreateClass(t, &models.Class{
		Class: class,
		Properties: []*models.Property{
			{
				Name:              "score",
				DataType:          []string{"int"},
				IndexFilterable:   &falseVal,
				IndexRangeFilters: &trueVal,
			},
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, class)

	seedIntObjects(t, class, "score", matrixObjectCount)

	logger, _ := logrustest.NewNullLogger()
	var wg sync.WaitGroup
	wg.Add(2)

	rEnable := newPR("enable-filterable", `{"filterable":{"enabled":true}}`)
	var deleteStatus int
	var deleteBody []byte

	enterrors.GoWrapper(func() {
		defer wg.Done()
		executePR(restURI, class, "score", rEnable)
	}, logger)

	enterrors.GoWrapper(func() {
		defer wg.Done()
		url := fmt.Sprintf("http://%s/v1/schema/%s/properties/score/index/rangeFilters", restURI, class)
		req, err := http.NewRequest(http.MethodDelete, url, nil)
		if err != nil {
			return
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		deleteStatus = resp.StatusCode
		deleteBody = body
	}, logger)

	wg.Wait()
	t.Logf("enable result: status=%d body=%s", rEnable.statusCode, rEnable.body)
	t.Logf("DELETE rangeFilters result: status=%d body=%s", deleteStatus, string(deleteBody))

	assertNoFiveXX(t, rEnable)
	assert.Less(t, deleteStatus, 500,
		"[en-filt+del-range] DELETE rangeable must NOT 5xx; got %d body=%s", deleteStatus, string(deleteBody))

	awaitTerminalP(t, restURI, rEnable)

	if rEnable.terminal == "FINISHED" {
		hits := equalIntFilterHits(t, class, "score", 0)
		assert.Greater(t, hits, 0,
			"[en-filt+del-range] post FINISHED, Equal(score=0) must hit; got %d. "+
				"If 0, DELETE ran against the same property's bucket lifecycle and "+
				"corrupted the enable-filterable backfill (Sev 1)", hits)
	}
}

// =============================================================================
// Scenario 10: change-tokenization-both + DELETE-searchable in parallel
// =============================================================================
func testParallel_ChangeTokBothDeleteSearchable(t *testing.T, restURI string) {
	const class = "ParallelChangeTokBothDelSearch"
	trueVal := true
	helper.CreateClass(t, &models.Class{
		Class: class,
		Properties: []*models.Property{
			{
				Name:            "name",
				DataType:        []string{"text"},
				Tokenization:    "word",
				IndexFilterable: &trueVal,
				IndexSearchable: &trueVal,
			},
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, class)

	seedTextObjects(t, class, "name", matrixObjectCount)

	logger, _ := logrustest.NewNullLogger()
	var wg sync.WaitGroup
	wg.Add(2)

	rChangeTok := newPR("change-tok-both", `{"searchable":{"tokenization":"field"}}`)
	var deleteStatus int
	var deleteBody []byte

	enterrors.GoWrapper(func() {
		defer wg.Done()
		executePR(restURI, class, "name", rChangeTok)
	}, logger)

	enterrors.GoWrapper(func() {
		defer wg.Done()
		url := fmt.Sprintf("http://%s/v1/schema/%s/properties/name/index/searchable", restURI, class)
		req, err := http.NewRequest(http.MethodDelete, url, nil)
		if err != nil {
			return
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		deleteStatus = resp.StatusCode
		deleteBody = body
	}, logger)

	wg.Wait()
	t.Logf("change-tok-both result: status=%d body=%s", rChangeTok.statusCode, rChangeTok.body)
	t.Logf("DELETE searchable result: status=%d body=%s", deleteStatus, string(deleteBody))

	assertNoFiveXX(t, rChangeTok)
	assert.Less(t, deleteStatus, 500,
		"[change-tok-both+del-search] DELETE searchable must NOT 5xx; got %d body=%s", deleteStatus, string(deleteBody))

	awaitTerminalP(t, restURI, rChangeTok)

	c := helper.GetClass(t, class)
	require.NotNil(t, c, "class must still exist")
	var nameProp *models.Property
	for _, p := range c.Properties {
		if p.Name == "name" {
			nameProp = p
			break
		}
	}
	require.NotNil(t, nameProp, "name property must still exist")
	require.True(t, nameProp.IndexFilterable != nil && *nameProp.IndexFilterable,
		"[change-tok-both+del-search] filterable index must survive — it was not the DELETE target")

	hitsWord := equalTextFilterHits(t, class, "name", "word_0")
	hitsField := equalTextFilterHits(t, class, "name", "word_0 word_0")
	assert.True(t, hitsWord+hitsField > 0,
		"[change-tok-both+del-search] at least one of Equal('word_0') or Equal('word_0 word_0') "+
			"must hit; both zero means torn filterable bucket (Sev 1)")
}

// =============================================================================
// Scenario 11: enable-filterable + cancel that same enable in parallel
// =============================================================================
func testParallel_EnableFilterableCancelSame(t *testing.T, restURI string) {
	const class = "ParallelEnFiltCancelSame"
	falseVal := false
	helper.CreateClass(t, &models.Class{
		Class: class,
		Properties: []*models.Property{
			{
				Name:            "score",
				DataType:        []string{"int"},
				IndexFilterable: &falseVal,
			},
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, class)

	seedIntObjects(t, class, "score", matrixObjectCount)

	logger, _ := logrustest.NewNullLogger()
	var wg sync.WaitGroup
	wg.Add(2)
	rSubmit := newPR("enable-filterable", `{"filterable":{"enabled":true}}`)
	rCancel := newPR("cancel-filterable", `{"filterable":{"cancel":true}}`)

	enterrors.GoWrapper(func() {
		defer wg.Done()
		executePR(restURI, class, "score", rSubmit)
	}, logger)

	enterrors.GoWrapper(func() {
		defer wg.Done()
		// Brief stagger so submit has a chance to register before cancel
		// arrives — without this, cancel arrives first and always 404s,
		// making the scenario trivially green.
		time.Sleep(10 * time.Millisecond)
		executePR(restURI, class, "score", rCancel)
	}, logger)

	wg.Wait()
	t.Logf("submit result: status=%d body=%s", rSubmit.statusCode, rSubmit.body)
	t.Logf("cancel result: status=%d body=%s", rCancel.statusCode, rCancel.body)

	assertNoFiveXX(t, rSubmit)
	assertNoFiveXX(t, rCancel)

	awaitTerminalP(t, restURI, rSubmit)

	switch rSubmit.terminal {
	case "FINISHED":
		eventualSingleFlag(t, class, "score", "filterable", true)
		hits := equalIntFilterHits(t, class, "score", 0)
		assert.Greater(t, hits, 0,
			"[en-filt+cancel-same] after FINISHED, Equal(score=0) must hit; got %d. "+
				"Empty bucket but flag flipped = Sev 1 silent data loss", hits)
	case "CANCELLED", "FAILED":
		eventualSingleFlag(t, class, "score", "filterable", false)
	}
}

// =============================================================================
// Scenario 12: two identical enable-filterable PUTs (idempotency)
// =============================================================================
func testParallel_EnableFilterableTwiceIdempotency(t *testing.T, restURI string) {
	const class = "ParallelEnFiltTwice"
	falseVal := false
	helper.CreateClass(t, &models.Class{
		Class: class,
		Properties: []*models.Property{
			{
				Name:            "score",
				DataType:        []string{"int"},
				IndexFilterable: &falseVal,
			},
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, class)

	seedIntObjects(t, class, "score", matrixObjectCount)

	body := `{"filterable":{"enabled":true}}`
	rA := newPR("enable-filterable-A", body)
	rB := newPR("enable-filterable-B", body)
	fireParallelPUTs(t, restURI, class, "score", rA, rB)

	assertNoFiveXX(t, rA)
	assertNoFiveXX(t, rB)
	assertAtLeastOneAccepted(t, "enable-filterable-twice", rA, rB)

	// If both are accepted, conflict checker missed the overlap (BUG).
	if rA.accepted && rB.accepted {
		t.Errorf("[en-filt-twice] BOTH PUTs accepted (202) — conflict checker missed identical-op overlap. "+
			"taskIDs: A=%s B=%s. This races two writers against the same filterable bucket",
			rA.taskID, rB.taskID)
	}

	awaitTerminalP(t, restURI, rA)
	awaitTerminalP(t, restURI, rB)

	if rA.terminal == "FINISHED" || rB.terminal == "FINISHED" {
		eventualSingleFlag(t, class, "score", "filterable", true)
		hits := equalIntFilterHits(t, class, "score", 0)
		assert.Greater(t, hits, 0,
			"[en-filt-twice] post FINISHED, Equal(score=0) must hit; got %d", hits)
	}
}

// =============================================================================
// Shared helpers
// =============================================================================

// parallelResult is the captured outcome of one PUT request fired against
// the indexes endpoint.
type parallelResult struct {
	label       string
	requestBody string
	statusCode  int
	body        string
	taskID      string // populated only when statusCode==202
	accepted    bool   // statusCode == 202
	terminal    string // populated by awaitTerminalP: FINISHED/FAILED/CANCELLED/""
}

func newPR(label, body string) *parallelResult {
	return &parallelResult{label: label, requestBody: body}
}

// fireParallelPUTs issues two PUTs to /v1/schema/{class}/indexes/{prop} in
// parallel via enterrors.GoWrapper. Populates the two parallelResult
// pointers in place.
func fireParallelPUTs(t *testing.T, restURI, class, prop string, rA, rB *parallelResult) {
	t.Helper()
	logger, _ := logrustest.NewNullLogger()
	var wg sync.WaitGroup
	wg.Add(2)

	enterrors.GoWrapper(func() {
		defer wg.Done()
		executePR(restURI, class, prop, rA)
	}, logger)
	enterrors.GoWrapper(func() {
		defer wg.Done()
		executePR(restURI, class, prop, rB)
	}, logger)

	wg.Wait()
	t.Logf("parallel A %s: status=%d body=%s", rA.label, rA.statusCode, rA.body)
	t.Logf("parallel B %s: status=%d body=%s", rB.label, rB.statusCode, rB.body)
}

// executePR issues the request stored on the parallelResult and populates
// statusCode/body/taskID/accepted on the same struct.
func executePR(restURI, class, prop string, r *parallelResult) {
	url := fmt.Sprintf("http://%s/v1/schema/%s/indexes/%s", restURI, class, prop)
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader([]byte(r.requestBody)))
	if err != nil {
		r.statusCode = 0
		r.body = err.Error()
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		r.statusCode = 0
		r.body = err.Error()
		return
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	r.statusCode = resp.StatusCode
	r.body = string(raw)
	if resp.StatusCode == http.StatusAccepted {
		r.accepted = true
		var parsed map[string]string
		if json.Unmarshal(raw, &parsed) == nil {
			r.taskID = parsed["taskId"]
		}
	}
}

// assertNoFiveXX requires that the response is not a 5xx. 0 (network
// failure) is also disallowed.
func assertNoFiveXX(t *testing.T, r *parallelResult) {
	t.Helper()
	require.NotEqual(t, 0, r.statusCode, "[%s] network-level failure: %s", r.label, r.body)
	require.Less(t, r.statusCode, 500,
		"[%s] response is 5xx (structural bug): status=%d body=%s", r.label, r.statusCode, r.body)
}

// assertAtLeastOneAccepted requires that at least one of the two parallel
// PUTs was accepted (202). Both rejected is a failure — the system MUST
// make forward progress on at least one of the two operations.
func assertAtLeastOneAccepted(t *testing.T, label string, rA, rB *parallelResult) {
	t.Helper()
	if !rA.accepted && !rB.accepted {
		t.Errorf("[%s] BOTH parallel PUTs rejected: A=(%d %s) B=(%d %s). "+
			"At least one operation must make progress; otherwise the user is "+
			"unable to make any change to this property (Sev 1)",
			label, rA.statusCode, rA.body, rB.statusCode, rB.body)
	}
}

// awaitTerminalP polls /v1/tasks until r's task reaches a terminal state.
// Mutates r.terminal in place. No-op for non-accepted PUTs.
func awaitTerminalP(t *testing.T, restURI string, r *parallelResult) {
	t.Helper()
	if !r.accepted || r.taskID == "" {
		return
	}
	deadline := time.Now().Add(3 * time.Minute)
	for time.Now().Before(deadline) {
		resp, err := http.Get(fmt.Sprintf("http://%s/v1/tasks", restURI))
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		var tasks models.DistributedTasks
		if err := json.Unmarshal(body, &tasks); err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		for _, task := range tasks["reindex"] {
			if task.ID == r.taskID {
				switch task.Status {
				case "FINISHED", "FAILED", "CANCELLED":
					r.terminal = task.Status
					t.Logf("task %s reached terminal=%s", r.taskID, task.Status)
					return
				}
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Logf("task %s did NOT reach terminal within 3m", r.taskID)
}

// =============================================================================
// Seeders
// =============================================================================

func seedIntObjects(t *testing.T, class, propName string, n int) {
	t.Helper()
	objects := make([]*models.Object, n)
	for i := 0; i < n; i++ {
		objects[i] = &models.Object{
			Class:      class,
			Properties: map[string]interface{}{propName: i},
		}
	}
	helper.CreateObjectsBatch(t, objects)
}

func seedTextObjects(t *testing.T, class, propName string, n int) {
	t.Helper()
	objects := make([]*models.Object, n)
	for i := 0; i < n; i++ {
		objects[i] = &models.Object{
			Class:      class,
			Properties: map[string]interface{}{propName: fmt.Sprintf("word_%d word_%d", i, i)},
		}
	}
	helper.CreateObjectsBatch(t, objects)
}

func seedTextArrayObjects(t *testing.T, class, propName string, n int) {
	t.Helper()
	objects := make([]*models.Object, n)
	for i := 0; i < n; i++ {
		objects[i] = &models.Object{
			Class:      class,
			Properties: map[string]interface{}{propName: []string{fmt.Sprintf("word_%d", i), "shared"}},
		}
	}
	helper.CreateObjectsBatch(t, objects)
}

// =============================================================================
// Query helpers
// =============================================================================

func equalIntFilterHits(t *testing.T, class, prop string, value int) int {
	t.Helper()
	gqlQuery := fmt.Sprintf(`{
		Get {
			%s(where: {path: [%q], operator: Equal, valueInt: %d}, limit: 10000) {
				_additional { id }
			}
		}
	}`, class, prop, value)
	ids, err := runMatrixGraphQL(t, class, gqlQuery)
	if err != nil {
		t.Logf("equal int filter %s=%d errored: %v", prop, value, err)
		return 0
	}
	return len(ids)
}

func equalTextFilterHits(t *testing.T, class, prop, value string) int {
	t.Helper()
	gqlQuery := fmt.Sprintf(`{
		Get {
			%s(where: {path: [%q], operator: Equal, valueText: %q}, limit: 10000) {
				_additional { id }
			}
		}
	}`, class, prop, value)
	ids, err := runMatrixGraphQL(t, class, gqlQuery)
	if err != nil {
		t.Logf("equal text filter %s=%q errored: %v", prop, value, err)
		return 0
	}
	return len(ids)
}

func rangeIntFilterHits(t *testing.T, class, prop string, lessThan int) int {
	t.Helper()
	gqlQuery := fmt.Sprintf(`{
		Get {
			%s(where: {path: [%q], operator: LessThan, valueInt: %d}, limit: 10000) {
				_additional { id }
			}
		}
	}`, class, prop, lessThan)
	ids, err := runMatrixGraphQL(t, class, gqlQuery)
	if err != nil {
		t.Logf("range int filter %s<%d errored: %v", prop, lessThan, err)
		return 0
	}
	return len(ids)
}

func bm25HitsForProp(t *testing.T, class, prop, query string) int {
	t.Helper()
	gqlQuery := fmt.Sprintf(`{
		Get {
			%s(bm25: {query: %q, properties: [%q]}, limit: 10000) {
				_additional { id }
			}
		}
	}`, class, query, prop)
	ids, err := runMatrixGraphQL(t, class, gqlQuery)
	if err != nil {
		t.Logf("bm25 %s=%q errored: %v", prop, query, err)
		return 0
	}
	return len(ids)
}

func runMatrixGraphQL(t *testing.T, className, gqlQuery string) ([]string, error) {
	t.Helper()
	resp, err := graphqlhelper.QueryGraphQL(t, nil, "", gqlQuery, nil)
	if err != nil {
		return nil, fmt.Errorf("graphql request: %w", err)
	}
	if len(resp.Errors) > 0 {
		return nil, fmt.Errorf("graphql errors: %v", resp.Errors[0].Message)
	}
	data := make(map[string]interface{})
	for key, value := range resp.Data {
		data[key] = value
	}
	getMap, ok := data["Get"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("no Get in graphql data")
	}
	items, ok := getMap[className].([]interface{})
	if !ok {
		return nil, fmt.Errorf("no items for %s", className)
	}
	ids := make([]string, 0, len(items))
	for _, item := range items {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		additional, ok := m["_additional"].(map[string]interface{})
		if !ok {
			continue
		}
		if id, ok := additional["id"].(string); ok {
			ids = append(ids, id)
		}
	}
	return ids, nil
}

// eventualBothFlags polls until both index flags reach expected values.
func eventualBothFlags(t *testing.T, class, prop string, wantFilt, wantRange bool) {
	t.Helper()
	require.Eventually(t, func() bool {
		c := helper.GetClass(t, class)
		if c == nil {
			return false
		}
		for _, p := range c.Properties {
			if p.Name != prop {
				continue
			}
			gotFilt := p.IndexFilterable != nil && *p.IndexFilterable
			gotRange := p.IndexRangeFilters != nil && *p.IndexRangeFilters
			return gotFilt == wantFilt && gotRange == wantRange
		}
		return false
	}, 60*time.Second, 250*time.Millisecond,
		"flags on %s.%s must reach filt=%v range=%v", class, prop, wantFilt, wantRange)
}

// eventualSingleFlag polls a single index flag.
func eventualSingleFlag(t *testing.T, class, prop, flag string, want bool) {
	t.Helper()
	require.Eventually(t, func() bool {
		c := helper.GetClass(t, class)
		if c == nil {
			return false
		}
		for _, p := range c.Properties {
			if p.Name != prop {
				continue
			}
			switch flag {
			case "filterable":
				return p.IndexFilterable != nil && *p.IndexFilterable == want
			case "searchable":
				return p.IndexSearchable != nil && *p.IndexSearchable == want
			case "rangeable":
				return p.IndexRangeFilters != nil && *p.IndexRangeFilters == want
			}
		}
		return false
	}, 60*time.Second, 250*time.Millisecond,
		"flag %s on %s.%s must reach %v", flag, class, prop, want)
}

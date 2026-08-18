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

// Package reindex_mt tests runtime reindex on multi-tenant collections.
// Each test creates its own MT collection with isolated tenants.
package reindex_mt

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
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

func TestMultiTenant_ReindexSuite(t *testing.T) {
	ctx := context.Background()

	compose, err := reindexhelpers.StartSingleNode(ctx)
	require.NoError(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	restURI := compose.GetWeaviate().URI()
	container := compose.GetWeaviate().Container()

	// Dump container logs on failure.
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
			if len(lines) > 200 {
				lines = lines[len(lines)-200:]
			}
			t.Logf("=== Container logs (last 200 lines) ===\n%s", strings.Join(lines, "\n"))
		}
	}()

	t.Run("RepairAllTenants", func(t *testing.T) {
		testRepairAllTenants(t, restURI)
	})

	t.Run("RepairSpecificTenants", func(t *testing.T) {
		testRepairSpecificTenants(t, restURI)
	})

	t.Run("ChangeTokenization", func(t *testing.T) {
		testChangeTokenizationMT(t, restURI)
	})

	t.Run("EnableRangeable", func(t *testing.T) {
		testEnableRangeableMT(t, restURI)
	})

	t.Run("Validation", func(t *testing.T) {
		testValidation(t, restURI)
	})

	// Cancel → deactivate → restart → re-submit, across the three tenant
	// populations the cleanup sweep answers differently. Runs on its own
	// compose (needs forced-lazy loading; the rest of this suite needs
	// eager loading for its own coverage) and restores the suite's client
	// afterward.
	t.Run("ColdAndUnhydratedTenantCancel", func(t *testing.T) {
		testColdAndUnhydratedTenantCancel(t)
		helper.SetupClient(restURI)
	})

	t.Run("TenantScopedRebuildCancel", func(t *testing.T) {
		testTenantScopedRebuildCancel(t, restURI)
	})

	// Crashes its node and needs forced-lazy loading, so it runs on its own
	// compose and restores the suite's client afterward.
	t.Run("TwoLoadsBeforeTheSchemaFlip", func(t *testing.T) {
		testTwoLoadsBeforeTheSchemaFlip(ctx, t)
		helper.SetupClient(restURI)
	})

	// Restart for deferred finalization.
	t.Run("PostRestart", func(t *testing.T) {
		t.Log("restarting container for deferred finalize")
		require.NoError(t, compose.StopAt(ctx, 0, nil))
		require.NoError(t, compose.StartAt(ctx, 0))
		helper.SetupClient(compose.GetWeaviate().URI())
		restURI = compose.GetWeaviate().URI()

		// Verify change-tokenization survived restart.
		testChangeTokenizationMTPostRestart(t, restURI)
		// Verify enable-rangeable survived restart.
		testEnableRangeableMTPostRestart(t, restURI)
	})
}

// =============================================================================
// Test 1: Repair all tenants
// =============================================================================

func testRepairAllTenants(t *testing.T, restURI string) {
	className := "MTRepairAll"
	tenantNames := []string{"tenantA", "tenantB", "tenantC", "tenantD", "tenantE"}

	createMTClass(t, className, []*models.Property{
		{Name: "text", DataType: []string{"text"}, Tokenization: "word"},
	})
	addTenants(t, className, tenantNames)

	// Insert 5 objects per tenant.
	for _, tn := range tenantNames {
		for i := 0; i < 5; i++ {
			obj := &models.Object{
				Class:      className,
				Properties: map[string]interface{}{"text": fmt.Sprintf("doc_%d for %s", i, tn)},
				Tenant:     tn,
			}
			require.NoError(t, helper.CreateObject(t, obj))
		}
	}

	// Verify data exists.
	for _, tn := range tenantNames {
		ids := bm25QueryTenant(t, className, "text", "doc", tn)
		require.Len(t, ids, 5, "tenant %s should have 5 objects", tn)
	}

	// Submit repair-searchable (no tenants param → all tenants).
	taskID := reindexhelpers.SubmitIndexUpsert(t, restURI, className, "text", "searchable", `{"algorithm":"blockmax"}`)
	t.Logf("repair all tenants task: %s", taskID)
	reindexhelpers.AwaitReindexFinished(t, restURI, taskID)

	// Verify data still intact.
	for _, tn := range tenantNames {
		ids := bm25QueryTenant(t, className, "text", "doc", tn)
		assert.Len(t, ids, 5, "tenant %s should still have 5 objects after repair", tn)
	}
}

// =============================================================================
// Test 2: Repair specific tenants
// =============================================================================

func testRepairSpecificTenants(t *testing.T, restURI string) {
	// Per-tenant filter dispatch via enable-rangeable (format-only).
	// ChangeAlgorithm + tenant subset is rejected post weaviate/0-weaviate-issues#254.
	className := "MTRepairSpecific"
	tenantNames := []string{"t1", "t2", "t3", "t4", "t5"}

	createMTClass(t, className, []*models.Property{
		{Name: "text", DataType: []string{"text"}, Tokenization: "word"},
		{Name: "score", DataType: []string{"int"}},
	})
	addTenants(t, className, tenantNames)

	for _, tn := range tenantNames {
		for i := 0; i < 3; i++ {
			obj := &models.Object{
				Class: className,
				Properties: map[string]interface{}{
					"text":  fmt.Sprintf("item_%d from %s", i, tn),
					"score": float64(i + 1),
				},
				Tenant: tn,
			}
			require.NoError(t, helper.CreateObject(t, obj))
		}
	}

	// Repair only t1 and t2 via enable-rangeFilters on the int property.
	targetTenants := []string{"t1", "t2"}
	taskID := reindexhelpers.SubmitIndexUpsert(t, restURI, className, "score",
		"rangeFilters", `{}`, reindexhelpers.WithTenants(targetTenants))
	t.Logf("repair specific tenants task: %s", taskID)
	reindexhelpers.AwaitReindexFinished(t, restURI, taskID)

	// All tenants should still have data.
	for _, tn := range tenantNames {
		ids := bm25QueryTenant(t, className, "text", "item", tn)
		assert.Len(t, ids, 3, "tenant %s should have 3 objects", tn)
	}
}

// =============================================================================
// Test 3: Change tokenization (MT, all tenants, grouped barrier)
// =============================================================================

// Store baselines for post-restart checks.
var changeTokenMTBaselines struct {
	className   string
	tenantNames []string
	// Post-migration expected: field tokenization on "filepath".
	postFullPathIDs map[string][]string // tenant -> IDs from full path BM25 query
}

func testChangeTokenizationMT(t *testing.T, restURI string) {
	className := "MTRetokenize"
	tenantNames := []string{"ct1", "ct2", "ct3"}
	changeTokenMTBaselines.className = className
	changeTokenMTBaselines.tenantNames = tenantNames
	changeTokenMTBaselines.postFullPathIDs = make(map[string][]string)

	createMTClass(t, className, []*models.Property{
		{Name: "filepath", DataType: []string{"text"}, Tokenization: "word"},
		{Name: "description", DataType: []string{"text"}, Tokenization: "word"},
	})
	addTenants(t, className, tenantNames)

	filepaths := []string{
		"/code/github.com/weaviate/weaviate/main.go",
		"/code/github.com/other/project/main.go",
		"/code/docs/tutorial/getting_started.md",
	}
	for _, tn := range tenantNames {
		for i, fp := range filepaths {
			obj := &models.Object{
				Class: className,
				Properties: map[string]interface{}{
					"filepath":    fp,
					"description": fmt.Sprintf("file %d for %s", i, tn),
				},
				Tenant: tn,
			}
			require.NoError(t, helper.CreateObject(t, obj))
		}
	}

	// Pre-migration: "weaviate" matches via word tokenization.
	for _, tn := range tenantNames {
		ids := bm25QueryTenant(t, className, "filepath", "weaviate", tn)
		require.NotEmpty(t, ids, "pre-migration: tenant %s should find 'weaviate'", tn)
	}

	// Change tokenization to field (must target all tenants).
	taskID := reindexhelpers.SubmitIndexUpsert(t, restURI, className, "filepath",
		"searchable", `{"tokenization":"field"}`)
	t.Logf("change tokenization MT task: %s", taskID)
	reindexhelpers.AwaitReindexFinished(t, restURI, taskID)

	// Wait for schema update.
	require.Eventually(t, func() bool {
		cls := helper.GetClass(t, className)
		for _, prop := range cls.Properties {
			if prop.Name == "filepath" {
				return prop.Tokenization == "field"
			}
		}
		return false
	}, 30*time.Second, 1*time.Second, "tokenization should change to field")

	// Post-migration: "weaviate" should NOT match (field tokenization).
	for _, tn := range tenantNames {
		ids := bm25QueryTenant(t, className, "filepath", "weaviate", tn)
		assert.Empty(t, ids, "post-migration: tenant %s should NOT find 'weaviate' with field tokenization", tn)
	}

	// Post-migration: full path should match exactly 1.
	for _, tn := range tenantNames {
		ids := bm25QueryTenant(t, className, "filepath",
			"/code/github.com/weaviate/weaviate/main.go", tn)
		assert.Len(t, ids, 1, "post-migration: tenant %s should find exactly 1 for full path", tn)
		changeTokenMTBaselines.postFullPathIDs[tn] = ids
	}
}

func testChangeTokenizationMTPostRestart(t *testing.T, restURI string) {
	className := changeTokenMTBaselines.className
	if className == "" {
		t.Skip("change tokenization baselines not set")
	}

	helper.SetupClient(restURI)

	// Schema should still show field tokenization.
	cls := helper.GetClass(t, className)
	for _, prop := range cls.Properties {
		if prop.Name == "filepath" {
			assert.Equal(t, "field", prop.Tokenization, "post-restart: should be field")
		}
	}

	// Queries should still work.
	for _, tn := range changeTokenMTBaselines.tenantNames {
		ids := bm25QueryTenant(t, className, "filepath", "weaviate", tn)
		assert.Empty(t, ids, "post-restart: tenant %s should NOT find 'weaviate'", tn)

		ids = bm25QueryTenant(t, className, "filepath",
			"/code/github.com/weaviate/weaviate/main.go", tn)
		assert.Len(t, ids, 1, "post-restart: tenant %s should find 1 for full path", tn)
	}
}

// =============================================================================
// Test 4: Enable rangeable (MT)
// =============================================================================

var enableRangeableMTBaselines struct {
	className   string
	tenantNames []string
}

func testEnableRangeableMT(t *testing.T, restURI string) {
	className := "MTRangeable"
	tenantNames := []string{"rt1", "rt2", "rt3"}
	enableRangeableMTBaselines.className = className
	enableRangeableMTBaselines.tenantNames = tenantNames

	createMTClass(t, className, []*models.Property{
		{Name: "name", DataType: []string{"text"}},
		{Name: "score", DataType: []string{"int"}},
	})
	addTenants(t, className, tenantNames)

	for _, tn := range tenantNames {
		for i := 0; i < 10; i++ {
			obj := &models.Object{
				Class: className,
				Properties: map[string]interface{}{
					"name":  fmt.Sprintf("item_%d", i),
					"score": float64(i + 1),
				},
				Tenant: tn,
			}
			require.NoError(t, helper.CreateObject(t, obj))
		}
	}

	// Pre-migration: range queries work via filterable (slower but functional).
	for _, tn := range tenantNames {
		ids := rangeQueryTenant(t, className, tn,
			`{path:["score"], operator:GreaterThan, valueInt:5}`)
		require.Len(t, ids, 5, "tenant %s should have 5 items with score>5", tn)
	}

	// Enable rangeFilters.
	taskID := reindexhelpers.SubmitIndexUpsert(t, restURI, className, "score",
		"rangeFilters", `{}`)
	t.Logf("enable rangeable MT task: %s", taskID)
	reindexhelpers.AwaitReindexFinished(t, restURI, taskID)

	// Schema should show indexRangeFilters=true.
	require.Eventually(t, func() bool {
		cls := helper.GetClass(t, className)
		for _, prop := range cls.Properties {
			if prop.Name == "score" {
				return prop.IndexRangeFilters != nil && *prop.IndexRangeFilters
			}
		}
		return false
	}, 30*time.Second, 1*time.Second)

	// Range queries should still work.
	for _, tn := range tenantNames {
		ids := rangeQueryTenant(t, className, tn,
			`{path:["score"], operator:GreaterThan, valueInt:5}`)
		assert.Len(t, ids, 5, "post-rangeable: tenant %s should have 5 items with score>5", tn)
	}
}

func testEnableRangeableMTPostRestart(t *testing.T, restURI string) {
	className := enableRangeableMTBaselines.className
	if className == "" {
		t.Skip("enable rangeable baselines not set")
	}

	helper.SetupClient(restURI)

	cls := helper.GetClass(t, className)
	for _, prop := range cls.Properties {
		if prop.Name == "score" {
			require.NotNil(t, prop.IndexRangeFilters)
			assert.True(t, *prop.IndexRangeFilters, "post-restart: score should be rangeable")
		}
	}

	for _, tn := range enableRangeableMTBaselines.tenantNames {
		ids := rangeQueryTenant(t, className, tn,
			`{path:["score"], operator:GreaterThan, valueInt:5}`)
		assert.Len(t, ids, 5, "post-restart: tenant %s should have 5 items with score>5", tn)
	}
}

// =============================================================================
// Test 5: Validation
// =============================================================================

func testValidation(t *testing.T, restURI string) {
	// Non-MT class with tenants param → 400.
	nonMTClass := "MTValidateNonMT"
	createNonMTClass(t, nonMTClass, []*models.Property{
		{Name: "text", DataType: []string{"text"}},
	})
	for i := 0; i < 3; i++ {
		obj := &models.Object{
			Class:      nonMTClass,
			Properties: map[string]interface{}{"text": fmt.Sprintf("doc_%d", i)},
		}
		require.NoError(t, helper.CreateObject(t, obj))
	}

	t.Run("NonMT_with_tenants", func(t *testing.T) {
		got := reindexhelpers.SubmitIndexUpsertRaw(t, restURI, nonMTClass, "text",
			"searchable", `{"algorithm":"blockmax"}`, reindexhelpers.WithTenants([]string{"t1"}))
		require.Equal(t, http.StatusBadRequest, got.StatusCode,
			"non-MT class with tenants should reject as 400: %s", got.Body)
	})

	// MT class for remaining validations.
	mtClass := "MTValidate"
	createMTClass(t, mtClass, []*models.Property{
		{Name: "text", DataType: []string{"text"}, Tokenization: "word"},
	})
	addTenants(t, mtClass, []string{"active1", "active2"})
	for _, tn := range []string{"active1", "active2"} {
		obj := &models.Object{
			Class:      mtClass,
			Properties: map[string]interface{}{"text": "hello world"},
			Tenant:     tn,
		}
		require.NoError(t, helper.CreateObject(t, obj))
	}

	t.Run("ChangeTokenization_with_tenants", func(t *testing.T) {
		got := reindexhelpers.SubmitIndexUpsertRaw(t, restURI, mtClass, "text",
			"searchable", `{"tokenization":"field"}`, reindexhelpers.WithTenants([]string{"active1"}))
		require.Equal(t, http.StatusBadRequest, got.StatusCode,
			"MT class with tenants on change-tokenization should reject as 400: %s", got.Body)
	})

	t.Run("ChangeAlgorithm_with_tenants", func(t *testing.T) {
		got := reindexhelpers.SubmitIndexUpsertRaw(t, restURI, mtClass, "text",
			"searchable", `{"algorithm":"blockmax"}`, reindexhelpers.WithTenants([]string{"active1"}))
		require.Equal(t, http.StatusBadRequest, got.StatusCode,
			"MT class with tenants on change-algorithm should reject as 400: %s", got.Body)
	})

	t.Run("Nonexistent_tenant", func(t *testing.T) {
		got := reindexhelpers.SubmitIndexUpsertRaw(t, restURI, mtClass, "text",
			"searchable", `{"algorithm":"blockmax"}`, reindexhelpers.WithTenants([]string{"does_not_exist"}))
		require.Equal(t, http.StatusBadRequest, got.StatusCode,
			"non-existent tenant should reject as 400: %s", got.Body)
	})
}

// =============================================================================
// Test 6: Two shard loads between a completed migration and the schema flip
// =============================================================================

// A migration builds its index under a migration-scoped directory and only
// renames it to the canonical name at the next shard load. The schema flag the
// index is served from flips separately and cluster-wide, after the last shard
// is done. A node that dies in between comes back with the two disagreeing,
// and each load then sees a different half of it: the first promotes the
// rebuilt directory to the canonical name, and the second reads a canonical
// directory for a property whose flag still says the index does not exist, and
// deletes it.
//
// That deletion is what this test pins, because it is as far as the defect
// reaches end to end. Its consequence — an index that stays empty and still
// reports itself ready — needs a load with no migration left behind it to
// rebuild what was deleted, and a migration still running is the only thing
// that holds this window open in the first place. So the assertion below is
// that the promoted directory survives the second load, identified by inode so
// that one deleted and rebuilt under the same name reads as what it is. The
// per-tenant counts at the end guard against collateral damage and pass either
// way.
//
// The window to crash into is the preparation phase, which each shard spends
// prepending its rebuilt index into place. That costs the size of the index,
// so the ballast below buys the phase its width — a handful of tenants is
// enough, where widening the swap phase instead would take upwards of a
// thousand.
const (
	crashWindowClass = "MTCrashWindow"
	crashWindowProp  = "score"
	// Half of each tenant's objects sit below this pivot, so a bucket that
	// lost its contents answers zero where a complete one answers half.
	crashWindowPivot   = 50
	crashWindowTenants = 6
	crashWindowObjects = 1000
	// Values per object, all above the pivot and distinct across the corpus:
	// the filter never returns them, so every tenant answers the same count
	// however many there are, but the migration has to index every one.
	crashWindowBallast = 300
)

func testTwoLoadsBeforeTheSchemaFlip(ctx context.Context, t *testing.T) {
	// Forced-lazy loading, so the two shard loads this test turns on are its
	// own to place. Left to auto-detection the node hydrates every shard on the
	// way up and spends the first load before the test can look, and the
	// threshold that switches auto-detection over is 1000 shards — a tenant
	// count that costs minutes per run. The rest of the suite needs its own
	// eager-loaded coverage, so this cannot share the suite's container.
	compose, err := reindexhelpers.SingleNodeCompose().
		WithWeaviateEnv("LAZY_LOAD_SHARD_COUNT_THRESHOLD", "0").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate crash-window test containers: %s", err.Error())
		}
	}()
	restURI := compose.GetWeaviate().URI()
	helper.SetupClient(restURI)

	off := false
	createMTClass(t, crashWindowClass, []*models.Property{
		{Name: "name", DataType: []string{"text"}, Tokenization: "word"},
		{Name: crashWindowProp, DataType: []string{"int[]"}, IndexFilterable: &off, IndexRangeFilters: &off},
	})
	names := make([]string, 0, crashWindowTenants)
	for i := 0; i < crashWindowTenants; i++ {
		names = append(names, fmt.Sprintf("crash%02d", i))
	}
	addTenants(t, crashWindowClass, names)
	for _, name := range names {
		importCorpus(t, crashWindowClass, crashWindowProp, name, crashWindowObjects, crashWindowBallast)
	}

	taskID := reindexhelpers.SubmitIndexUpsert(t, restURI, crashWindowClass, crashWindowProp, "filterable", `{}`)
	t.Logf("enable-filterable to crash mid-preparation: task %s", taskID)

	// Crash once a shard has its rebuilt index prepended into place, so the
	// node comes back holding one the next load will promote and a schema that
	// still denies it. SIGKILL, not a graceful stop: a shutdown that drains
	// would let the task finish and close the window.
	awaitPreparedShard(ctx, t, compose.GetWeaviate().Container())
	container := crashAndRestart(ctx, t, compose)

	victim := preparedTenant(ctx, t, container)
	require.NotEmpty(t, bm25QueryTenant(t, crashWindowClass, "name", "corpus", victim),
		"tenant %q must serve its objects after the crash", victim)
	promoted := canonicalBucketID(ctx, t, container, victim)
	require.NotEmpty(t, promoted,
		"tenant %q had its rebuilt index prepended into place before the crash, so its first load "+
			"after it must promote that directory to %q; with nothing promoted the second load has "+
			"nothing to destroy and this run proves nothing", victim, canonicalBucket)
	require.False(t, crashWindowIndexEnabled(t),
		"the schema flipped before the first load landed, so the crash came too late; raise "+
			"crashWindowBallast to widen the preparation phase")

	// The second load is the destructive one: a canonical directory now exists
	// for a property whose flag still denies the index.
	container = crashAndRestart(ctx, t, compose)
	restURI = compose.GetWeaviate().URI()
	require.NotEmpty(t, bm25QueryTenant(t, crashWindowClass, "name", "corpus", victim),
		"tenant %q must serve its objects after the second crash", victim)
	require.False(t, crashWindowIndexEnabled(t),
		"the schema flipped before the second load landed; both loads have to land while the "+
			"property still reads disabled for this run to prove anything")
	require.Equal(t, promoted, canonicalBucketID(ctx, t, container, victim),
		"the second load deleted tenant %q's rebuilt index. The schema still says this property has "+
			"no filterable index, and the load read that as license to remove the directory the "+
			"first load had just promoted", victim)

	reindexhelpers.AwaitReindexFinished(t, restURI, taskID, reindexhelpers.WithTimeout(300*time.Second))
	require.Eventually(t, func() bool { return crashWindowIndexEnabled(t) },
		120*time.Second, 200*time.Millisecond,
		"the filterable index must read enabled in the schema once the task finishes")

	for _, name := range names {
		hits := rangeHits(t, crashWindowClass, crashWindowProp, name, crashWindowPivot, crashWindowObjects*2)
		assert.Len(t, hits, crashWindowObjects/2,
			"tenant %q came out of two crashes answering the filter with %d of the %d objects the "+
				"migration indexed for it", name, len(hits), crashWindowObjects/2)
	}
}

// canonicalBucket is the directory a promoted filterable index ends up in, and
// the one the startup sweep deletes for a property the schema says has no such
// index.
var canonicalBucket = "property_" + crashWindowProp

// preparedSentinel marks a shard that has finished prepending its rebuilt
// index. It is the earliest point a crash leaves something behind that the
// next load promotes.
const preparedSentinel = "/.migrations/*/merged.mig"

// awaitPreparedShard blocks until some shard reaches that point, which is when
// the window opens. Waited out inside the container, because the phase lasts
// under a second and a look from here costs a docker exec. The task reaching
// SWAPPING is not the same signal — the status changes before any shard has
// got that far, and a crash there leaves nothing behind.
func awaitPreparedShard(ctx context.Context, t *testing.T, c testcontainers.Container) {
	t.Helper()
	execInContainer(ctx, t, c, fmt.Sprintf(
		"i=0; until ls /data/%s/*/lsm%s >/dev/null 2>&1; do i=$((i+1)); "+
			"[ $i -gt 15000 ] && exit 1; sleep 0.02; done",
		strings.ToLower(crashWindowClass), preparedSentinel))
}

// crashAndRestart SIGKILLs the node and waits for it to serve again, returning
// the container it comes back as.
func crashAndRestart(ctx context.Context, t *testing.T, compose *docker.DockerCompose) testcontainers.Container {
	t.Helper()
	zero := time.Duration(0)
	require.NoError(t, compose.RestartAt(ctx, 0, &zero), "crash and restart the node")
	helper.SetupClient(compose.GetWeaviate().URI())
	return compose.GetWeaviate().Container()
}

// preparedTenant names a tenant that got that far before the crash. Read from
// disk, because the task's status is collection-wide and says nothing about
// which tenants did.
func preparedTenant(ctx context.Context, t *testing.T, c testcontainers.Container) string {
	t.Helper()
	tenant := firstTenantWith(ctx, t, c, preparedSentinel)
	require.NotEmpty(t, tenant,
		"no shard had its rebuilt index prepended into place when the node died, so the crash "+
			"landed ahead of the preparation phase; raise crashWindowBallast to widen it")
	t.Logf("tenant %q had its rebuilt index prepared before the crash", tenant)
	return tenant
}

// canonicalBucketID identifies a tenant's canonical index directory, or "" if
// it has none. An inode rather than a bare existence check, so a directory
// deleted and rebuilt under the same name reads as what it is: a different
// directory.
func canonicalBucketID(ctx context.Context, t *testing.T, c testcontainers.Container, tenant string) string {
	t.Helper()
	return delimited(execInContainer(ctx, t, c, fmt.Sprintf(
		"stat -c '<%%i>' /data/%s/%s/lsm/%s 2>/dev/null; true",
		strings.ToLower(crashWindowClass), tenant, canonicalBucket)))
}

// firstTenantWith names the first tenant of the collection carrying a path
// that matches suffix, or "" if none does.
func firstTenantWith(ctx context.Context, t *testing.T, c testcontainers.Container, suffix string) string {
	t.Helper()
	return delimited(execInContainer(ctx, t, c, fmt.Sprintf(
		"for f in /data/%s/*/lsm%s; do [ -e \"$f\" ] && printf '<%%s>' \"$(echo $f | cut -d/ -f4)\" && break; done; true",
		strings.ToLower(crashWindowClass), suffix)))
}

// delimited returns what an in-container probe wrapped in angle brackets, so
// docker's exec stream framing cannot be read as part of the value.
func delimited(out string) string {
	i, j := strings.Index(out, "<"), strings.Index(out, ">")
	if i < 0 || j <= i+1 {
		return ""
	}
	return out[i+1 : j]
}

func crashWindowIndexEnabled(t *testing.T) bool {
	t.Helper()
	for _, prop := range helper.GetClass(t, crashWindowClass).Properties {
		if prop.Name == crashWindowProp {
			return prop.IndexFilterable != nil && *prop.IndexFilterable
		}
	}
	return false
}

// =============================================================================
// Helpers
// =============================================================================

func createMTClass(t *testing.T, className string, properties []*models.Property) {
	t.Helper()
	class := &models.Class{
		Class:      className,
		Properties: properties,
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: true,
		},
		Vectorizer: "none",
	}
	helper.CreateClass(t, class)
}

func createNonMTClass(t *testing.T, className string, properties []*models.Property) {
	t.Helper()
	class := &models.Class{
		Class:      className,
		Properties: properties,
		Vectorizer: "none",
	}
	helper.CreateClass(t, class)
}

func addTenants(t *testing.T, className string, tenantNames []string) {
	t.Helper()
	tenants := make([]*models.Tenant, len(tenantNames))
	for i, tn := range tenantNames {
		tenants[i] = &models.Tenant{
			Name:           tn,
			ActivityStatus: models.TenantActivityStatusHOT,
		}
	}
	helper.CreateTenants(t, className, tenants)
}

func bm25QueryTenant(t *testing.T, className, property, query, tenant string) []string {
	t.Helper()
	gqlQuery := fmt.Sprintf(`{
		Get {
			%s(bm25: {query: %q, properties: [%q]}, tenant: %q) {
				_additional { id }
			}
		}
	}`, className, query, property, tenant)
	return runGraphQLQuery(t, className, gqlQuery)
}

func rangeQueryTenant(t *testing.T, className, tenant, where string) []string {
	t.Helper()
	gqlQuery := fmt.Sprintf(`{
		Get {
			%s(where: %s, tenant: %q) {
				_additional { id }
			}
		}
	}`, className, where, tenant)
	return runGraphQLQuery(t, className, gqlQuery)
}

func runGraphQLQuery(t *testing.T, className, gqlQuery string) []string {
	t.Helper()
	resp, err := graphqlhelper.QueryGraphQL(t, nil, "", gqlQuery, nil)
	require.NoError(t, err)
	if len(resp.Errors) > 0 {
		t.Fatalf("graphql errors: %v", resp.Errors[0].Message)
	}
	data := make(map[string]interface{})
	for key, value := range resp.Data {
		data[key] = value
	}
	getMap := data["Get"].(map[string]interface{})
	items := getMap[className].([]interface{})
	ids := make([]string, 0, len(items))
	for _, item := range items {
		m := item.(map[string]interface{})
		additional := m["_additional"].(map[string]interface{})
		ids = append(ids, additional["id"].(string))
	}
	sort.Strings(ids)
	return ids
}

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

package reindex_singlenode

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
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
)

// testTornResumeReindexedNotTidied pins the journey:
//
//	"a prior reindex left the on-disk migration in IsStarted+IsReindexed
//	 state but never reached IsTidied — what does a fresh re-submit do?"
//
// Realistic root causes that produce this on-disk shape:
//
//   - a runtime error inside runtimeSwap (e.g. PrependSegmentsFromBucket
//     hit ENOSPC, or any I/O failure between markReindexed and markTidied),
//     so markReindexed() ran but none of the markPrepended /
//     markMerged / markSwapped / markTidied ran;
//
//   - a container kill that landed between the markReindexed() write and
//     the first runtimeSwap step (no .mig files past reindexed.mig on disk);
//
//   - a control-plane bug or RAFT replay that crashed the process mid-swap
//     and left disk in a torn shape.
//
// The bug this test guards against: OnAfterLsmInitAsync at the
// `if rt.IsReindexed() { ... "nothing to do" ... }` branch short-circuits
// without ever invoking runtimeSwap. For non-semantic migration types
// (enable-rangeable, repair-*), the full lifecycle is supposed to complete
// inside RunOnShard — there is NO OnGroupCompleted swap fallback. So the
// re-submitted task reports FINISHED while:
//
//   - the schema flag is never flipped (OnMigrationComplete never fires);
//   - the in-place "new" bucket layer (whatever was prepared in the reindex
//     bucket, or the in-progress ingest bucket) is never promoted to the
//     main bucket slot;
//   - any subsequent filter/range/bm25 query that relies on the new bucket
//     returns no hits or stale hits.
//
// For semantic migration types (enable-filterable, enable-searchable,
// change-tokenization), OnGroupCompleted's RunSwapOnShard does run on the
// FINISHED unit — but if the on-disk reindex bucket is empty (because the
// failure that produced the torn state was BEFORE any data was actually
// written), the swap promotes an empty bucket and flips the schema flag,
// also a silent data loss.
//
// To reproduce reliably without relying on iteration timing, we directly
// craft the "reindexed but not tidied" sentinel state on disk before any
// reindex submission. The shape we craft:
//
//	.migrations/<migDir>/started.mig    (RFC3339Nano timestamp)
//	.migrations/<migDir>/reindexed.mig  (RFC3339Nano timestamp)
//	.migrations/<migDir>/properties.mig (comma-joined property names)
//
// — that's the disk state a real failed run would leave behind in the
// narrow window between markReindexed() and markPrepended(). No
// __reindex / __ingest sidecar dirs are created; if the code (correctly)
// rebuilds them via CreateOrLoadBucket and re-runs iteration on
// resubmit, the migration finishes correctly. If it (incorrectly)
// short-circuits on IsReindexed and skips iteration / swap, the test
// catches the silent failure.
//
// Test variants (one per non-semantic strategy + a semantic-strategy
// canary):
//
//   - enable-rangeable (non-semantic): missing-swap is the dominant
//     failure mode here because OnGroupCompleted has no swap path.
//
//   - repair-filterable (non-semantic, RoaringSetRefresh): same shape.
//
//   - enable-filterable (semantic): OnGroupCompleted DOES run swap, but
//     against an empty reindex bucket → schema flag flips to true on an
//     empty bucket. Silent data loss in a different shape.
//
// restURI is re-derived inside each subtest from plantTornSentinelsAcrossRestart
// (the host port changes across the restart), so no URI is threaded in here.
func testTornResumeReindexedNotTidied(t *testing.T, compose *docker.DockerCompose) {
	t.Run("enable_rangeable_nonSemantic", func(t *testing.T) {
		testTornResumeEnableRangeable(t, compose)
	})
	t.Run("repair_filterable_nonSemantic", func(t *testing.T) {
		testTornResumeRepairFilterable(t, compose)
	})
	t.Run("enable_filterable_semantic", func(t *testing.T) {
		testTornResumeEnableFilterable(t, compose)
	})
}

// tornResumeObjectCount is deliberately ≤ 50 so we can stay inside the
// default `limit: 50` on the shared equalFilterHits / rangeFilterHits
// helpers in delete_then_reenable_test.go without forking helpers. 30
// is enough corpus to make a wrong hit-count obvious (50/50 split for
// rangeable, 30-of-30 hits for filterable) but small enough that the
// reindex from scratch finishes within the per-subtest timeout even on
// a slow CI runner.
const tornResumeObjectCount = 30

func testTornResumeEnableRangeable(t *testing.T, compose *docker.DockerCompose) {
	const class = "TornResumeRangeable"
	trueVal, falseVal := true, false
	helper.CreateClass(t, &models.Class{
		Class: class,
		Properties: []*models.Property{
			{Name: "score", DataType: []string{"int"}, IndexFilterable: &trueVal, IndexRangeFilters: &falseVal},
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, class)

	for i := 0; i < tornResumeObjectCount; i++ {
		score := 10
		if i%2 == 0 {
			score = 100
		}
		require.NoError(t, helper.CreateObject(t, &models.Object{
			Class: class, Properties: map[string]interface{}{"score": score},
		}))
	}

	// migrationDirName for enable-rangeable on a single property:
	// MigrationDirPrefixFilterableToRangeable + "_" + propName.
	migDir := "filterable_to_rangeable_score"
	restURI := plantTornSentinelsAcrossRestart(t, compose, class, migDir, []string{"score"})

	taskID := reindexhelpers.SubmitIndexUpdate(t, restURI, class, "score",
		`{"rangeable":{"enabled":true}}`)
	t.Logf("torn-resume rangeable: submitted task %s with planted torn sentinels", taskID)
	reindexhelpers.AwaitReindexFinished(t, restURI, taskID)

	// Functional check: half the objects have score<50, half score>50.
	expected := tornResumeObjectCount / 2
	hits := rangeFilterHits(t, class, "score", 50)

	// Schema-flag check: this is the customer-visible "ready" signal.
	cls := helper.GetClass(t, class)
	var rangeFiltersEnabled *bool
	for _, p := range cls.Properties {
		if p.Name == "score" {
			rangeFiltersEnabled = p.IndexRangeFilters
		}
	}

	require.NotNil(t, rangeFiltersEnabled,
		"post-torn-resume: IndexRangeFilters must be non-nil")
	assert.True(t, *rangeFiltersEnabled,
		"post-torn-resume: IndexRangeFilters must be true (schema flag flipped)")
	require.Equal(t, expected, hits,
		"post-torn-resume: LessThan(50) must return %d; got %d. "+
			"If the schema flag flipped (above) but this hit count is wrong, the "+
			"runtime swap completed on an EMPTY reindex bucket (the torn-state "+
			"resume skipped iteration). If both are wrong, the swap never fired. "+
			"Either is a Sev 1 silent failure.",
		expected, hits)
}

func testTornResumeRepairFilterable(t *testing.T, compose *docker.DockerCompose) {
	const class = "TornResumeRepairFilterable"
	trueVal := true
	helper.CreateClass(t, &models.Class{
		Class: class,
		Properties: []*models.Property{
			{Name: "name", DataType: []string{"text"}, IndexFilterable: &trueVal, IndexSearchable: &trueVal, Tokenization: "word"},
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, class)

	for i := 0; i < tornResumeObjectCount; i++ {
		require.NoError(t, helper.CreateObject(t, &models.Object{
			Class:      class,
			Properties: map[string]interface{}{"name": "shared_repair_name"},
		}))
	}

	// migrationDirName for repair-filterable: the fixed
	// MigrationDirFilterableRoaringsetRefresh constant.
	migDir := "filterable_roaringset_refresh"
	restURI := plantTornSentinelsAcrossRestart(t, compose, class, migDir, []string{"name"})

	taskID := reindexhelpers.SubmitIndexUpdate(t, restURI, class, "name",
		`{"filterable":{"rebuild":true}}`)
	t.Logf("torn-resume repair-filterable: submitted task %s with planted torn sentinels", taskID)
	reindexhelpers.AwaitReindexFinished(t, restURI, taskID)

	hits := equalFilterHits(t, class, "name", "shared_repair_name")
	require.Equal(t, tornResumeObjectCount, hits,
		"post-torn-resume: filterable Equal('shared_repair_name') must return %d; "+
			"got %d. If 0, the runtime swap silently no-opped or promoted an empty "+
			"reindex bucket — schema reports ready but customer queries are broken "+
			"(Sev 1)",
		tornResumeObjectCount, hits)
}

func testTornResumeEnableFilterable(t *testing.T, compose *docker.DockerCompose) {
	const class = "TornResumeEnableFilterable"
	trueVal, falseVal := true, false
	helper.CreateClass(t, &models.Class{
		Class: class,
		Properties: []*models.Property{
			{Name: "name", DataType: []string{"text"}, IndexFilterable: &falseVal, IndexSearchable: &trueVal, Tokenization: "word"},
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, class)

	for i := 0; i < tornResumeObjectCount; i++ {
		require.NoError(t, helper.CreateObject(t, &models.Object{
			Class:      class,
			Properties: map[string]interface{}{"name": "shared_enable_name"},
		}))
	}

	// migrationDirName for enable-filterable: MigrationDirPrefixEnableFilterable
	// + "_" + sorted propNames.
	migDir := "enable_filterable_name"
	restURI := plantTornSentinelsAcrossRestart(t, compose, class, migDir, []string{"name"})

	taskID := reindexhelpers.SubmitIndexUpdate(t, restURI, class, "name",
		`{"filterable":{"enabled":true}}`)
	t.Logf("torn-resume enable-filterable: submitted task %s with planted torn sentinels", taskID)
	reindexhelpers.AwaitReindexFinished(t, restURI, taskID)

	// Verify schema flag flipped.
	requireFilterableEnabled(t, class, "name")

	// Functional check: every object has name="shared_enable_name".
	hits := equalFilterHits(t, class, "name", "shared_enable_name")
	require.Equal(t, tornResumeObjectCount, hits,
		"post-torn-resume (semantic): filterable Equal('shared_enable_name') "+
			"must return %d; got %d. If the schema flag flipped (above) but the "+
			"hits are 0, the swap promoted an empty reindex bucket and silently "+
			"flipped the customer's `ready` state on top of no data (Sev 1).",
		tornResumeObjectCount, hits)
}

// plantTornSentinelsAcrossRestart plants the on-disk sentinel files for a
// run that reached markReindexed() and then crashed before any swap-phase
// sentinel was written, then restarts the container. Layout:
//
//	.migrations/<migDir>/started.mig    — RFC3339Nano timestamp
//	.migrations/<migDir>/reindexed.mig  — RFC3339Nano timestamp
//	.migrations/<migDir>/properties.mig — comma-joined prop names
//
// Stop → plant → start avoids racing the server's async
// cleanStaleMigrationDirs (shard_init_properties.go:134-152, 336); see
// weaviate/0-weaviate-issues#254. Returns the new REST URI; host port
// mapping changes across restart.
func plantTornSentinelsAcrossRestart(
	t *testing.T,
	compose *docker.DockerCompose,
	class, migDir string,
	props []string,
) string {
	t.Helper()
	ctx := context.Background()

	container := compose.GetWeaviate().Container()

	// Locate the shard while the server is up — path is stable across restart.
	shardPath := findShardPathInContainer(t, container, class)
	containerMigDir := fmt.Sprintf("%s/lsm/.migrations/%s", shardPath, migDir)
	lsmPath := fmt.Sprintf("%s/lsm", shardPath)

	require.NoError(t, compose.StopAt(ctx, 0, nil),
		"plantTornSentinelsAcrossRestart: graceful stop before planting must succeed")

	// CopyDirToContainer works against stopped containers via Docker's archive
	// API; docker exec does not.
	stagingRoot := t.TempDir()
	stagedDotMigrations := filepath.Join(stagingRoot, ".migrations")
	stagedMigDir := filepath.Join(stagedDotMigrations, migDir)
	require.NoError(t, os.MkdirAll(stagedMigDir, 0o755))

	nowRFC := time.Now().UTC().Format(time.RFC3339Nano)
	propsCSV := strings.Join(props, ",")
	for _, pair := range [][2]string{
		{"started.mig", nowRFC},
		{"reindexed.mig", nowRFC},
		{"properties.mig", propsCSV},
	} {
		fname, content := pair[0], pair[1]
		require.NoError(t,
			os.WriteFile(filepath.Join(stagedMigDir, fname), []byte(content), 0o666),
			"plantTornSentinelsAcrossRestart: staging %s on host must succeed", fname)
	}

	// containerParentPath = <lsm>/.migrations → extracts at <lsm>/ (testcontainers
	// extracts into filepath.Dir of the target). Mode 0o755 applies to every
	// tar entry; directories need the execute bit so the server can stat
	// payload files on next start.
	require.NoError(t,
		container.CopyDirToContainer(ctx, stagedDotMigrations,
			fmt.Sprintf("%s/.migrations", lsmPath), 0o755),
		"plantTornSentinelsAcrossRestart: CopyDirToContainer must succeed against the stopped container")

	require.NoError(t, compose.StartAt(ctx, 0),
		"plantTornSentinelsAcrossRestart: restart after planting must succeed")
	newRestURI := compose.GetWeaviate().URI()
	helper.SetupClient(newRestURI)

	// Diagnostic only: may already be cleaned by shard init.
	if _, lsReader, lsErr := container.Exec(ctx, []string{"ls", "-la", containerMigDir}); lsErr == nil && lsReader != nil {
		out, _ := io.ReadAll(lsReader)
		t.Logf("plantTornSentinelsAcrossRestart: %s post-restart contents:\n%s", containerMigDir, string(out))
	}

	return newRestURI
}

// findShardPathInContainer locates the on-disk path for the first shard
// of the named class inside the running container. Returns
// /data/<class-lowercase>/<shard-uuid>. Used for direct sentinel
// manipulation in tests.
func findShardPathInContainer(t *testing.T, container testcontainers.Container, class string) string {
	t.Helper()
	ctx := context.Background()

	classDir := fmt.Sprintf("/data/%s", strings.ToLower(class))
	code, reader, err := container.Exec(ctx, []string{"ls", "-1", classDir})
	require.NoError(t, err)
	require.Zero(t, code, "ls %s must succeed", classDir)

	out, _ := io.ReadAll(reader)
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		// Skip control-character prefixed lines from exec output.
		if strings.ContainsAny(line[:1], "\x00\x01\x02\x03") {
			line = strings.TrimLeftFunc(line, func(r rune) bool {
				return r < 0x20
			})
		}
		if line == "" {
			continue
		}
		// Anything non-shard would be a file like backups.db; shards are dirs
		// with UUID names. Use the first directory entry.
		probe := classDir + "/" + line + "/lsm"
		code, _, err := container.Exec(ctx, []string{"test", "-d", probe})
		if err == nil && code == 0 {
			return classDir + "/" + line
		}
	}
	t.Fatalf("could not locate shard dir for class %s under %s", class, classDir)
	return ""
}

// TestTornResume_StandaloneSmoke is a self-contained, single-scenario
// entry point so the test can be run in isolation with quick feedback:
//
//	go test -count 1 -v -timeout 10m \
//	  -run TestTornResume_StandaloneSmoke \
//	  ./test/acceptance/reindex_singlenode/
//
// The suite-driven version is wired into TestSingleNode_ReindexSuite so
// it runs alongside every other sub-test on the shared container in CI.
func TestTornResume_StandaloneSmoke(t *testing.T) {
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

	helper.SetupClient(compose.GetWeaviate().URI())
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
			t.Logf("=== Container logs (last 400 lines) ===\n%s",
				strings.Join(lines, "\n"))
		}
	}()

	testTornResumeReindexedNotTidied(t, compose)
}

// TestSuppress ensures this file compiles in isolation. The suite-driven
// path runs via TestSingleNode_ReindexSuite/TornResumeReindexedNotTidied.
func TestSuppress_TornResume(t *testing.T) {
	assert.NotNil(t, testTornResumeReindexedNotTidied)
}

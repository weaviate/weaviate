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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/acceptance/helpers/reindexrecords"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// testTornResumeReindexedNotTidied pins the journey: a prior reindex crashed
// mid-rebuild (I/O failure, container kill, a process death mid-swap), so the
// shard carries a record naming staged directories that never reached disk,
// for a task the cluster has never heard of. A fresh submit for the same
// property must reclaim that state and rebuild from scratch.
//
// The failure it catches is a resume that trusts the leftover record and
// skips either the iteration or the flip. The task still reports FINISHED,
// so both halves are asserted per variant — the schema flag AND the hit
// count — because either alone passes on the wrong state.
//
// Planting the record directly, rather than racing a real run, is what makes
// the starting state exact and the test independent of iteration timing.
//
// One variant per shape:
//
//   - enable-rangeable (non-semantic): the whole lifecycle completes inside
//     RunOnShard with no OnGroupCompleted swap fallback, so a skipped flip
//     has nothing behind it — the schema flag never flips and queries miss.
//
//   - repair-filterable (non-semantic, RoaringSetRefresh): same shape.
//
//   - enable-filterable (semantic): OnGroupCompleted does run the swap, so
//     the failure lands the other way round — an empty staged bucket goes
//     live with the schema flag flipped over it.
//
// restURI is re-derived inside each subtest from plantTornMigrationAcrossRestart
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

// tornResumeGeneration is the generation the crashed run was at. Every
// directory the planted record names carries it, because a tracker and the
// sidecars beside it always come from the same run.
const tornResumeGeneration = 1

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

	restURI := plantTornMigrationAcrossRestart(t, compose, class,
		db.StrategyCodeFilterableToRangeable, "enable-rangeable", []string{"score"})

	taskID := reindexhelpers.SubmitIndexUpsert(t, restURI, class, "score", "rangeFilters",
		`{}`)
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

	restURI := plantTornMigrationAcrossRestart(t, compose, class,
		db.StrategyCodeFilterableRoaringsetRefresh, "repair-filterable", []string{"name"})

	taskID := reindexhelpers.RebuildIndex(t, restURI, class, "name", "filterable")
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

	restURI := plantTornMigrationAcrossRestart(t, compose, class,
		db.StrategyCodeEnableFilterable, "enable-filterable", []string{"name"})

	taskID := reindexhelpers.SubmitIndexUpsert(t, restURI, class, "name", "filterable",
		`{}`)
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

// plantTornMigrationAcrossRestart plants the on-disk state of a run that
// crashed mid-rebuild, then restarts the container. Layout:
//
//	.migrations/<tracker>/payload.mig                        — the task payload
//	.migrations/records/<version>_<strategyCode>_<unit>.json — the state
//
// Every directory the record names comes from [reindexrecords], so the planted
// state is one a crashed run on this build could actually have left: a record
// naming a staged or sidecar directory the writer would not have written is
// refused outright, and pinning behavior against a refused record pins nothing.
//
// The record is Iterating: the rebuild never reported complete, so nothing
// staged is a candidate for becoming live and a submit that lands afterwards
// has to start from scratch rather than short-circuit on it.
//
// Stop → plant → start avoids racing the server's async
// cleanStaleMigrationDirs (shard_init_properties.go:134-152, 336); see
// weaviate/0-weaviate-issues#254. Returns the new REST URI; host port
// mapping changes across restart.
func plantTornMigrationAcrossRestart(
	t *testing.T,
	compose *docker.DockerCompose,
	class string,
	strategyCode db.MigrationStrategyCode,
	migrationType string,
	props []string,
) string {
	t.Helper()
	ctx := context.Background()

	migDir := reindexrecords.TrackerDir(t, strategyCode, props, tornResumeGeneration)

	container := compose.GetWeaviate().Container()

	// Locate the shard while the server is up — path is stable across restart.
	shardPath := findShardPathInContainer(t, container, class)
	containerMigDir := fmt.Sprintf("%s/lsm/.migrations/%s", shardPath, migDir)
	lsmPath := fmt.Sprintf("%s/lsm", shardPath)

	require.NoError(t, compose.StopAt(ctx, 0, nil),
		"plantTornMigrationAcrossRestart: graceful stop before planting must succeed")

	// CopyDirToContainer works against stopped containers via Docker's archive
	// API; docker exec does not.
	stagingRoot := t.TempDir()
	stagedDotMigrations := filepath.Join(stagingRoot, ".migrations")
	stagedMigDir := filepath.Join(stagedDotMigrations, migDir)
	require.NoError(t, os.MkdirAll(stagedMigDir, 0o755))

	subject := db.MigrationSubject{
		Key: db.MigrationRecordKey{
			TaskVersion:  1,
			StrategyCode: strategyCode,
			UnitID:       "u0",
		},
		TaskID:          "torn-resume-crashed-run",
		MigrationType:   db.ReindexMigrationType(migrationType),
		Properties:      props,
		IterationCutoff: time.Now().UTC(),
		TrackerDir:      migDir,
		StagedDirs:      make(map[string]string, len(props)),
		CanonicalDirs:   make(map[string]string, len(props)),
		SidecarDirs:     make(map[string]string, len(props)),
	}
	quoted := make([]string, len(props))
	for i, prop := range props {
		quoted[i] = strconv.Quote(prop)
		handles := reindexrecords.HandlesFor(t, strategyCode, prop, tornResumeGeneration)
		subject.StagedDirs[prop] = handles.Staged
		subject.CanonicalDirs[prop] = handles.Canonical
		subject.SidecarDirs[prop] = handles.Sidecar
	}
	recordName, record := reindexrecords.Encode(t,
		db.NewMigrationRecordIterating(subject, db.MigrationCheckpoint{}))

	payload := fmt.Sprintf(
		`{"taskID":"torn-resume-crashed-run","taskVersion":1,"unitID":"u0",`+
			`"payload":{"collection":%q,"migrationType":%q,"properties":[%s]}}`,
		class, migrationType, strings.Join(quoted, ","))

	stagedRecordsDir := filepath.Join(stagedDotMigrations, "records")
	require.NoError(t, os.MkdirAll(stagedRecordsDir, 0o755))
	for path, content := range map[string]string{
		filepath.Join(stagedMigDir, "payload.mig"):  payload,
		filepath.Join(stagedRecordsDir, recordName): record,
	} {
		require.NoError(t, os.WriteFile(path, []byte(content), 0o666),
			"plantTornMigrationAcrossRestart: staging %s on host must succeed", path)
	}

	// containerParentPath = <lsm>/.migrations → extracts at <lsm>/ (testcontainers
	// extracts into filepath.Dir of the target). Mode 0o755 applies to every
	// tar entry; directories need the execute bit so the server can stat
	// payload files on next start.
	require.NoError(t,
		container.CopyDirToContainer(ctx, stagedDotMigrations,
			fmt.Sprintf("%s/.migrations", lsmPath), 0o755),
		"plantTornMigrationAcrossRestart: CopyDirToContainer must succeed against the stopped container")

	require.NoError(t, compose.StartAt(ctx, 0),
		"plantTornMigrationAcrossRestart: restart after planting must succeed")
	newRestURI := compose.GetWeaviate().URI()
	helper.SetupClient(newRestURI)

	// Diagnostic only: may already be cleaned by shard init.
	if _, lsReader, lsErr := container.Exec(ctx, []string{"ls", "-la", containerMigDir}, tcexec.Multiplexed()); lsErr == nil && lsReader != nil {
		out, _ := io.ReadAll(lsReader)
		t.Logf("plantTornMigrationAcrossRestart: %s post-restart contents:\n%s", containerMigDir, string(out))
	}

	return newRestURI
}

// findShardPathInContainer locates the on-disk path for the first shard
// of the named class inside the running container. Returns
// /data/<class-lowercase>/<shard-uuid>. Used for planting on-disk migration
// state in tests.
func findShardPathInContainer(t *testing.T, container testcontainers.Container, class string) string {
	t.Helper()
	ctx := context.Background()

	classDir := fmt.Sprintf("/data/%s", strings.ToLower(class))
	code, reader, err := container.Exec(ctx, []string{"ls", "-1", classDir}, tcexec.Multiplexed())
	require.NoError(t, err)
	require.Zero(t, code, "ls %s must succeed", classDir)

	out, _ := io.ReadAll(reader)
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
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

	compose, err := reindexhelpers.StartSingleNode(ctx)
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

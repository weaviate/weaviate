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

package reindex_mt

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
	"github.com/weaviate/weaviate/test/helper"
)

// Acceptance reproducer for weaviate/0-weaviate-issues#464: cancelling an
// enable-rangeable migration then restarting used to leave the schema
// claiming an index that most tenant shards held only as an empty bucket,
// because the cluster-wide flag flipped on the first shard's swap while the
// startup readiness gate rebuilt only from tracker dirs, which cancel
// deletes. enable-rangeable is now semantic (flag flips once, at task
// completion), so a cancelled task never reaches the flip. Assertions cover
// both query results and per-tenant-shard disk state, since fixing only the
// visible symptom would leave the cancel's residue behind.
func TestMultiTenant_EnableRangeable_CancelRestartJourney(t *testing.T) {
	ctx := context.Background()

	compose, err := reindexhelpers.SingleNodeCompose().
		WithWeaviateEnv("REINDEX_CONCURRENCY", "2").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("terminate test containers: %s", err)
		}
	}()

	restURI := compose.GetWeaviate().URI()
	helper.SetupClient(restURI)

	defer func() {
		if t.Failed() {
			dumpWeaviateLogs(ctx, t, compose.GetWeaviate().Container())
		}
	}()

	restart := func(t *testing.T, graceful bool) string {
		t.Helper()
		var timeout *time.Duration
		if !graceful {
			zero := time.Duration(0)
			timeout = &zero
		}
		require.NoError(t, compose.StopAt(ctx, 0, timeout))
		require.NoError(t, compose.StartAt(ctx, 0))
		uri := compose.GetWeaviate().URI()
		helper.SetupClient(uri)
		return uri
	}

	// One dose is not evidence: 464's signature was that the number of
	// wrong tenants tracked how far the migration got. The answer must be
	// "0 wrong" at every dose.
	for _, dose := range []float32{0.4, 0.7} {
		t.Run(fmt.Sprintf("CancelAt%d", int(dose*100)), func(t *testing.T) {
			className := fmt.Sprintf("MTRangeCancel%d", int(dose*100))
			f := seedRangeableFixture(t, className)

			taskID := reindexhelpers.SubmitIndexUpdate(t, restURI, className, "score",
				`{"rangeable":{"enabled":true}}`)
			awaitReindexProgress(t, restURI, className, "score", taskID, dose)
			cancelReindex(t, restURI, className, "score")
			awaitTaskStatus(t, restURI, taskID, "CANCELLED")

			// Pre-restart: the flag must already be false. Under the old
			// behavior it was true here too — the damage only became
			// visible after the restart below.
			requireRangeFiltersFlag(t, restURI, className, "score", false)
			f.requireAllTenantsCorrect(t, "after cancel, before restart")

			restURI = restart(t, true)

			requireRangeFiltersFlag(t, restURI, className, "score", false)
			f.requireAllTenantsCorrect(t, "after cancel + graceful restart")

			// The disk half. Cancel's cleanup plus the dead-task-scoped
			// startup promotion must leave no rangeable working state
			// behind, and must not have touched the filterable bucket the
			// queries above fall back to.
			for _, tenant := range f.tenants {
				dirs := listShardLSMDirs(ctx, t, compose.GetWeaviate().Container(), className, tenant)
				assertNoRangeableResidue(t, dirs, tenant)
				assert.Contains(t, dirs, "property_score",
					"tenant %s: the filterable bucket the fallback reads must survive the cancel", tenant)
			}

			// The bogus 400 on re-enable (the old check read the same flag
			// the migration had wrongly flipped) must be gone, and the
			// re-run must produce a complete index.
			retryID := reindexhelpers.SubmitIndexUpdate(t, restURI, className, "score",
				`{"rangeable":{"enabled":true}}`)
			reindexhelpers.AwaitReindexFinished(t, restURI, retryID,
				reindexhelpers.WithTimeout(5*time.Minute))
			requireRangeFiltersFlag(t, restURI, className, "score", true)
			f.requireAllTenantsCorrect(t, "after re-enable completed")

			for _, tenant := range f.tenants {
				dirs := listShardLSMDirs(ctx, t, compose.GetWeaviate().Container(), className, tenant)
				assert.NotEmpty(t, rangeableBucketDir(dirs),
					"tenant %s: a completed enable-rangeable must leave a rangeable bucket, got %v",
					tenant, dirs)
			}

			helper.DeleteClass(t, className)
		})
	}

	// Control (a): a migration that is allowed to finish must survive a
	// restart with its index intact. This is the arm that would catch a
	// "fix" that simply stopped building the index.
	t.Run("Control_FinishThenRestart", func(t *testing.T) {
		const className = "MTRangeControlFinish"
		f := seedRangeableFixture(t, className)
		defer helper.DeleteClass(t, className)

		taskID := reindexhelpers.SubmitIndexUpdate(t, restURI, className, "score",
			`{"rangeable":{"enabled":true}}`)
		reindexhelpers.AwaitReindexFinished(t, restURI, taskID,
			reindexhelpers.WithTimeout(5*time.Minute))
		requireRangeFiltersFlag(t, restURI, className, "score", true)

		before := rangeableBytesPerTenant(ctx, t, compose.GetWeaviate().Container(), className, f.tenants)
		for tenant, size := range before {
			require.Greater(t, size, int64(0),
				"tenant %s: a finished migration must leave a non-empty rangeable bucket", tenant)
		}
		require.Len(t, before, len(f.tenants), "every tenant shard must have a rangeable bucket")

		restURI = restart(t, true)

		requireRangeFiltersFlag(t, restURI, className, "score", true)
		f.requireAllTenantsCorrect(t, "after finish + restart")
		after := rangeableBytesPerTenant(ctx, t, compose.GetWeaviate().Container(), className, f.tenants)
		for tenant, size := range before {
			assert.GreaterOrEqual(t, after[tenant], size,
				"tenant %s: rangeable bytes must not shrink across a restart", tenant)
		}
	})

	// Control (b): an ungraceful stop is a crash, not a cancel — the task
	// is still live, so recovery must resume it and finish the job.
	t.Run("Control_SigkillThenResume", func(t *testing.T) {
		const className = "MTRangeControlKill"
		f := seedRangeableFixture(t, className)
		defer helper.DeleteClass(t, className)

		taskID := reindexhelpers.SubmitIndexUpdate(t, restURI, className, "score",
			`{"rangeable":{"enabled":true}}`)
		awaitReindexProgress(t, restURI, className, "score", taskID, 0.4)

		restURI = restart(t, false)

		reindexhelpers.AwaitReindexFinished(t, restURI, taskID,
			reindexhelpers.WithTimeout(5*time.Minute))
		requireRangeFiltersFlag(t, restURI, className, "score", true)
		f.requireAllTenantsCorrect(t, "after SIGKILL + resume")
	})
}

// rangeableFixture is one MT collection seeded so that every tenant has the
// same known number of objects inside the query band.
type rangeableFixture struct {
	className string
	tenants   []string
	// inBand is how many objects per tenant satisfy the range predicate.
	inBand int
}

const (
	// Enough tenants that a cancel mid-migration is guaranteed to catch some
	// shards never-started and some in-flight, at a fine enough progress
	// resolution that a 70% dose can't be skipped between polls.
	rangeableFixtureTenants = 32
	// Large enough that the migration outlives the poll-then-cancel round
	// trip; too few and the cancel races a task that already finished.
	rangeableFixtureObjects = 250
)

// seedRangeableFixture creates an MT collection whose `score` property is
// filterable but not rangeable, so enable-rangeable has real work to do and
// the pre-migration query path is the filterable walk the fallback uses.
func seedRangeableFixture(t *testing.T, className string) *rangeableFixture {
	t.Helper()

	trueVal, falseVal := true, false
	createMTClass(t, className, []*models.Property{
		{Name: "name", DataType: []string{"text"}},
		{
			Name:              "score",
			DataType:          []string{"int"},
			IndexFilterable:   &trueVal,
			IndexRangeFilters: &falseVal,
		},
	})

	tenants := make([]string, 0, rangeableFixtureTenants)
	for i := 0; i < rangeableFixtureTenants; i++ {
		tenants = append(tenants, fmt.Sprintf("t%02d", i))
	}
	addTenants(t, className, tenants)

	for _, tenant := range tenants {
		objects := make([]*models.Object, 0, rangeableFixtureObjects)
		for i := 0; i < rangeableFixtureObjects; i++ {
			objects = append(objects, &models.Object{
				Class: className,
				Properties: map[string]interface{}{
					"name":  "item_" + strconv.Itoa(i),
					"score": float64(i),
				},
				Tenant: tenant,
			})
		}
		helper.CreateObjectsBatch(t, objects)
	}

	f := &rangeableFixture{
		className: className,
		tenants:   tenants,
		// score in [0, rangeableFixtureObjects); the band keeps the top 150.
		inBand: rangeableFixtureObjects - 100,
	}
	f.requireAllTenantsCorrect(t, "baseline, before any migration")
	return f
}

// requireAllTenantsCorrect is the 464 assertion: every tenant answers the
// range filter with the full count. The failure it targets returns 0.
func (f *rangeableFixture) requireAllTenantsCorrect(t *testing.T, phase string) {
	t.Helper()
	var wrong []string
	for _, tenant := range f.tenants {
		// Explicit limit: the shared helper takes GraphQL's default of
		// 100, which would cap a correct answer below the expected count.
		gql := fmt.Sprintf(`{
			Get {
				%s(where: {path:["score"], operator:GreaterThanEqual, valueInt:100},
				   tenant: %q, limit: %d) {
					_additional { id }
				}
			}
		}`, f.className, tenant, rangeableFixtureObjects)
		if got := len(runGraphQLQuery(t, f.className, gql)); got != f.inBand {
			wrong = append(wrong, fmt.Sprintf("%s=%d", tenant, got))
		}
	}
	require.Empty(t, wrong,
		"%s: every tenant must return %d objects for the range filter; wrong tenants (want %d): %v",
		phase, f.inBand, f.inBand, wrong)
}

// assertNoRangeableResidue pins the disk half of 464. A cancelled migration
// must leave neither its sidecar buckets nor a promoted ingest dir behind:
// the next enable has to start from a clean slate, and nothing may sit at
// the canonical name pretending to be an index.
func assertNoRangeableResidue(t *testing.T, dirs []string, tenant string) {
	t.Helper()
	for _, dir := range dirs {
		for _, residue := range []string{
			"__rangeable_reindex", "__rangeable_ingest", "__rangeable_backup",
		} {
			assert.NotContains(t, dir, residue,
				"tenant %s: cancelled migration left sidecar %q behind", tenant, dir)
		}
	}
	assert.NotContains(t, dirs, "property_score_rangeable",
		"tenant %s: cancelled migration left a bucket at the canonical rangeable name; "+
			"if the schema flag were ever true this is exactly what would serve zero rows", tenant)
}

// listShardLSMDirs reads the LSM directory of one tenant's shard from inside
// the container. In an MT collection the shard name is the tenant name.
func listShardLSMDirs(ctx context.Context, t *testing.T, c testcontainers.Container,
	className, tenant string,
) []string {
	t.Helper()
	path := fmt.Sprintf("/data/%s/%s/lsm", strings.ToLower(className), tenant)
	code, reader, err := c.Exec(ctx, []string{"ls", "-1", path})
	require.NoError(t, err, "exec ls on container")
	require.Equal(t, 0, code, "ls %s returned non-zero exit code", path)
	out, err := io.ReadAll(reader)
	require.NoError(t, err)

	var dirs []string
	for _, line := range strings.Split(string(out), "\n") {
		// Exec multiplexes stdout behind an 8-byte frame header, which
		// lands in the middle of the first line.
		if i := strings.LastIndexByte(line, 0x00); i >= 0 {
			line = line[i+1:]
		}
		if line = strings.TrimSpace(line); line != "" {
			dirs = append(dirs, line)
		}
	}
	return dirs
}

// rangeableBucketDir finds a tenant shard's rangeable bucket whatever name
// it currently sits under. A migration that just completed still has its
// data under the ingest name: the rename to the canonical name is deferred
// to the next startup, because renaming a dir whose segments are mmap'd
// would corrupt the store. Both names mean "this shard has the index".
func rangeableBucketDir(dirs []string) string {
	for _, dir := range dirs {
		if strings.HasPrefix(dir, "property_score_rangeable") {
			return dir
		}
	}
	return ""
}

// rangeableBytesPerTenant returns the on-disk size of each tenant shard's
// canonical rangeable bucket. Byte counts, not existence: an empty bucket
// dir is exactly what 464 left behind.
func rangeableBytesPerTenant(ctx context.Context, t *testing.T, c testcontainers.Container,
	className string, tenants []string,
) map[string]int64 {
	t.Helper()
	out := make(map[string]int64, len(tenants))
	for _, tenant := range tenants {
		bucket := rangeableBucketDir(listShardLSMDirs(ctx, t, c, className, tenant))
		if bucket == "" {
			continue
		}
		path := fmt.Sprintf("/data/%s/%s/lsm/%s", strings.ToLower(className), tenant, bucket)
		code, reader, err := c.Exec(ctx, []string{
			"sh", "-c",
			fmt.Sprintf("du -sk %s | cut -f1", path),
		})
		require.NoError(t, err)
		require.Equal(t, 0, code, "du %s returned non-zero exit code", path)
		raw, err := io.ReadAll(reader)
		require.NoError(t, err)
		// Exec multiplexes stdout behind an 8-byte frame header, so keep
		// the digits rather than trusting field positions.
		digits := strings.Map(func(r rune) rune {
			if r >= '0' && r <= '9' {
				return r
			}
			return -1
		}, string(raw))
		require.NotEmpty(t, digits, "du produced no size for %s (raw %q)", path, raw)
		size, err := strconv.ParseInt(digits, 10, 64)
		require.NoError(t, err)
		out[tenant] = size
	}
	return out
}

// awaitReindexProgress blocks until the migration reports at least `want`
// progress, so cancels land at a controlled dose rather than wherever the
// scheduler happens to be.
func awaitReindexProgress(t *testing.T, restURI, className, propName, taskID string, want float32) {
	t.Helper()
	require.Eventually(t, func() bool {
		if task, ok := reindexhelpers.TryFetchTasks(restURI); ok {
			for _, tsk := range task["reindex"] {
				if tsk.ID == taskID && (tsk.Status == "FINISHED" || tsk.Status == "FAILED") {
					t.Fatalf("task %s reached %s before %.0f%% progress; the fixture is too small",
						taskID, tsk.Status, want*100)
				}
			}
		}
		indexes, ok := reindexhelpers.TryGetIndexes(restURI, className)
		if !ok {
			return false
		}
		for _, prop := range indexes.Properties {
			if prop.Name != propName {
				continue
			}
			for _, idx := range prop.Indexes {
				if idx.Type == "rangeable" && idx.Progress >= want {
					t.Logf("task %s reached progress %.2f (wanted %.2f)", taskID, idx.Progress, want)
					return true
				}
			}
		}
		return false
	}, 4*time.Minute, 100*time.Millisecond,
		"enable-rangeable should reach %.0f%% progress", want*100)
}

func cancelReindex(t *testing.T, restURI, className, propName string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut,
		fmt.Sprintf("http://%s/v1/schema/%s/indexes/%s", restURI, className, propName),
		strings.NewReader(`{"rangeable":{"cancel":true}}`))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusAccepted, resp.StatusCode, "cancel failed: %s", body)
}

func awaitTaskStatus(t *testing.T, restURI, taskID, want string) {
	t.Helper()
	require.Eventually(t, func() bool {
		tasks, ok := reindexhelpers.TryFetchTasks(restURI)
		if !ok {
			return false
		}
		for _, task := range tasks["reindex"] {
			if task.ID == taskID {
				return task.Status == want
			}
		}
		return false
	}, 60*time.Second, 500*time.Millisecond, "task %s should reach %s", taskID, want)
}

func requireRangeFiltersFlag(t *testing.T, restURI, className, propName string, want bool) {
	t.Helper()
	require.Eventually(t, func() bool {
		cls, ok := reindexhelpers.FetchClass(restURI, className, false)
		if !ok {
			return false
		}
		for _, prop := range cls.Properties {
			if prop.Name != propName {
				continue
			}
			actual := prop.IndexRangeFilters != nil && *prop.IndexRangeFilters
			if actual != want {
				t.Logf("%s.%s indexRangeFilters is %v, waiting for %v", className, propName, actual, want)
			}
			return actual == want
		}
		return false
	}, 30*time.Second, 500*time.Millisecond,
		"%s.%s indexRangeFilters should be %v", className, propName, want)
}

func dumpWeaviateLogs(ctx context.Context, t *testing.T, c testcontainers.Container) {
	t.Helper()
	reader, err := c.Logs(ctx)
	if err != nil {
		t.Logf("failed to get container logs: %v", err)
		return
	}
	defer reader.Close()
	logs, _ := io.ReadAll(reader)
	lines := strings.Split(string(logs), "\n")
	if len(lines) > 300 {
		lines = lines[len(lines)-300:]
	}
	t.Logf("=== Container logs (last 300 lines) ===\n%s", strings.Join(lines, "\n"))
}

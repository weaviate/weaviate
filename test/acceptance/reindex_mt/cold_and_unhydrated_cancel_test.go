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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"sort"
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

// The pre-submit cleanup sweep walks every shard of a collection to wipe the
// on-disk leftovers of a cancelled reindex. On a multi-tenant collection most
// of those shards are tenants that hold nothing to wipe, and hydrating them
// just to find that out costs a full shard load each. The sweep therefore
// answers "is there anything here?" from the tenant's directory listing and
// leaves a clean, unloaded tenant unloaded.
//
// This journey pins all three populations that answer differently, in one
// run against one collection:
//
//   - HOT + unloaded + stale leftovers → hydrated and swept clean.
//   - HOT + unloaded + nothing to sweep → left unloaded (the saving).
//   - COLD → never reached at all; a deactivated tenant is not in the
//     index's shard map, so its leftovers survive untouched until it is
//     reactivated and a later submit sweeps it.
//
// The third case is the one that is easy to state backwards: a COLD tenant's
// tracker surviving the sweep is correct, not a leak. The final step proves
// it is only deferred by reactivating those tenants and showing the next
// submit does drain them.
const (
	coldCancelClass = "MTColdCancelSweep"
	coldCancelProp  = "score"

	// Objects per tenant. Enough that the first enable-rangeable is still
	// running when the cancel lands, and that a range query returning the
	// wrong half is unmistakable.
	coldCancelObjectsPerTenant = 200

	// Clean HOT tenants. Hydration runs at one tenant/sec in the background,
	// so this count is also how many seconds the re-submit has to land while
	// tenants are still unloaded. 14 is an order of magnitude more than a
	// single HTTP round trip needs.
	coldCancelCleanTenants = 14

	// Tracker dir planted on tenants that must have something to sweep.
	// Generation 9 is far above anything this collection's own migrations
	// claim, so it can't be mistaken for the cancelled run's own.
	coldCancelPlantedDir = "filterable_to_rangeable_" + coldCancelProp + "_9"

	// Prometheus port. The compose does not publish it, so every scrape runs
	// inside the container.
	coldCancelMetricsPort = 2112
)

// sweepTenant is one row of the population matrix above.
type sweepTenant struct {
	name string
	// cold deactivates the tenant before the restart.
	cold bool
	// stale plants a tracker dir the sweep is expected to own.
	stale bool
	// wantTrackerAfterSweep is the state of that tracker dir once the
	// post-restart submit has run its sweep.
	wantTrackerAfterSweep bool
}

func coldCancelTenants() []sweepTenant {
	tenants := []sweepTenant{
		{name: "hot_stale_a", stale: true},
		{name: "hot_stale_b", stale: true},
		{name: "cold_stale_a", cold: true, stale: true, wantTrackerAfterSweep: true},
		{name: "cold_stale_b", cold: true, stale: true, wantTrackerAfterSweep: true},
	}
	for i := 0; i < coldCancelCleanTenants; i++ {
		tenants = append(tenants, sweepTenant{name: fmt.Sprintf("hot_clean_%02d", i)})
	}
	return tenants
}

// testColdAndUnhydratedTenantCancel runs the journey on its own compose. The
// unloaded-shard population it pins only exists under forced-lazy loading,
// which the rest of the suite must not run under (it needs its own
// eager-loaded coverage), so this test cannot share the suite's container.
func testColdAndUnhydratedTenantCancel(t *testing.T) {
	ctx := context.Background()

	compose, err := reindexhelpers.SingleNodeCompose().
		WithWeaviateEnv("LAZY_LOAD_SHARD_COUNT_THRESHOLD", "0").
		// This test checks the cleanup sweep's claim about which tenants it
		// left unloaded against the shard-load metrics, which the sweep does
		// not write. Without it that second reading returns nothing and the
		// test is back to asking the sweep to report on itself.
		WithWeaviateEnv("PROMETHEUS_MONITORING_ENABLED", "true").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate cold-cancel test containers: %s", err.Error())
		}
	}()

	restURI := compose.GetWeaviate().URI()
	container := compose.GetWeaviate().Container()
	helper.SetupClient(restURI)

	tenants := coldCancelTenants()
	falseVal := false
	createMTClass(t, coldCancelClass, []*models.Property{
		{Name: "name", DataType: []string{"text"}, Tokenization: "word"},
		{Name: coldCancelProp, DataType: []string{"int"}, IndexRangeFilters: &falseVal},
	})
	addTenants(t, coldCancelClass, tenantNames(tenants, func(sweepTenant) bool { return true }))
	importColdCancelCorpus(t, tenants)

	// Step 1: leave a cancelled enable-rangeable behind on every tenant. The
	// planting below does not depend on what this leaves on disk, but the
	// journey does: this is where a customer's stale state comes from.
	cancelEnableRangeableInFlight(t, restURI)

	// Step 2: deactivate. A COLD tenant leaves the index's shard map, which is
	// what puts it out of every later sweep's reach.
	coldNames := tenantNames(tenants, func(tn sweepTenant) bool { return tn.cold })
	setTenantStatus(t, coldNames, models.TenantActivityStatusCOLD)

	// Step 3: plant. Deterministic on purpose — whether the cancel above
	// managed to sweep itself clean before the drain timeout is a race, and
	// the populations this test separates must not depend on it.
	for _, tn := range tenants {
		if tn.stale {
			plantStaleTracker(ctx, t, container, tn.name)
		}
	}

	// Step 4: restart. This is what makes every HOT tenant an unloaded
	// lazy shard again, which is the state the sweep's gate exists for.
	require.NoError(t, compose.StopAt(ctx, 0, nil))
	require.NoError(t, compose.StartAt(ctx, 0))
	restURI = compose.GetWeaviate().URI()
	container = compose.GetWeaviate().Container()
	helper.SetupClient(restURI)

	// The cancelled run in step 1 touched every tenant and left residue on
	// whichever subset it had reached, so a tenant this matrix calls clean is
	// only clean by luck — and the sweep rightly hydrates one that is not.
	// Clear it here, while the shards are unloaded and nothing holds the files
	// open, so "nothing to sweep" means what it says.
	clearReindexResidue(ctx, t, container,
		tenantNames(tenants, func(tn sweepTenant) bool { return !tn.cold && !tn.stale }))

	// Step 5: re-submit, naming the HOT tenants — an unnamed COLD tenant would
	// get a task unit and fail on a shard the index map no longer has. The
	// sweep itself is collection-wide regardless: it walks the shard map, not
	// the tenant filter.
	hotNames := tenantNames(tenants, func(tn sweepTenant) bool { return !tn.cold })
	logMark := len(containerLogs(ctx, t, container))

	// The sweep runs inside the submit request, before the task is dispatched,
	// so the shards loaded across this call are the ones it decided to hydrate.
	probeStart := time.Now()
	loadedBefore := loadedShardsOfClass(ctx, t, container, coldCancelClass)
	taskID := reindexhelpers.SubmitIndexUpsert(t, restURI, coldCancelClass, coldCancelProp,
		"rangeable", `{}`, reindexhelpers.WithTenants(hotNames))
	loadedAfter := loadedShardsOfClass(ctx, t, container, coldCancelClass)
	probeWindow := time.Since(probeStart)

	t.Logf("post-restart enable-rangeable task: %s", taskID)
	reindexhelpers.AwaitReindexFinished(t, restURI, taskID)

	sweepLog := containerLogs(ctx, t, container)[logMark:]

	// (b) The direct end-to-end oracle: the sweep left unloaded tenants
	// unloaded. Read twice, because the sweep's own counter is the subject
	// reporting on itself: a gate that skipped one tenant out of fourteen
	// and hydrated the rest still satisfies "more than zero".
	//
	// Both readings only prove something while tenants are still unloaded, so
	// establish that first: a run whose background hydration finished before
	// the submit cannot tell a working gate from a broken one, and must say so
	// rather than pass.
	skipped, found := maxSkippedShards(sweepLog)
	require.True(t, found,
		"the post-restart submit must report a cleanup sweep; no sweep line found in the container log")
	stillUnloaded := len(hotNames) - len(loadedBefore)
	require.GreaterOrEqual(t, stillUnloaded, coldCancelCleanTenants/2,
		"only %d of %d HOT tenants were still unloaded when the submit landed, so this run cannot "+
			"observe what the sweep skipped; background hydration outran the test",
		stillUnloaded, len(hotNames))

	hydrated := newlyLoadedShards(loadedBefore, loadedAfter)
	stayedUnloaded := stillUnloaded - len(hydrated)
	t.Logf("sweep skipped %d of %d unloaded tenants; %d still unloaded after the %s submit window (loaded: %v)",
		skipped, stillUnloaded, stayedUnloaded, probeWindow.Round(time.Millisecond), hydrated)

	// First reading: the counter, with a floor that means something. All
	// fourteen is unattainable, since background hydration loads one tenant
	// per second and races the submit, so half is the floor. Both readings
	// report before the test gives up, so a failure shows whether they agree.
	assert.GreaterOrEqual(t, skipped, coldCancelCleanTenants/2,
		"the sweep hydrated most unloaded tenants: only %d were skipped, of %d clean HOT tenants that "+
			"had nothing to sweep. Those loads run inside the submit handler, so its latency grows with "+
			"tenant count, and tenants the submit does not name get loaded too",
		skipped, coldCancelCleanTenants)

	// Second reading, from a subsystem the sweep does not write: shard-scoped
	// prometheus series appear when LazyLoadShard.Load builds the shard, so a
	// tenant without one at the end of the submit is a tenant nothing
	// hydrated. Same claim as the counter above and the same floor, counted
	// from outside the sweep. Not an exact figure: a few tenants are hydrated
	// per submit by paths other than this sweep, and background hydration
	// keeps loading one per second throughout.
	assert.GreaterOrEqual(t, stayedUnloaded, coldCancelCleanTenants/2,
		"only %d of the %d tenants that were unloaded when the submit landed were still unloaded when "+
			"it returned; the submit is hydrating tenants that could be ruled out from their directory "+
			"listing alone. Loaded: %v",
		stayedUnloaded, stillUnloaded, hydrated)

	// (a) + (c) Whose trackers survived.
	for _, tn := range tenants {
		dirs := trackerDirs(ctx, t, container, tn.name)
		has := containsDir(dirs, coldCancelPlantedDir)
		if tn.wantTrackerAfterSweep {
			assert.True(t, has,
				"tenant %q is COLD, so the sweep cannot reach it; its tracker must survive untouched. dirs: %v",
				tn.name, dirs)
			continue
		}
		assert.False(t, has,
			"tenant %q had a stale tracker the sweep owns and it is still on disk; the next submit resumes "+
				"against it and reports success on an index it never built. dirs: %v",
			tn.name, dirs)
	}

	// (e) The migration itself still landed, on every tenant it named.
	for _, name := range hotNames {
		hits := rangeHits(t, coldCancelClass, coldCancelProp, name, 50, coldCancelObjectsPerTenant*2)
		assert.Len(t, hits, coldCancelObjectsPerTenant/2,
			"tenant %q must answer the range query from a fully built index; %d hits means the migration "+
				"reported success on an empty bucket", name, len(hits))
	}

	// (d) The COLD tenants' state is deferred, not lost: reactivate them and
	// the next submit in the same sweep scope drains them.
	setTenantStatus(t, coldNames, models.TenantActivityStatusHOT)
	drainTaskID := reindexhelpers.RebuildIndex(t, restURI, coldCancelClass, coldCancelProp,
		"filterable", reindexhelpers.WithTenants(coldNames))
	t.Logf("reactivated-tenant filterable rebuild task: %s", drainTaskID)
	reindexhelpers.AwaitReindexFinished(t, restURI, drainTaskID)

	for _, name := range coldNames {
		dirs := trackerDirs(ctx, t, container, name)
		assert.False(t, containsDir(dirs, coldCancelPlantedDir),
			"tenant %q is HOT again, so the sweep reaches it; its tracker must be gone. dirs: %v", name, dirs)
		hits := bm25QueryTenant(t, coldCancelClass, "name", "corpus", name)
		assert.NotEmpty(t, hits, "reactivated tenant %q must still serve its objects", name)
	}
}

// =============================================================================
// Journey steps
// =============================================================================

func importColdCancelCorpus(t *testing.T, tenants []sweepTenant) {
	t.Helper()
	for _, tn := range tenants {
		importCorpus(t, coldCancelClass, coldCancelProp, tn.name, coldCancelObjectsPerTenant)
	}
}

// importCorpus writes one tenant's objects, half of them carrying a value
// above the range-query pivot and half below, so a bucket that lost its
// contents answers zero where a complete one answers half.
func importCorpus(t *testing.T, className, prop, tenant string, n int) {
	t.Helper()
	objs := make([]*models.Object, 0, n)
	for i := 0; i < n; i++ {
		score := 10
		if i%2 == 0 {
			score = 100
		}
		objs = append(objs, &models.Object{
			Class:      className,
			Properties: map[string]interface{}{"name": "corpus doc", prop: score},
			Tenant:     tenant,
		})
	}
	helper.CreateObjectsBatch(t, objs)
}

// cancelEnableRangeableInFlight submits an enable-rangeable across every
// tenant and cancels it at an unsynchronized moment, so the response can be
// 202 CANCELLED, 409 (already past cancellable), or 202 NO_OP (already
// terminal). Every arm waits for a terminal state before returning.
func cancelEnableRangeableInFlight(t *testing.T, restURI string) {
	t.Helper()

	taskID := reindexhelpers.SubmitIndexUpsert(t, restURI, coldCancelClass, coldCancelProp,
		"rangeable", `{}`)
	t.Logf("submitted enable-rangeable %s to cancel in flight", taskID)

	live := false
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		task, ok := fetchTask(restURI, taskID)
		if ok {
			switch task.Status {
			case "STARTED", "PREPARING", "SWAPPING":
				live = true
			case "FINISHED", "FAILED", "CANCELLED":
				t.Logf("task %s reached %s before the cancel could be issued", taskID, task.Status)
				awaitTerminalTask(t, restURI, taskID)
				return
			}
		}
		if live {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	resp := reindexhelpers.CancelIndexRaw(t, restURI, coldCancelClass, coldCancelProp, "rangeable")

	switch resp.StatusCode {
	case http.StatusAccepted:
		var result map[string]string
		require.NoError(t, json.Unmarshal([]byte(resp.Body), &result))
		t.Logf("cancel of %s returned %s", taskID, result["status"])
	case http.StatusConflict:
		t.Logf("cancel raced the completion of %s: %s", taskID, resp.Body)
	default:
		t.Fatalf("unexpected status %d cancelling task %s: %s", resp.StatusCode, taskID, resp.Body)
	}
	awaitTerminalTask(t, restURI, taskID)
}

func fetchTask(restURI, taskID string) (models.DistributedTask, bool) {
	tasks, ok := reindexhelpers.TryFetchTasks(restURI)
	if !ok {
		return models.DistributedTask{}, false
	}
	for _, task := range tasks["reindex"] {
		if task.ID == taskID {
			return task, true
		}
	}
	return models.DistributedTask{}, false
}

func awaitTerminalTask(t *testing.T, restURI, taskID string) {
	t.Helper()
	require.Eventually(t, func() bool {
		task, ok := fetchTask(restURI, taskID)
		if !ok {
			return false
		}
		return task.Status == "FINISHED" || task.Status == "FAILED" || task.Status == "CANCELLED"
	}, 120*time.Second, 100*time.Millisecond, "task %s should reach a terminal state", taskID)
}

func setTenantStatus(t *testing.T, names []string, status string) {
	t.Helper()
	updates := make([]*models.Tenant, len(names))
	for i, name := range names {
		updates[i] = &models.Tenant{Name: name, ActivityStatus: status}
	}
	helper.UpdateTenants(t, coldCancelClass, updates)

	wanted := map[string]bool{status: true}
	switch status {
	case models.TenantActivityStatusCOLD:
		wanted[models.TenantActivityStatusINACTIVE] = true
	case models.TenantActivityStatusHOT:
		wanted[models.TenantActivityStatusACTIVE] = true
	}
	require.Eventually(t, func() bool {
		got, err := helper.GetTenants(t, coldCancelClass)
		if err != nil {
			return false
		}
		pending := len(names)
		for _, tenant := range got.Payload {
			for _, name := range names {
				if tenant.Name == name && wanted[tenant.ActivityStatus] {
					pending--
				}
			}
		}
		return pending == 0
	}, 60*time.Second, 200*time.Millisecond,
		"tenants %v should report activity status %q", names, status)
}

// =============================================================================
// Container filesystem
// =============================================================================

func tenantLSMPath(tenant string) string {
	return fmt.Sprintf("/data/%s/%s/lsm", strings.ToLower(coldCancelClass), tenant)
}

// plantStaleTracker writes the on-disk shape a run that started and never
// finished leaves behind: a tracker dir holding started.mig and nothing else.
// No payload.mig, so the sweep resolves the dir from its own name.
func plantStaleTracker(ctx context.Context, t *testing.T, c testcontainers.Container, tenant string) {
	t.Helper()
	dir := tenantLSMPath(tenant) + "/.migrations/" + coldCancelPlantedDir
	execInContainer(ctx, t, c, fmt.Sprintf(
		"mkdir -p %s && printf '%%s' 2026-01-01T00:00:00.000000000Z > %s/started.mig", dir, dir))
	require.True(t, containsDir(trackerDirs(ctx, t, c, tenant), coldCancelPlantedDir),
		"planted tracker for tenant %q must be on disk before the restart", tenant)
}

// clearReindexResidue makes each tenant's on-disk state look like no
// migration ever ran on it: no tracker dirs, and no sidecar buckets of the
// property under test. Both are what the sweep's gate answers "there is
// something here" from.
func clearReindexResidue(ctx context.Context, t *testing.T, c testcontainers.Container, tenants []string) {
	t.Helper()
	var cmd strings.Builder
	for _, tenant := range tenants {
		lsm := tenantLSMPath(tenant)
		fmt.Fprintf(&cmd, "rm -rf %s/.migrations %s/property_%s__*; ", lsm, lsm, coldCancelProp)
	}
	execInContainer(ctx, t, c, cmd.String())
}

func trackerDirs(ctx context.Context, t *testing.T, c testcontainers.Container, tenant string) []string {
	t.Helper()
	out := execInContainer(ctx, t, c,
		fmt.Sprintf("ls -1 %s/.migrations 2>/dev/null || true", tenantLSMPath(tenant)))
	var dirs []string
	for _, line := range strings.Split(out, "\n") {
		if cleaned := cleanExecLine(line); cleaned != "" {
			dirs = append(dirs, cleaned)
		}
	}
	return dirs
}

func containsDir(dirs []string, want string) bool {
	for _, d := range dirs {
		if d == want {
			return true
		}
	}
	return false
}

// execOutputSentinel absorbs the docker stream framing bytes that prefix each
// exec frame, so the lines the caller cares about arrive clean.
const execOutputSentinel = "___EXEC_BEGIN___"

func execInContainer(ctx context.Context, t *testing.T, c testcontainers.Container, cmd string) string {
	t.Helper()
	code, reader, err := c.Exec(ctx, []string{"sh", "-c", "echo " + execOutputSentinel + "; " + cmd})
	require.NoError(t, err, "exec %q", cmd)
	out, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Zero(t, code, "exec %q failed: %s", cmd, string(out))
	if idx := strings.Index(string(out), execOutputSentinel); idx >= 0 {
		return string(out)[idx+len(execOutputSentinel):]
	}
	return string(out)
}

func cleanExecLine(line string) string {
	return strings.TrimSpace(strings.Map(func(r rune) rune {
		if r < 0x20 {
			return -1
		}
		return r
	}, line))
}

func containerLogs(ctx context.Context, t *testing.T, c testcontainers.Container) string {
	t.Helper()
	reader, err := c.Logs(ctx)
	require.NoError(t, err)
	defer reader.Close()
	out, err := io.ReadAll(reader)
	require.NoError(t, err)
	return string(out)
}

// The metrics probe reports how many lines the metrics page had, so an
// unreachable endpoint fails loudly instead of reading as "no shard is
// loaded", and how many shards it found, so a truncated read cannot pass for
// a small one. Shard names are delimited because docker's exec stream framing
// prefixes each write with bytes that survive into the text.
var (
	metricsLineCountField  = regexp.MustCompile(`LINES=\s*(\d+)`)
	metricsShardCountField = regexp.MustCompile(`COUNT=\s*(\d+)`)
	metricsShardName       = regexp.MustCompile(`<([A-Za-z0-9_./\-]+)>`)
)

// loadedShardsOfClass reports which tenants of className have shard-scoped
// prometheus series. Those series are registered when the shard itself is
// built, so an unloaded lazy shard contributes none and the result is the
// collection's loaded-shard set — published by the shard loader, not by the
// cleanup sweep whose claim it is used to check.
func loadedShardsOfClass(ctx context.Context, t *testing.T, c testcontainers.Container,
	className string,
) map[string]bool {
	t.Helper()
	// Filtered and de-duplicated inside the container: the whole metrics page
	// is far too large to carry over an exec stream intact. The result leaves
	// in a single write, so the one frame header lands ahead of the payload
	// instead of inside a shard name.
	out := execInContainer(ctx, t, c, fmt.Sprintf(
		"wget -qO- http://localhost:%d/metrics > /tmp/metrics.probe 2>/dev/null; "+
			"names=$(grep -i 'class_name=\"%s\"' /tmp/metrics.probe | "+
			"sed -n 's/.*shard_name=\"\\([^\"]*\\)\".*/<\\1>/p' | sort -u | tr -d '\\n'); "+
			"printf 'LINES=%%s COUNT=%%s SHARDS=%%s' "+
			"\"$(wc -l < /tmp/metrics.probe)\" \"$(printf '%%s' \"$names\" | tr -cd '<' | wc -c)\" \"$names\"",
		coldCancelMetricsPort, className))
	out = cleanExecLine(out)

	lines := metricsLineCountField.FindStringSubmatch(out)
	require.NotNil(t, lines, "metrics probe returned no line count: %q", out)
	pageLines, err := strconv.Atoi(lines[1])
	require.NoError(t, err)
	require.Positive(t, pageLines,
		"the metrics endpoint on port %d served nothing; this test needs "+
			"PROMETHEUS_MONITORING_ENABLED on the compose", coldCancelMetricsPort)

	found := metricsShardCountField.FindStringSubmatch(out)
	require.NotNil(t, found, "metrics probe returned no shard count: %q", out)
	wantShards, err := strconv.Atoi(found[1])
	require.NoError(t, err)

	loaded := map[string]bool{}
	names := metricsShardName.FindAllStringSubmatch(out, -1)
	require.Len(t, names, wantShards,
		"the metrics probe reported %d shards but only %d survived the exec stream, so this reading "+
			"undercounts loaded shards: %q", wantShards, len(names), out)
	for _, name := range names {
		// The collection's own index-level series carry this placeholder in
		// the shard label; it is not a tenant.
		if name[1] == "n/a" {
			continue
		}
		loaded[name[1]] = true
	}
	return loaded
}

// newlyLoadedShards is the sorted set of shards present in after but not before.
func newlyLoadedShards(before, after map[string]bool) []string {
	var added []string
	for name := range after {
		if !before[name] {
			added = append(added, name)
		}
	}
	sort.Strings(added)
	return added
}

// skippedShardsField matches the count the cleanup sweep reports. Matched on
// the field rather than the message: the wording around it is being reworked
// on this branch, the field is what the operator greps for.
var skippedShardsField = regexp.MustCompile(`"skipped_shards":\s*(\d+)`)

// maxSkippedShards is the highest count any sweep in logs reported. Highest,
// not the first: a submit sweeps once per index type its migration touches,
// and background cleanup after a terminal task sweeps again.
func maxSkippedShards(logs string) (int, bool) {
	matches := skippedShardsField.FindAllStringSubmatch(logs, -1)
	if len(matches) == 0 {
		return 0, false
	}
	highest := 0
	for _, m := range matches {
		if n, err := strconv.Atoi(m[1]); err == nil && n > highest {
			highest = n
		}
	}
	return highest, true
}

// =============================================================================
// Queries
// =============================================================================

// rangeHits runs a range filter with an explicit limit — the container's
// QUERY_DEFAULTS_LIMIT would otherwise truncate a per-tenant corpus this size
// and make a half-built index look complete.
func rangeHits(t *testing.T, className, prop, tenant string, pivot, limit int) []string {
	t.Helper()
	return runGraphQLQuery(t, className, fmt.Sprintf(`{
		Get {
			%s(where: {path:[%q], operator: LessThan, valueInt: %d}, tenant: %q, limit: %d) {
				_additional { id }
			}
		}
	}`, className, prop, pivot, tenant, limit))
}

func tenantNames(tenants []sweepTenant, keep func(sweepTenant) bool) []string {
	names := make([]string, 0, len(tenants))
	for _, tn := range tenants {
		if keep(tn) {
			names = append(names, tn.name)
		}
	}
	return names
}

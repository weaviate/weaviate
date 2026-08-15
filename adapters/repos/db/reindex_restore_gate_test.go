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

package db

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	entitiesbackup "github.com/weaviate/weaviate/entities/backup"
)

func reindexTask(id string, status distributedtask.TaskStatus, payload string) *distributedtask.Task {
	return &distributedtask.Task{
		TaskDescriptor: distributedtask.TaskDescriptor{ID: id},
		Status:         status,
		Payload:        []byte(payload),
	}
}

func payloadFor(collection string) string {
	return `{"collection":"` + collection + `","unitToShard":{"u1":"s1"}}`
}

func TestNewAnyReindexActivityLookup(t *testing.T) {
	tests := []struct {
		name        string
		tasks       []*distributedtask.Task
		ask         []string
		wantBlocked bool
		wantNamed   string
		wantTaskID  string
	}{
		{
			name:        "live task on the collection asked about",
			tasks:       []*distributedtask.Task{reindexTask("t1", distributedtask.TaskStatusStarted, payloadFor("Movies"))},
			ask:         []string{"Movies"},
			wantBlocked: true,
			wantNamed:   "Movies",
			wantTaskID:  "t1",
		},
		{
			name:  "live task on a collection this restore does not cover",
			tasks: []*distributedtask.Task{reindexTask("t1", distributedtask.TaskStatusStarted, payloadFor("Movies"))},
			ask:   []string{"Shows"},
		},
		{
			// The backup-side overlap check reads an empty list the
			// opposite way.
			name:        "no class named means every collection",
			tasks:       []*distributedtask.Task{reindexTask("t1", distributedtask.TaskStatusStarted, payloadFor("Movies"))},
			wantBlocked: true,
			wantNamed:   "Movies",
			wantTaskID:  "t1",
		},
		{
			name:  "no live task and no class named",
			tasks: []*distributedtask.Task{reindexTask("t1", distributedtask.TaskStatusFinished, payloadFor("Movies"))},
		},
		{
			name:  "terminal task does not block",
			tasks: []*distributedtask.Task{reindexTask("t1", distributedtask.TaskStatusCancelled, payloadFor("Movies"))},
			ask:   []string{"Movies"},
		},
		{
			// A status this build never declared reads as live: the other
			// answer restores over a migration a newer node is running.
			name:        "a status this build cannot classify counts as live",
			tasks:       []*distributedtask.Task{reindexTask("t1", distributedtask.TaskStatus("from-the-future"), payloadFor("Movies"))},
			ask:         []string{"Movies"},
			wantBlocked: true,
			wantNamed:   "Movies",
			wantTaskID:  "t1",
		},
		{
			// The payload names no collection, so nothing says which
			// collection the task holds and every restore is refused.
			name:        "unattributable task blocks a collection it never named",
			tasks:       []*distributedtask.Task{reindexTask("t1", distributedtask.TaskStatusStarted, `{"unitToShard":{"u1":"s1"}}`)},
			ask:         []string{"Shows"},
			wantBlocked: true,
			wantTaskID:  "t1",
		},
		{
			name:        "collection match ignores case",
			tasks:       []*distributedtask.Task{reindexTask("t1", distributedtask.TaskStatusStarted, payloadFor("movies"))},
			ask:         []string{"Movies"},
			wantBlocked: true,
			wantNamed:   "movies",
			wantTaskID:  "t1",
		},
		{
			// A field a newer node retyped: the task stays attributable
			// because the collection is read on its own.
			name:        "shard map retyped by a newer node",
			tasks:       []*distributedtask.Task{reindexTask("t1", distributedtask.TaskStatusStarted, `{"collection":"Movies","unitToShard":"shardA"}`)},
			ask:         []string{"Movies"},
			wantBlocked: true,
			wantNamed:   "Movies",
			wantTaskID:  "t1",
		},
		{
			// Decodes without error and leaves an empty collection, which
			// is the same loss as not decoding at all.
			name:        "collection field renamed by a newer node",
			tasks:       []*distributedtask.Task{reindexTask("t1", distributedtask.TaskStatusStarted, `{"class":"Movies","unitToShard":{"u1":"s1"}}`)},
			ask:         []string{"Shows"},
			wantBlocked: true,
			wantTaskID:  "t1",
		},
		{
			name:        "not json at all",
			tasks:       []*distributedtask.Task{reindexTask("t1", distributedtask.TaskStatusStarted, `not json`)},
			ask:         []string{"Shows"},
			wantBlocked: true,
			wantTaskID:  "t1",
		},
		{
			name:        "truncated mid-payload with the name still visible",
			tasks:       []*distributedtask.Task{reindexTask("t1", distributedtask.TaskStatusStarted, `{"collection":"Movies","unitToShard":{"u1":"sha`)},
			ask:         []string{"Shows"},
			wantBlocked: true,
			wantTaskID:  "t1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lookup := NewAnyReindexActivityLookup(tt.tasks)
			activity, blocked := lookup(tt.ask)
			require.Equal(t, tt.wantBlocked, blocked)
			assert.Equal(t, tt.wantNamed, activity.Collection)
			assert.Equal(t, tt.wantTaskID, activity.TaskID)
		})
	}
}

// Two nodes answering one request have to name the same task, or the same
// restore is refused with two different bodies depending on which node the
// client reached.
func TestNewAnyReindexActivityLookup_NamesTheSameTaskEverywhere(t *testing.T) {
	ascending := []*distributedtask.Task{
		reindexTask("task-a", distributedtask.TaskStatusStarted, payloadFor("Movies")),
		reindexTask("task-b", distributedtask.TaskStatusStarted, payloadFor("Shows")),
	}
	descending := []*distributedtask.Task{ascending[1], ascending[0]}
	for _, tasks := range [][]*distributedtask.Task{ascending, descending} {
		activity, blocked := NewAnyReindexActivityLookup(tasks)(nil)
		require.True(t, blocked)
		require.Equal(t, "task-a", activity.TaskID,
			"the lowest task id must win whatever order the list arrived in")
	}
}

// The tenant-sized fields are what take a real payload into the megabytes.
func tenantScaleTaskPayload(tb testing.TB, collection string, tenants int) []byte {
	tb.Helper()
	p := ReindexTaskPayload{
		MigrationType: ReindexTypeEnableFilterable,
		Collection:    collection,
		Properties:    []string{"a", "b", "c", "d"},
		Tenants:       make([]string, 0, tenants),
		UnitToNode:    make(map[string]string, tenants),
		UnitToShard:   make(map[string]string, tenants),
	}
	for i := 0; i < tenants; i++ {
		tenant := fmt.Sprintf("%08x-3f4b-7c1e-9d2a-6f8b1c3d5e70", i)
		p.Tenants = append(p.Tenants, tenant)
		p.UnitToNode[tenant] = "node-1"
		p.UnitToShard[tenant] = tenant
	}
	doc, err := json.Marshal(p)
	require.NoError(tb, err)
	return doc
}

func terminalTenantScaleTasks(tb testing.TB, tasks, tenants int) []*distributedtask.Task {
	tb.Helper()
	doc := string(tenantScaleTaskPayload(tb, "Docs", tenants))
	out := make([]*distributedtask.Task, 0, tasks)
	for i := 0; i < tasks; i++ {
		out = append(out, reindexTask(fmt.Sprintf("task-%02d", i), distributedtask.TaskStatusFinished, doc))
	}
	return out
}

// The ratio is the assertion, not an absolute: what must hold is that the
// tenants of already-finished migrations cost nothing.
func TestNewAnyReindexActivityLookupSkipsTerminalPayloads(t *testing.T) {
	withTenants := terminalTenantScaleTasks(t, 20, 10_000)
	withoutTenants := terminalTenantScaleTasks(t, 20, 0)
	got := testing.AllocsPerRun(3, func() { NewAnyReindexActivityLookup(withTenants) })
	baseline := testing.AllocsPerRun(3, func() { NewAnyReindexActivityLookup(withoutTenants) })
	require.Less(t, got, baseline*2,
		"a terminal task's payload must never be decoded")
}

func TestRefuseIfAnyReindexInFlight(t *testing.T) {
	live := []*distributedtask.Task{
		reindexTask("t1", distributedtask.TaskStatusStarted, payloadFor("Movies")),
	}
	t.Run("refuses and names the collection the caller asked about", func(t *testing.T) {
		db, hook, built := gatedDB(t, gateFixtures{tasks: live})
		err := db.RefuseIfAnyReindexInFlight(context.Background(), []string{"Shows", "Movies"})
		require.Error(t, err)
		require.ErrorIs(t, err, entitiesbackup.ErrReindexInFlight)
		require.NotErrorIs(t, err, entitiesbackup.ErrBackupBlockedByInFlightReindex,
			"the restore chain and the backup chain must stay separable")
		assert.Contains(t, err.Error(), `collection "Movies"`)
		assert.Equal(t, 1, built.activity, "the gate asks once per call")

		// The task id and the node reach the operator, not the caller.
		assert.NotContains(t, err.Error(), "t1")
		assert.NotContains(t, err.Error(), "node-7")
		require.Len(t, hook.AllEntries(), 1)
		assert.Equal(t, "t1", hook.AllEntries()[0].Data["task_id"])
		assert.Equal(t, "node-7", hook.AllEntries()[0].Data["node"])
	})
	t.Run("admits a restore of collections nothing is migrating", func(t *testing.T) {
		db, hook, _ := gatedDB(t, gateFixtures{tasks: live})
		require.NoError(t, db.RefuseIfAnyReindexInFlight(context.Background(), []string{"Shows"}))
		require.Empty(t, hook.AllEntries(), "an admitted restore logs nothing")
	})
	t.Run("a restore naming no class covers every collection", func(t *testing.T) {
		db, _, _ := gatedDB(t, gateFixtures{tasks: live})
		err := db.RefuseIfAnyReindexInFlight(context.Background(), nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "a collection this restore covers",
			"with several collections in play the refusal must not pick one to name")
		assert.NotContains(t, err.Error(), "Movies",
			"the collection the subject withheld must not come back in the remedy's URLs")
		// The task here is attributable; what withholds the collection is
		// the refusal, so saying the task cannot be attributed is false.
		assert.NotContains(t, err.Error(), "cannot be attributed")
		assert.Contains(t, err.Error(), "GET /v1/tasks",
			"a refusal that names no collection still owes a route to check")
	})
	t.Run("a caller that named one collection is told about that one", func(t *testing.T) {
		unattributable := []*distributedtask.Task{
			reindexTask("t1", distributedtask.TaskStatusStarted, `{"unitToShard":{"u1":"s1"}}`),
		}
		db, _, _ := gatedDB(t, gateFixtures{tasks: unattributable})
		err := db.RefuseIfAnyReindexInFlight(context.Background(), []string{"Shows"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), `collection "Shows"`,
			"it is the only collection the refusal can be about from where the caller stands")
		// Being the subject is not the same as being the one migrating.
		// Nothing observed says a task is on Shows.
		assert.NotContains(t, err.Error(), "has an active runtime-reindex task")
	})
	t.Run("an unreadable task list is not reported as a migration", func(t *testing.T) {
		logger, hook := logrustest.NewNullLogger()
		db := &DB{logger: logger, localNodeName: "node-7"}
		db.SetAnyReindexActivityLookup(func(context.Context) AnyReindexActivityLookup {
			return func([]string) (ReindexActivity, bool) {
				return ReindexActivity{Unreadable: true}, true
			}
		})
		err := db.RefuseIfAnyReindexInFlight(context.Background(), []string{"Movies"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "could not be read")
		// Nothing was observed, so nothing may be named or promised.
		assert.NotContains(t, err.Error(), "has an active runtime-reindex task")
		assert.NotContains(t, err.Error(), "retry after the migration finishes")
		require.Len(t, warnOrAbove(hook), 1)
		assert.Equal(t, reindexReasonTaskListUnreadable, warnOrAbove(hook)[0].Data["reason"])
	})
	t.Run("a node-local hold answers before the cluster is asked", func(t *testing.T) {
		// The hold is a local map read; the DTM question is a
		// leader-forwarded RAFT query a held collection would be refused
		// after anyway.
		db, hook, built := gatedDB(t, gateFixtures{tasks: []*distributedtask.Task{}, holds: map[string]ReindexHold{"Movies": ReindexHoldCleanup}})
		err := db.RefuseIfAnyReindexInFlight(context.Background(), []string{"Movies"})
		require.Error(t, err)
		require.ErrorIs(t, err, entitiesbackup.ErrReindexInFlight)
		assert.Contains(t, err.Error(), "still removing its temporary index files")
		assert.Zero(t, built.activity, "the cluster must not be asked once the local answer is no")
		require.Len(t, hook.AllEntries(), 1)
		assert.Equal(t, ReindexHoldCleanup.String(), hook.AllEntries()[0].Data["reason"])
	})
	t.Run("a hold kind this build does not know still refuses", func(t *testing.T) {
		db, _, built := gatedDB(t, gateFixtures{tasks: []*distributedtask.Task{}, holds: map[string]ReindexHold{"Movies": ReindexHold(99)}})
		err := db.RefuseIfAnyReindexInFlight(context.Background(), []string{"Movies"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "does not recognize")
		assert.Zero(t, built.activity)
	})
	t.Run("the feature flag skips both halves", func(t *testing.T) {
		db, _, built := gatedDB(t, gateFixtures{tasks: live, holds: map[string]ReindexHold{"Movies": ReindexHoldCleanup}})
		db.config.RuntimeReindexDisabled = true
		require.NoError(t, db.RefuseIfAnyReindexInFlight(context.Background(), []string{"Movies"}))
		assert.Zero(t, built.activity, "the flag must be read before any lookup is built")
	})
	t.Run("an unwired gate admits and reports", func(t *testing.T) {
		logger, hook := logrustest.NewNullLogger()
		db := &DB{logger: logger, localNodeName: "node-7"}
		restoreGateWarnBudget = reindexGateWarnBudget{}
		require.NoError(t, db.RefuseIfAnyReindexInFlight(context.Background(), []string{"Movies"}))
		require.Len(t, hook.AllEntries(), 1)
		assert.Equal(t, logrus.WarnLevel, hook.AllEntries()[0].Level)
		assert.Equal(t, "restore", hook.AllEntries()[0].Data["gate"])
	})
}

func TestRefuseIfAnyReindexInFlight_WideRequestLogsBounded(t *testing.T) {
	classes := make([]string, 0, 1000)
	for i := range 1000 {
		classes = append(classes, "Class"+string(rune('A'+i%26))+string(rune('0'+i%10)))
	}
	classes = append(classes, "Movies")
	live := []*distributedtask.Task{
		reindexTask("t1", distributedtask.TaskStatusStarted, payloadFor("Movies")),
	}
	db, hook, _ := gatedDB(t, gateFixtures{tasks: live})
	require.Error(t, db.RefuseIfAnyReindexInFlight(context.Background(), classes))
	require.Len(t, hook.AllEntries(), 1, "one refusal, one entry")
	entry := hook.AllEntries()[0]
	assert.Equal(t, len(classes), entry.Data["requested_class_count"])
	assert.Len(t, entry.Data["requested_classes"], reindexRefusalSampleLimit)
}

// authorization.Backups uppercases its input in place, so a log field
// sharing the caller's array would be rewritten under it.
func TestRefuseIfAnyReindexInFlight_LogDoesNotAliasTheRequest(t *testing.T) {
	classes := make([]string, 0, 20)
	for i := range 20 {
		classes = append(classes, fmt.Sprintf("Class%02d", i))
	}
	live := []*distributedtask.Task{
		reindexTask("t1", distributedtask.TaskStatusStarted, payloadFor(classes[0])),
	}
	db, hook, _ := gatedDB(t, gateFixtures{tasks: live})
	require.Error(t, db.RefuseIfAnyReindexInFlight(context.Background(), classes))
	require.Len(t, hook.AllEntries(), 1)
	logged := hook.AllEntries()[0].Data["requested_classes"].([]string)
	require.Equal(t, "Class00", logged[0])

	classes[0] = "CLASS00"
	assert.Equal(t, "Class00", logged[0], "the log field must not follow the caller's slice")
}

// A line per request buries the log of a node whose wiring never fired; a
// line per process is gone before anyone reads it.
func TestReindexGateWarnBudget(t *testing.T) {
	var budget reindexGateWarnBudget
	start := time.Now()
	assert.True(t, budget.allow(start), "the first report always goes out")
	assert.False(t, budget.allow(start.Add(time.Minute)))
	assert.False(t, budget.allow(start.Add(reindexGateWarnInterval-time.Nanosecond)))
	assert.True(t, budget.allow(start.Add(reindexGateWarnInterval)))
	assert.False(t, budget.allow(start.Add(reindexGateWarnInterval+time.Minute)),
		"the window restarts from the report that was allowed")
}

// The gates are wired separately, so a shared budget would hide whichever
// half is actually broken.
func TestReindexGateWarnBudgetsAreSeparate(t *testing.T) {
	shardGateWarnBudget = reindexGateWarnBudget{}
	restoreGateWarnBudget = reindexGateWarnBudget{}
	logger, hook := logrustest.NewNullLogger()
	db := &DB{logger: logger}
	require.False(t, db.AnyLiveReindexForShard("Movies", "shard-1"))
	require.NoError(t, db.RefuseIfAnyReindexInFlight(context.Background(), []string{"Movies"}))
	gates := make(map[string]int)
	for _, entry := range hook.AllEntries() {
		gates[entry.Data["gate"].(string)]++
	}
	assert.Equal(t, map[string]int{"backup": 1, "restore": 1}, gates)
	actions := map[string]int{}
	for _, entry := range hook.AllEntries() {
		actions[entry.Data["action"].(string)]++
	}
	assert.Equal(t, map[string]int{"backup_reindex_gate": 1, "restore_reindex_gate": 1}, actions,
		"a wiring failure must file under its own gate's action, not the other's")
}

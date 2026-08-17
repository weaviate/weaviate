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

// One set of tasks measured twice, so status is the only variable.
func TestNewAnyReindexActivityLookupSkipsTerminalPayloads(t *testing.T) {
	tasks := terminalTenantScaleTasks(t, 20, 10_000)
	skipped := testing.AllocsPerRun(3, func() { NewAnyReindexActivityLookup(tasks) })
	for _, task := range tasks {
		task.Status = distributedtask.TaskStatusStarted
	}
	decoded := testing.AllocsPerRun(3, func() { NewAnyReindexActivityLookup(tasks) })
	require.Less(t, skipped, decoded/2, "a terminal task's payload must never be decoded")
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

		assert.NotContains(t, err.Error(), "t1")
		assert.NotContains(t, err.Error(), "node-7")
		require.Len(t, hook.AllEntries(), 1)
		assert.Equal(t, "t1", hook.AllEntries()[0].Data["task_id"])
		assert.Equal(t, "node-7", hook.AllEntries()[0].Data["node"])
	})
	t.Run("a case variant is answered in the caller's own spelling", func(t *testing.T) {
		db, _, _ := gatedDB(t, gateFixtures{tasks: live})
		err := db.RefuseIfAnyReindexInFlight(context.Background(), []string{"movies"})
		require.ErrorContains(t, err, `collection "movies" has an active runtime-reindex task`)
		assert.NotContains(t, err.Error(), "Movies",
			"a spelling the caller never used is the cluster's to disclose, remedy routes included")
		assert.NotContains(t, err.Error(), "/v1/schema/movies/",
			"and a route in the caller's spelling answers 404, so it must not be handed out either")
		assert.Contains(t, err.Error(), "GET /v1/tasks",
			"leaving the route that needs no collection name as the one it can offer")
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
		assert.NotContains(t, err.Error(), "has an active runtime-reindex task")
		assert.NotContains(t, err.Error(), "retry after the migration finishes")
		warned := warnOrAbove(hook)
		require.Len(t, warned, 1, "one refused call, one line")
		assert.Equal(t, reindexReasonTaskListUnreadable, warned[0].Data["reason"])
	})
	t.Run("a node-local hold refuses when nothing else does", func(t *testing.T) {
		db, hook, built := gatedDB(t, gateFixtures{tasks: []*distributedtask.Task{}, holds: map[string]ReindexHold{"Movies": ReindexHoldCleanup}})
		err := db.RefuseIfAnyReindexInFlight(context.Background(), []string{"Movies"})
		require.Error(t, err)
		require.ErrorIs(t, err, entitiesbackup.ErrReindexInFlight)
		assert.Contains(t, err.Error(), "still removing its temporary index files")
		assert.Equal(t, 1, built.activity, "the arm that outranks the hold has to be asked")
		require.Len(t, hook.AllEntries(), 1)
		assert.Equal(t, ReindexHoldCleanup.String(), hook.AllEntries()[0].Data["reason"])
	})
	t.Run("a hold outranks an unreadable list, because the hold was observed", func(t *testing.T) {
		db, _, _ := gatedDB(t, gateFixtures{holds: map[string]ReindexHold{"Movies": ReindexHoldCleanup}})
		db.SetAnyReindexActivityLookup(func(context.Context) AnyReindexActivityLookup {
			return func([]string) (ReindexActivity, bool) { return ReindexActivity{Unreadable: true}, true }
		})
		require.ErrorContains(t, db.RefuseIfAnyReindexInFlight(context.Background(), []string{"Movies"}),
			"still removing its temporary index files")
	})
	t.Run("a live task outranks a hold, so the remedy an operator can act on wins", func(t *testing.T) {
		db, _, _ := gatedDB(t, gateFixtures{tasks: live, holds: map[string]ReindexHold{"Movies": ReindexHoldCleanup}})
		err := db.RefuseIfAnyReindexInFlight(context.Background(), []string{"Movies"})
		require.ErrorContains(t, err, "has an active runtime-reindex task")
		assert.NotContains(t, err.Error(), "retry once the cleanup finishes")
	})
	t.Run("the builder hands back nothing", func(t *testing.T) {
		db, _, _ := gatedDB(t, gateFixtures{})
		db.SetAnyReindexActivityLookup(func(context.Context) AnyReindexActivityLookup { return nil })
		require.NoError(t, db.RefuseIfAnyReindexInFlight(context.Background(), []string{"Movies"}))
	})
	t.Run("a lookup that hands nothing back still reads the hold", func(t *testing.T) {
		db, _, _ := gatedDB(t, gateFixtures{holds: map[string]ReindexHold{"Movies": ReindexHoldCleanup}})
		db.SetAnyReindexActivityLookup(func(context.Context) AnyReindexActivityLookup { return nil })
		require.ErrorContains(t, db.RefuseIfAnyReindexInFlight(context.Background(), []string{"Movies"}),
			"still removing its temporary index files",
			"a cluster query that answers nothing must not skip the hold")
	})
	t.Run("an unwired gate still reads the hold", func(t *testing.T) {
		db, _, _ := gatedDB(t, gateFixtures{holds: map[string]ReindexHold{"Movies": ReindexHoldCleanup}})
		require.ErrorContains(t, db.RefuseIfAnyReindexInFlight(context.Background(), []string{"Movies"}),
			"still removing its temporary index files", "the branch that never asks the cluster must not skip the hold")
	})
	t.Run("a hold raised while the cluster is being asked still refuses", func(t *testing.T) {
		db, _, _ := gatedDB(t, gateFixtures{})
		db.SetAnyReindexActivityLookup(func(context.Context) AnyReindexActivityLookup {
			return func([]string) (ReindexActivity, bool) {
				db.reindexHolds.acquire("Movies", ReindexHoldCleanup)
				return ReindexActivity{}, false
			}
		})
		require.ErrorContains(t, db.RefuseIfAnyReindexInFlight(context.Background(), []string{"Movies"}),
			"still removing its temporary index files")
	})
	t.Run("the feature flag skips both halves", func(t *testing.T) {
		db, _, built := gatedDB(t, gateFixtures{tasks: live, holds: map[string]ReindexHold{"Movies": ReindexHoldCleanup}})
		db.config.RuntimeReindexDisabled = true
		require.NoError(t, db.RefuseIfAnyReindexInFlight(context.Background(), []string{"Movies"}))
		assert.Zero(t, built.activity, "the flag must be read before any lookup is built")
	})
}

// authorization.Backups uppercases the caller's slice in place, so the log field must not alias it.
func TestRefuseIfAnyReindexInFlight_LogIsBoundedAndDoesNotAliasTheRequest(t *testing.T) {
	classes := make([]string, 0, 20)
	for i := range 20 {
		classes = append(classes, fmt.Sprintf("Class%02d", i))
	}
	live := []*distributedtask.Task{
		reindexTask("t1", distributedtask.TaskStatusStarted, payloadFor(classes[0])),
	}
	db, hook, _ := gatedDB(t, gateFixtures{tasks: live})
	require.Error(t, db.RefuseIfAnyReindexInFlight(context.Background(), classes))
	require.Len(t, hook.AllEntries(), 1, "one refusal, one entry")
	assert.Equal(t, len(classes), hook.AllEntries()[0].Data["requested_class_count"])
	logged := hook.AllEntries()[0].Data["requested_classes"].([]string)
	require.Len(t, logged, reindexRefusalSampleLimit)
	require.Equal(t, "Class00", logged[0])

	classes[0] = "CLASS00"
	assert.Equal(t, "Class00", logged[0], "the log field must not follow the caller's slice")
}

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

func TestReindexGateWarnBudgetsAreSeparate(t *testing.T) {
	shardGateWarnBudget = reindexGateWarnBudget{}
	restoreGateWarnBudget = reindexGateWarnBudget{}
	logger, hook := logrustest.NewNullLogger()
	db := &DB{logger: logger}
	live, _ := db.AnyLiveReindexForShard("Movies", "shard-1")
	require.False(t, live)
	require.NoError(t, db.RefuseIfAnyReindexInFlight(context.Background(), []string{"Movies"}))
	gates := make(map[string]int)
	for _, entry := range hook.AllEntries() {
		require.Equal(t, logrus.WarnLevel, entry.Level, "an unwired gate is an operator-facing report")
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

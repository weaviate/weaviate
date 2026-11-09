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

package rest

import (
	"encoding/json"
	"maps"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
)

// failApplied runs the FSM's fail apply on task over a schema holding class
// and returns the task as the FSM recorded it.
func failApplied(t *testing.T, task *distributedtask.Task, class *models.Class) *distributedtask.Task {
	raw, err := json.Marshal(task)
	require.NoError(t, err)
	var enc map[string]any
	require.NoError(t, json.Unmarshal(raw, &enc))
	enc["units"] = maps.Collect(task.Units())
	snap, err := json.Marshal(map[string]any{"tasks": map[string]any{db.ReindexNamespace: []any{enc}}})
	require.NoError(t, err)
	m := distributedtask.NewManager(distributedtask.ManagerParameters{
		Logger:                  logrus.New(),
		SchemaMutationDetectors: map[string]distributedtask.SchemaMutationDetector{db.ReindexNamespace: db.ReindexDetector{}},
		ReadOnlyClass:           func(string) *models.Class { return class },
	})
	require.NoError(t, m.Restore(snap))
	sub, err := json.Marshal(&cmd.MarkTaskFailedRequest{
		Namespace: db.ReindexNamespace, Id: task.ID, Version: task.Version,
		Error: "rpc error after the leader committed", FailedAtUnixMillis: time.Now().UnixMilli(),
	})
	require.NoError(t, err)
	require.NoError(t, m.MarkTaskFailed(&cmd.ApplyRequest{SubCommand: sub}))
	list, err := m.ListDistributedTasks(t.Context())
	require.NoError(t, err)
	require.Len(t, list[db.ReindexNamespace], 1)
	return list[db.ReindexNamespace][0]
}

// A run whose cutover write landed while its proposer saw an error, with a
// swap still owed: dropping and re-adding its index must lock nothing, and the
// re-add must rebuild every tenant.
func TestDropAndReAddAfterALandedRun(t *testing.T) {
	cases := []struct {
		name   string
		mt     db.ReindexMigrationType
		tok    string
		idx    string
		landed func() *models.Property
		drop   func(*models.Property)
		readd  *models.IndexUpsertRequest
	}{
		{
			"enable-filterable", db.ReindexTypeEnableFilterable, "", "filterable",
			func() *models.Property {
				return &models.Property{Name: "p", DataType: []string{"text"}, Tokenization: "word", IndexFilterable: boolPtr(true), IndexSearchable: boolPtr(true)}
			},
			func(p *models.Property) { p.IndexFilterable = boolPtr(false) }, &models.IndexUpsertRequest{},
		},
		{
			"enable-searchable", db.ReindexTypeEnableSearchable, "word", "searchable",
			func() *models.Property {
				return &models.Property{Name: "p", DataType: []string{"text"}, Tokenization: "word", IndexFilterable: boolPtr(true), IndexSearchable: boolPtr(true), SearchableBlockmax: boolPtr(true)}
			},
			func(p *models.Property) { p.IndexSearchable, p.SearchableBlockmax = boolPtr(false), nil }, &models.IndexUpsertRequest{Tokenization: "word"},
		},
		{
			"enable-rangeable", db.ReindexTypeEnableRangeable, "", "rangeable",
			func() *models.Property {
				return &models.Property{Name: "p", DataType: []string{"int"}, IndexFilterable: boolPtr(true), IndexRangeFilters: boolPtr(true)}
			},
			func(p *models.Property) { p.IndexRangeFilters = boolPtr(false) }, &models.IndexUpsertRequest{},
		},
		{
			"change-algorithm", db.ReindexTypeChangeAlgorithm, "", "searchable",
			func() *models.Property {
				return &models.Property{Name: "p", DataType: []string{"text"}, Tokenization: "word", IndexFilterable: boolPtr(true), IndexSearchable: boolPtr(true), SearchableBlockmax: boolPtr(true)}
			},
			func(p *models.Property) { p.IndexSearchable, p.SearchableBlockmax = boolPtr(false), nil }, nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := tc.landed()
			class := &models.Class{Class: "C", Properties: []*models.Property{p}, InvertedIndexConfig: &models.InvertedIndexConfig{}}

			run := buildTask(t, "T1", distributedtask.TaskStatusSwapping,
				db.ReindexTaskPayload{Collection: "C", Properties: []string{"p"}, MigrationType: tc.mt, TargetTokenization: tc.tok},
				map[string]*distributedtask.Unit{"a": {ID: "a", GroupID: "a"}, "b": {ID: "b", GroupID: "b"}})
			run.SwapDecidedGroups = map[string]time.Time{"a": {}, "b": {}}
			run.SwapsOwed = map[string][]string{"b": {"n2"}}
			tasks := []*distributedtask.Task{failApplied(t, run, class)}

			require.NoError(t, db.ReindexDetector{}.CheckPropertyUpdate("C", "p", tasks), "the drop")
			tc.drop(p)
			require.NoError(t, db.ReindexDetector{}.CheckPropertyUpdate("C", "p", tasks), "a change after the drop")
			_, err := (&indexesHandlers{}).resolveRebuildPlan(class, p, "filterable", tasks, true)
			require.NotErrorIs(t, err, errRebuildPropertyPartlyMigrated)
			if tc.readd == nil {
				return
			}
			plan, err := resolveUpsertPlan(class, "C", p, tc.idx, tc.readd, tasks, true)
			require.NoError(t, err)
			require.Empty(t, plan.conflict)
			require.Equal(t, tc.mt, plan.migrationType)
			got := skipTenantsAlreadyMigrated(map[string][]string{"n1": {"a", "b"}, "n2": {"a", "b"}}, "C", "p", plan, tasks)
			require.ElementsMatch(t, []string{"a", "b"}, got["n1"])
		})
	}
}

// A class update that turns the BlockMax flag on is not a stopped
// change-algorithm run's own write: its resubmit still runs, and other
// changes stay refused.
func TestAClassUpdateDoesNotLockAPartialChangeAlgorithmRun(t *testing.T) {
	for _, status := range []distributedtask.TaskStatus{distributedtask.TaskStatusCancelled, distributedtask.TaskStatusFailed} {
		for _, group := range []string{"", "a"} {
			t.Run(string(status)+"/group="+group, func(t *testing.T) {
				p := &models.Property{Name: "p", DataType: []string{"text"}, Tokenization: "word", IndexFilterable: boolPtr(true), IndexSearchable: boolPtr(true)}
				class := &models.Class{Class: "C", Properties: []*models.Property{p}, InvertedIndexConfig: &models.InvertedIndexConfig{UsingBlockMaxWAND: true}}
				units := map[string]*distributedtask.Unit{"s1": {ID: "s1"}}
				if group != "" {
					units = map[string]*distributedtask.Unit{"a": {ID: "a", GroupID: "a"}, "b": {ID: "b", GroupID: "b"}}
				}
				task := buildTask(t, "T1", status, db.ReindexTaskPayload{Collection: "C", Properties: []string{"p"}, MigrationType: db.ReindexTypeChangeAlgorithm}, units)
				task.SwapDecidedGroups = map[string]time.Time{group: {}}
				tasks := []*distributedtask.Task{task}

				plan, err := resolveUpsertPlan(class, "C", p, "searchable", &models.IndexUpsertRequest{Algorithm: "blockmax"}, tasks, true)
				require.NoError(t, err)
				require.Empty(t, plan.conflict)
				require.False(t, plan.noop)
				require.Equal(t, db.ReindexTypeChangeAlgorithm, plan.migrationType)
				_, err = (&indexesHandlers{}).resolveRebuildPlan(class, p, "searchable", tasks, true)
				require.ErrorIs(t, err, errRebuildPropertyPartlyMigrated)
			})
		}
	}
}

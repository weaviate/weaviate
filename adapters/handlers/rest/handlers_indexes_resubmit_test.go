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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
)

// Pins that the resubmit skip and the partial status agree on which tenants
// already serve the target, across every run since the last flip.
func TestMigratedTenantsAcrossRuns(t *testing.T) {
	ownership := map[string][]string{"n1": {"a", "b", "c"}, "n2": {"a", "b", "c"}}
	plan := upsertPlan{
		migrationType: db.ReindexTypeChangeTokenization, targetTok: models.PropertyTokenizationField,
		bucketStrategy: "mapcollection",
	}
	start := time.Now()
	// run builds a task holding tenants held, of which decided swapped.
	run := func(version uint64, status distributedtask.TaskStatus, mt db.ReindexMigrationType, tok string, held string, decided ...string) *distributedtask.Task {
		task := buildTask(t, "T", status, db.ReindexTaskPayload{
			Collection: "C", Properties: []string{"title"}, MigrationType: mt,
			TargetTokenization: tok, BucketStrategy: "mapcollection",
		}, nil)
		task.Version, task.StartedAt = version, start.Add(time.Duration(version)*time.Minute)
		units := map[string]*distributedtask.Unit{}
		for _, g := range held {
			units[string(g)] = &distributedtask.Unit{ID: string(g), GroupID: string(g)}
		}
		task.SetUnits(units)
		for _, g := range decided {
			if task.SwapDecidedGroups == nil {
				task.SwapDecidedGroups = map[string]time.Time{}
			}
			task.SwapDecidedGroups[g] = time.Time{}
		}
		return task
	}
	changeTok, rebuild := db.ReindexTypeChangeTokenization, db.ReindexTypeRebuildSearchable
	field, whitespace := models.PropertyTokenizationField, models.PropertyTokenizationWhitespace
	failed, cancelled, finished := distributedtask.TaskStatusFailed, distributedtask.TaskStatusCancelled, distributedtask.TaskStatusFinished
	only := func(tenants ...string) map[string][]string { return map[string][]string{"n1": tenants, "n2": tenants} }

	for _, tc := range []struct {
		name  string
		tasks []*distributedtask.Task
		want  map[string][]string
		// status of the filterable entry; migrated and total apply to partial.
		status          string
		migrated, total int64
	}{
		{
			name:   "a failed identical run skips the tenants it decided",
			tasks:  []*distributedtask.Task{run(1, failed, changeTok, field, "abc", "a")},
			want:   only("b", "c"),
			status: "partial", migrated: 1, total: 3,
		},
		{
			name:   "a cancelled identical run skips them too",
			tasks:  []*distributedtask.Task{run(1, cancelled, changeTok, field, "abc", "a")},
			want:   only("b", "c"),
			status: "partial", migrated: 1, total: 3,
		},
		{
			name:   "another target reruns everything",
			tasks:  []*distributedtask.Task{run(1, failed, changeTok, whitespace, "abc", "a")},
			want:   ownership,
			status: "partial", migrated: 1, total: 3,
		},
		{
			name:   "a run that decided nothing skips nothing",
			tasks:  []*distributedtask.Task{run(1, failed, changeTok, field, "abc")},
			want:   ownership,
			status: "failed",
		},
		{
			name:   "every tenant decided: rerun them all, the flip still has to land",
			tasks:  []*distributedtask.Task{run(1, failed, changeTok, field, "abc", "a", "b", "c")},
			want:   ownership,
			status: "partial", migrated: 3, total: 3,
		},
		{
			name: "a resubmit that swapped nothing keeps the first run's tenants",
			tasks: []*distributedtask.Task{
				run(1, cancelled, changeTok, field, "abc", "a", "b"),
				run(2, cancelled, changeTok, field, "c"),
			},
			want:   only("c"),
			status: "partial", migrated: 2, total: 3,
		},
		{
			name: "a resubmit that swapped some adds them to the first run's",
			tasks: []*distributedtask.Task{
				run(1, cancelled, changeTok, field, "abc", "a"),
				run(2, failed, changeTok, field, "bc", "b"),
			},
			want:   only("c"),
			status: "partial", migrated: 2, total: 3,
		},
		{
			name: "a finished rebuild flips nothing and hides nothing",
			tasks: []*distributedtask.Task{
				run(1, cancelled, changeTok, field, "abc", "a"),
				run(2, finished, rebuild, "", "abc"),
			},
			want:   only("b", "c"),
			status: "partial", migrated: 1, total: 3,
		},
		{
			name: "a newer swap to another target takes the tenant back",
			tasks: []*distributedtask.Task{
				run(1, failed, changeTok, field, "abc", "a", "b"),
				run(2, failed, changeTok, whitespace, "abc", "b"),
			},
			want:   only("b", "c"),
			status: "partial", migrated: 1, total: 3,
		},
		{
			name: "a finished flip of another target rewrote every tenant",
			tasks: []*distributedtask.Task{
				run(1, failed, changeTok, field, "abc", "a"),
				run(2, finished, changeTok, whitespace, "abc"),
			},
			want:   ownership,
			status: "ready",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, skipTenantsAlreadyMigrated(ownership, "C", "title", plan, tc.tasks))

			idx := &models.IndexStatus{Type: "filterable", Status: "ready"}
			mergeReindexStatus(idx, "C", "title", "filterable", parseReindexTasks(tc.tasks), nil)
			require.Equal(t, tc.status, idx.Status)
			require.Equal(t, tc.migrated, idx.MigratedTenants, "migrated tenants")
			require.Equal(t, tc.total, idx.TotalTenants, "total tenants")
		})
	}
}

// Pins that a rebuild of an index some tenants already migrated is refused
// with the resubmit that finishes the migration, and only that index.
func TestRebuildRefusedOnPartlyMigratedIndex(t *testing.T) {
	partial := buildTask(t, "T", distributedtask.TaskStatusCancelled, db.ReindexTaskPayload{
		Collection: "C", Properties: []string{"title"}, MigrationType: db.ReindexTypeChangeTokenizationFilterable,
		TargetTokenization: models.PropertyTokenizationField,
	}, map[string]*distributedtask.Unit{"a": {ID: "a", GroupID: "a"}, "b": {ID: "b", GroupID: "b"}})
	partial.SwapDecidedGroups = map[string]time.Time{"a": {}}
	prop := &models.Property{Name: "title", DataType: []string{"text"}, IndexFilterable: boolPtr(true), IndexSearchable: boolPtr(true)}
	class := &models.Class{Class: "C", Properties: []*models.Property{prop}, InvertedIndexConfig: &models.InvertedIndexConfig{UsingBlockMaxWAND: true}}

	for _, tc := range []struct {
		name      string
		indexType string
		tasks     []*distributedtask.Task
		refused   bool
	}{
		{name: "the migrated index is refused", indexType: "filterable", tasks: []*distributedtask.Task{partial}, refused: true},
		{name: "another index of the property is not", indexType: "searchable", tasks: []*distributedtask.Task{partial}},
		{name: "no partial run, no refusal", indexType: "filterable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := (&indexesHandlers{}).resolveRebuildPlan(class, prop, tc.indexType, tc.tasks)
			if !tc.refused {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, errRebuildPartlyMigrated)
			require.Contains(t, err.Error(), "1 of 2 tenants")
			require.Contains(t, err.Error(), `PUT /v1/schema/C/properties/title/index/filterable -d '{"tokenization":"field"}'`)
		})
	}
}

// Pins that while a partial run stands, a PUT is refused with the resubmit
// unless it is that resubmit, so no second target can hide the partial run.
func TestUpsertRefusedOnPartlyMigratedIndex(t *testing.T) {
	prop := &models.Property{
		Name: "title", DataType: []string{"text"}, Tokenization: models.PropertyTokenizationWord,
		IndexFilterable: boolPtr(true), IndexSearchable: boolPtr(true),
	}
	class := &models.Class{Class: "C", Properties: []*models.Property{prop}, InvertedIndexConfig: &models.InvertedIndexConfig{UsingBlockMaxWAND: true}}
	resubmit := &models.IndexUpsertRequest{Tokenization: models.PropertyTokenizationField}
	want, err := resolveUpsertPlan(class, "C", prop, "searchable", resubmit, nil)
	require.NoError(t, err)
	partial := buildTask(t, "T", distributedtask.TaskStatusCancelled, db.ReindexTaskPayload{
		Collection: "C", Properties: []string{"title"}, MigrationType: want.migrationType,
		TargetTokenization: want.targetTok, BucketStrategy: want.bucketStrategy,
	}, map[string]*distributedtask.Unit{"a": {ID: "a", GroupID: "a"}, "b": {ID: "b", GroupID: "b"}})
	partial.SwapDecidedGroups = map[string]time.Time{"a": {}}

	for _, tc := range []struct {
		name      string
		indexType string
		body      *models.IndexUpsertRequest
		refused   bool
	}{
		{name: "back to the schema's value", indexType: "searchable", body: &models.IndexUpsertRequest{Tokenization: models.PropertyTokenizationWord}, refused: true},
		{name: "back to it on the other index", indexType: "filterable", body: &models.IndexUpsertRequest{Tokenization: models.PropertyTokenizationWord}, refused: true},
		{name: "a third target", indexType: "searchable", body: &models.IndexUpsertRequest{Tokenization: models.PropertyTokenizationWhitespace}, refused: true},
		{name: "the resubmit", indexType: "searchable", body: resubmit},
		{name: "a request naming no tokenization", indexType: "searchable", body: &models.IndexUpsertRequest{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := resolveUpsertPlan(class, "C", prop, tc.indexType, tc.body, []*distributedtask.Task{partial})
			require.NoError(t, err)
			if !tc.refused {
				require.Empty(t, plan.conflict)
				return
			}
			require.Contains(t, plan.conflict, "1 of 2 tenants")
			require.Contains(t, plan.conflict, `PUT /v1/schema/C/properties/title/index/searchable -d '{"tokenization":"field"}'`)
		})
	}
}

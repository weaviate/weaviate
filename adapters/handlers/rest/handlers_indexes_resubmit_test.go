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

// Pins which tenants a resubmitted migration leaves out: only those the
// property's latest run decided, and only when this request is that same
// migration.
func TestSkipTenantsAlreadyMigrated(t *testing.T) {
	ownership := map[string][]string{"n1": {"a", "b"}, "n2": {"a", "b"}}
	plan := upsertPlan{
		migrationType: db.ReindexTypeChangeTokenization, targetTok: models.PropertyTokenizationField,
		bucketStrategy: "mapcollection",
	}
	run := func(version uint64, status distributedtask.TaskStatus, mt db.ReindexMigrationType, tok string, decided ...string) *distributedtask.Task {
		task := buildTask(t, "T", status, db.ReindexTaskPayload{
			Collection: "C", Properties: []string{"title"}, MigrationType: mt,
			TargetTokenization: tok, BucketStrategy: "mapcollection",
		}, nil)
		task.Version = version
		for _, g := range decided {
			if task.SwapDecidedGroups == nil {
				task.SwapDecidedGroups = map[string]time.Time{}
			}
			task.SwapDecidedGroups[g] = time.Time{}
		}
		return task
	}
	changeTok := db.ReindexTypeChangeTokenization
	field, whitespace := models.PropertyTokenizationField, models.PropertyTokenizationWhitespace

	for _, tc := range []struct {
		name  string
		tasks []*distributedtask.Task
		want  map[string][]string
	}{
		{
			name:  "a failed identical run skips the tenants it decided",
			tasks: []*distributedtask.Task{run(1, distributedtask.TaskStatusFailed, changeTok, field, "a")},
			want:  map[string][]string{"n1": {"b"}, "n2": {"b"}},
		},
		{
			name:  "a cancelled identical run skips them too",
			tasks: []*distributedtask.Task{run(1, distributedtask.TaskStatusCancelled, changeTok, field, "a")},
			want:  map[string][]string{"n1": {"b"}, "n2": {"b"}},
		},
		{
			name:  "another target reruns everything",
			tasks: []*distributedtask.Task{run(1, distributedtask.TaskStatusFailed, changeTok, whitespace, "a")},
			want:  ownership,
		},
		{
			name: "only the latest run on the property counts",
			tasks: []*distributedtask.Task{
				run(1, distributedtask.TaskStatusFailed, changeTok, field, "a"),
				run(2, distributedtask.TaskStatusFailed, changeTok, whitespace, "b"),
			},
			want: ownership,
		},
		{
			name:  "a run that decided nothing skips nothing",
			tasks: []*distributedtask.Task{run(1, distributedtask.TaskStatusFailed, changeTok, field)},
			want:  ownership,
		},
		{
			name:  "every tenant decided: rerun them all, the flip still has to land",
			tasks: []*distributedtask.Task{run(1, distributedtask.TaskStatusFailed, changeTok, field, "a", "b")},
			want:  ownership,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, skipTenantsAlreadyMigrated(ownership, "C", "title", plan, tc.tasks))
		})
	}
}

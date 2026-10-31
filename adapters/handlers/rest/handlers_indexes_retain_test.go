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

// TestIndexStatusAfterTaskTTL applies the scheduler's TTL sweep to records
// that are all past the TTL, then reads C.foo's index status from the rest.
func TestIndexStatusAfterTaskTTL(t *testing.T) {
	type rec struct {
		status   distributedtask.TaskStatus
		mt       db.ReindexMigrationType
		prop     string
		badBytes bool
	}
	failed := func(mt db.ReindexMigrationType, prop string) rec {
		return rec{status: distributedtask.TaskStatusFailed, mt: mt, prop: prop}
	}
	withStatus := func(r rec, s distributedtask.TaskStatus) rec { r.status = s; return r }
	filterable := failed(db.ReindexTypeEnableFilterable, "foo")

	for _, tt := range []struct {
		name       string
		records    []rec // oldest first
		wantKept   []int // indexes into records
		filterable string
		searchable string
	}{
		{"a failed migration still reads failed", []rec{filterable}, []int{0}, "failed", "ready"},
		{"a newer failed run replaces it", []rec{filterable, filterable}, []int{1}, "failed", "ready"},
		{"a newer successful run releases it", []rec{filterable, withStatus(filterable, distributedtask.TaskStatusFinished)}, nil, "ready", "ready"},
		{"a running retry releases it", []rec{filterable, withStatus(filterable, distributedtask.TaskStatusStarted)}, []int{1}, "pending", "ready"},
		{"a run on another property keeps it", []rec{filterable, failed(db.ReindexTypeEnableFilterable, "bar")}, []int{0, 1}, "failed", "ready"},
		{"a successful run of another type on the same index releases it", []rec{filterable, withStatus(failed(db.ReindexTypeRepairFilterable, "foo"), distributedtask.TaskStatusFinished)}, nil, "ready", "ready"},
		{"a run on another index keeps it", []rec{filterable, withStatus(failed(db.ReindexTypeEnableRangeable, "foo"), distributedtask.TaskStatusFinished)}, []int{0}, "failed", "ready"},
		{"a run covering only one of its indexes keeps both", []rec{failed(db.ReindexTypeChangeTokenization, "foo"), withStatus(filterable, distributedtask.TaskStatusFinished)}, []int{0, 1}, "ready", "failed"},
		{"runs covering all of its indexes release it", []rec{
			failed(db.ReindexTypeChangeTokenization, "foo"),
			withStatus(filterable, distributedtask.TaskStatusFinished),
			withStatus(failed(db.ReindexTypeRebuildSearchable, "foo"), distributedtask.TaskStatusFinished),
		}, nil, "ready", "ready"},
		{"a cancelled migration is not kept", []rec{withStatus(filterable, distributedtask.TaskStatusCancelled)}, nil, "ready", "ready"},
		{"an unreadable record is not kept", []rec{{status: distributedtask.TaskStatusFailed, badBytes: true}}, nil, "ready", "ready"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Now()
			namespace := map[distributedtask.TaskDescriptor]*distributedtask.Task{}
			var all []*distributedtask.Task
			for i, r := range tt.records {
				task := buildTask(t, string(rune('a'+i)), r.status, db.ReindexTaskPayload{
					MigrationType: r.mt, Collection: "C", Properties: []string{r.prop},
				}, nil)
				task.Version = uint64(i + 1)
				task.StartedAt = now.Add(time.Duration(i) * time.Minute)
				if r.badBytes {
					task.Payload = []byte("{")
				}
				namespace[task.TaskDescriptor] = task
				all = append(all, task)
			}

			retainer := &db.ReindexProvider{}
			var kept []*distributedtask.Task
			var keptIdx []int
			for i, task := range all {
				if task.Status.IsActive() || retainer.ShouldRetainCompletedTask(task, namespace) {
					kept = append(kept, task)
					keptIdx = append(keptIdx, i)
				}
			}

			for indexType, want := range map[string]string{"filterable": tt.filterable, "searchable": tt.searchable} {
				idx := &models.IndexStatus{Type: indexType, Status: "ready"}
				mergeReindexStatus(idx, "C", "foo", indexType, parseReindexTasks(kept), nil)
				require.Equal(t, want, idx.Status, indexType)
			}
			require.Equal(t, tt.wantKept, keptIdx)
		})
	}
}

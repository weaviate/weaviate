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
	"slices"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

// TestIndexStatusAfterTaskTTL keeps each record, all past the TTL, that is
// active or that ShouldRetainCompletedTask retains, then reads C.foo's
// index status from the kept records.
func TestIndexStatusAfterTaskTTL(t *testing.T) {
	type rec struct {
		status     distributedtask.TaskStatus
		mt         db.ReindexMigrationType
		prop       string
		collection string // empty means "C"
		badBytes   bool
	}
	failed := func(mt db.ReindexMigrationType, prop string) rec {
		return rec{status: distributedtask.TaskStatusFailed, mt: mt, prop: prop}
	}
	withStatus := func(r rec, s distributedtask.TaskStatus) rec { r.status = s; return r }
	filterable := failed(db.ReindexTypeEnableFilterable, "foo")
	onD := filterable
	onD.collection = "D"
	changeTokThenFixes := []rec{
		failed(db.ReindexTypeChangeTokenization, "foo"),
		withStatus(filterable, distributedtask.TaskStatusFinished),
		withStatus(failed(db.ReindexTypeRebuildSearchable, "foo"), distributedtask.TaskStatusFinished),
	}

	for _, tt := range []struct {
		name     string
		records  []rec // oldest first
		wantKept []int // indexes into records
		// failedCleanup runs a second sweep in which only these released
		// records' cleanup writes failed.
		failedCleanup []int
		filterableOff bool // the schema has C.foo's filterable index dropped
		filterable    string
		searchable    string
	}{
		{name: "a failed migration still reads failed", records: []rec{filterable}, wantKept: []int{0}, filterable: "failed", searchable: "ready"},
		{name: "a newer failed run replaces it", records: []rec{filterable, filterable}, wantKept: []int{1}, filterable: "failed", searchable: "ready"},
		{name: "a newer successful run releases it", records: []rec{filterable, withStatus(filterable, distributedtask.TaskStatusFinished)}, wantKept: []int{1}, filterable: "ready", searchable: "ready"},
		{name: "a running retry releases it", records: []rec{filterable, withStatus(filterable, distributedtask.TaskStatusStarted)}, wantKept: []int{1}, filterable: "pending", searchable: "ready"},
		{name: "a run on another property keeps it", records: []rec{filterable, failed(db.ReindexTypeEnableFilterable, "bar")}, wantKept: []int{0, 1}, filterable: "failed", searchable: "ready"},
		{name: "a run on another collection keeps it", records: []rec{filterable, withStatus(onD, distributedtask.TaskStatusFinished)}, wantKept: []int{0}, filterable: "failed", searchable: "ready"},
		{name: "a successful run of another type on the same index releases it", records: []rec{filterable, withStatus(failed(db.ReindexTypeRepairFilterable, "foo"), distributedtask.TaskStatusFinished)}, wantKept: []int{1}, filterable: "ready", searchable: "ready"},
		{name: "a run on another index keeps it", records: []rec{filterable, withStatus(failed(db.ReindexTypeEnableRangeable, "foo"), distributedtask.TaskStatusFinished)}, wantKept: []int{0}, filterable: "failed", searchable: "ready"},
		{name: "a run covering only one of its indexes keeps both", records: []rec{failed(db.ReindexTypeChangeTokenization, "foo"), withStatus(filterable, distributedtask.TaskStatusFinished)}, wantKept: []int{0, 1}, filterable: "ready", searchable: "failed"},
		{name: "runs covering all of its indexes release it", records: changeTokThenFixes, wantKept: []int{1, 2}, filterable: "ready", searchable: "ready"},
		{name: "a failed cleanup of it does not bring it back", records: changeTokThenFixes, failedCleanup: []int{0}, wantKept: []int{1, 2}, filterable: "ready", searchable: "ready"},
		{name: "a failed repair on an index dropped since is not kept", records: []rec{failed(db.ReindexTypeRepairFilterable, "foo")}, filterableOff: true, filterable: "ready", searchable: "ready"},
		{name: "a failed enable on an index still off is kept", records: []rec{filterable}, filterableOff: true, wantKept: []int{0}, filterable: "failed", searchable: "ready"},
		{name: "a cancelled migration is not kept", records: []rec{withStatus(filterable, distributedtask.TaskStatusCancelled)}, filterable: "ready", searchable: "ready"},
		{name: "an unreadable record is not kept", records: []rec{{status: distributedtask.TaskStatusFailed, badBytes: true}}, filterable: "ready", searchable: "ready"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Now()
			var all []*distributedtask.Task
			for i, r := range tt.records {
				collection := r.collection
				if collection == "" {
					collection = "C"
				}
				task := buildTask(t, string(rune('a'+i)), r.status, db.ReindexTaskPayload{
					MigrationType: r.mt, Collection: collection, Properties: []string{r.prop},
				}, nil)
				task.Version = uint64(i + 1)
				task.StartedAt = now.Add(time.Duration(i) * time.Minute)
				if r.badBytes {
					task.Payload = []byte("{")
				}
				all = append(all, task)
			}

			reader := schemaUC.NewMockSchemaReader(t)
			reader.EXPECT().ReadOnlyClass("C").Return(&models.Class{Class: "C", Properties: []*models.Property{
				{Name: "foo", IndexFilterable: boolPtr(!tt.filterableOff)},
			}}).Maybe()
			reader.EXPECT().ReadOnlyClass("D").Return(nil).Maybe()
			retainer := db.NewReindexProvider(nil, &schemaUC.Manager{SchemaReader: reader}, nil, logrus.New(), "", nil, nil)
			sweep := func(present []int) []int {
				namespace := map[distributedtask.TaskDescriptor]*distributedtask.Task{}
				for _, i := range present {
					namespace[all[i].TaskDescriptor] = all[i]
				}
				var kept []int
				for _, i := range present {
					if all[i].Status.IsActive() || retainer.ShouldRetainCompletedTask(all[i], namespace) {
						kept = append(kept, i)
					}
				}
				return kept
			}
			var present []int
			for i := range all {
				present = append(present, i)
			}
			keptIdx := sweep(present)
			if tt.failedCleanup != nil {
				keptIdx = sweep(append(keptIdx, tt.failedCleanup...))
				slices.Sort(keptIdx)
			}

			var kept []*distributedtask.Task
			for _, i := range keptIdx {
				kept = append(kept, all[i])
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

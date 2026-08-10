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
	"encoding/json"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/cluster/proto/api"
)

// rebuild-searchable used to be missing from both bucket-touch
// switches, so every path that reached the conflict check with it on
// either side crashed. The tests below walk that path from the
// predicate out to the RAFT apply entry point.

// TestBucketTouchesCoversEveryMigrationType drives the classification
// off allKnownMigrationTypes() instead of a hand-written table, so a
// type that ships without a classification fails here as well as at
// lint time.
func TestBucketTouchesCoversEveryMigrationType(t *testing.T) {
	for _, mt := range allKnownMigrationTypes() {
		t.Run(string(mt), func(t *testing.T) {
			require.NoError(t, ValidateReindexMigrationType(mt))

			_, err := TouchesSearchable(mt)
			require.NoError(t, err)
			_, err = TouchesFilterable(mt)
			require.NoError(t, err)
		})
	}
}

func TestBucketTouchesRebuildSearchable(t *testing.T) {
	searchable, err := TouchesSearchable(ReindexTypeRebuildSearchable)
	require.NoError(t, err)
	require.True(t, searchable, "rebuild-searchable rewrites the searchable bucket")

	filterable, err := TouchesFilterable(ReindexTypeRebuildSearchable)
	require.NoError(t, err)
	require.False(t, filterable, "rebuild-searchable leaves the filterable bucket alone")
}

func TestBucketTouchesUnknownTypeErrors(t *testing.T) {
	phantom := ReindexMigrationType("phantom")

	require.Error(t, ValidateReindexMigrationType(phantom))

	_, err := TouchesSearchable(phantom)
	require.ErrorContains(t, err, `unknown reindex migration type "phantom"`)

	_, err = TouchesFilterable(phantom)
	require.ErrorContains(t, err, `unknown reindex migration type "phantom"`)
}

func TestTypesConflictReason_RebuildSearchable(t *testing.T) {
	tests := []struct {
		name         string
		newType      ReindexMigrationType
		newProps     []string
		existType    ReindexMigrationType
		existProps   []string
		wantConflict bool
	}{
		{
			name: "rebuild-searchable is the new task", newType: ReindexTypeRebuildSearchable,
			newProps: []string{"p"}, existType: ReindexTypeEnableFilterable, existProps: []string{"p"},
			wantConflict: true,
		},
		{
			name: "rebuild-searchable is the in-flight task", newType: ReindexTypeEnableFilterable,
			newProps: []string{"p"}, existType: ReindexTypeRebuildSearchable, existProps: []string{"p"},
			wantConflict: true,
		},
		{
			name: "non-overlapping properties", newType: ReindexTypeRebuildSearchable,
			newProps: []string{"a"}, existType: ReindexTypeEnableFilterable, existProps: []string{"b"},
			wantConflict: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reason, err := typesConflictReason(tc.newType, tc.newProps, tc.existType, tc.existProps)
			require.NoError(t, err)
			require.Equal(t, tc.wantConflict, reason != "", "reason: %q", reason)
		})
	}
}

func TestTypesConflictReason_UnknownTypeErrors(t *testing.T) {
	phantom := ReindexMigrationType("phantom")

	_, err := typesConflictReason(phantom, []string{"p"}, ReindexTypeEnableFilterable, []string{"p"})
	require.ErrorContains(t, err, "new migration type")

	_, err = typesConflictReason(ReindexTypeEnableFilterable, []string{"p"}, phantom, []string{"p"})
	require.ErrorContains(t, err, "in-flight migration type")
}

func reindexPayloadJSON(t *testing.T, mt ReindexMigrationType, collection string, props []string) []byte {
	t.Helper()
	b, err := json.Marshal(ReindexTaskPayload{
		MigrationType: mt, Collection: collection, Properties: props,
	})
	require.NoError(t, err)
	return b
}

func startedReindexTask(id string, payload []byte) *distributedtask.Task {
	return &distributedtask.Task{
		Namespace:      ReindexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: id},
		Status:         distributedtask.TaskStatusStarted,
		Payload:        payload,
	}
}

func TestCheckConflict_RebuildSearchable(t *testing.T) {
	provider := &ReindexProvider{}

	inflightFilterable := []*distributedtask.Task{startedReindexTask(
		"t1", reindexPayloadJSON(t, ReindexTypeEnableFilterable, "Coll", []string{"p"}))}
	err := provider.CheckConflict(
		reindexPayloadJSON(t, ReindexTypeRebuildSearchable, "Coll", []string{"p"}), inflightFilterable)
	require.ErrorContains(t, err, "conflicts")

	inflightRebuild := []*distributedtask.Task{startedReindexTask(
		"t1", reindexPayloadJSON(t, ReindexTypeRebuildSearchable, "Coll", []string{"p"}))}
	err = provider.CheckConflict(
		reindexPayloadJSON(t, ReindexTypeEnableFilterable, "Coll", []string{"p"}), inflightRebuild)
	require.ErrorContains(t, err, "conflicts")

	// A rebuild-searchable submit on a property nobody else is touching
	// must still be accepted.
	err = provider.CheckConflict(
		reindexPayloadJSON(t, ReindexTypeRebuildSearchable, "Coll", []string{"other"}), inflightFilterable)
	require.NoError(t, err)
}

func TestCheckConflict_UnknownTypeIsRejectedNotFatal(t *testing.T) {
	provider := &ReindexProvider{}

	inflight := []*distributedtask.Task{startedReindexTask(
		"t1", reindexPayloadJSON(t, ReindexTypeEnableFilterable, "Coll", []string{"p"}))}

	err := provider.CheckConflict(
		reindexPayloadJSON(t, ReindexMigrationType("phantom"), "Coll", []string{"p"}), inflight)
	require.ErrorContains(t, err, `unknown reindex migration type "phantom"`)
}

// TestAddTask_RebuildSearchable exercises the real RAFT apply entry
// point. A failure here used to kill the node inside Apply, and since
// the log entry is durable it replayed into a crashloop on restart.
func TestAddTask_RebuildSearchable(t *testing.T) {
	logger, _ := test.NewNullLogger()
	manager := distributedtask.NewManager(distributedtask.ManagerParameters{Logger: logger})
	manager.SetConflictDetectors(map[string]distributedtask.ConflictDetector{
		ReindexNamespace: &ReindexProvider{},
	})

	addTask := func(id string, mt ReindexMigrationType) *api.ApplyRequest {
		sub, err := json.Marshal(api.AddDistributedTaskRequest{
			Namespace: ReindexNamespace,
			Id:        id,
			Payload:   reindexPayloadJSON(t, mt, "Coll", []string{"p"}),
			UnitIds:   []string{"u1"},
		})
		require.NoError(t, err)
		return &api.ApplyRequest{SubCommand: sub}
	}

	require.NoError(t, manager.AddTask(addTask("t1", ReindexTypeEnableFilterable), 1))

	// What a concurrent submit puts into the RAFT log. Every node
	// applying it must reject the task, not die.
	err := manager.AddTask(addTask("t2", ReindexTypeRebuildSearchable), 2)
	require.ErrorContains(t, err, "conflicts")

	// Same for a type this build does not know at all.
	err = manager.AddTask(addTask("t3", ReindexMigrationType("phantom")), 3)
	require.ErrorContains(t, err, `unknown reindex migration type "phantom"`)
}

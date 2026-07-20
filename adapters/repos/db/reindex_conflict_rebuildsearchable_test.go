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
	"time"

	"github.com/jonboulle/clockwork"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
)

// TestCheckConflict_RebuildSearchable_NoPanic pins the latent
// cluster-wide poison pill: ReindexTypeRebuildSearchable is submittable
// (handlers_indexes.go) but was absent from the Touches* classification
// switches, so their default arm panicked. CheckConflict exercises those
// switches, and CheckConflict runs in the RAFT FSM AddTask apply path on
// every node — so a rebuild-searchable request racing another active task
// on the same collection crashed the whole cluster, replayed on restart.
//
// Both sub-tests drive the real CheckConflict (never a mocked switch): the
// first directly, the second through Manager.AddTask, the actual FSM apply
// entry point where the panic would poison the cluster.
func TestCheckConflict_RebuildSearchable_NoPanic(t *testing.T) {
	newPayload, err := json.Marshal(ReindexTaskPayload{
		MigrationType: ReindexTypeRebuildSearchable,
		Collection:    "C",
		Properties:    []string{"title"},
	})
	require.NoError(t, err)

	existPayload, err := json.Marshal(ReindexTaskPayload{
		MigrationType: ReindexTypeEnableFilterable,
		Collection:    "C",
		Properties:    []string{"title"},
	})
	require.NoError(t, err)

	t.Run("direct CheckConflict returns conflict, does not panic", func(t *testing.T) {
		provider := &ReindexProvider{}
		existing := []*distributedtask.Task{{
			TaskDescriptor: distributedtask.TaskDescriptor{ID: "existing", Version: 1},
			Status:         distributedtask.TaskStatusStarted,
			Payload:        existPayload,
		}}
		require.NotPanics(t, func() {
			err := provider.CheckConflict(newPayload, existing)
			require.Error(t, err)
			require.Contains(t, err.Error(), "conflict")
		})
	})

	t.Run("Manager.AddTask apply path returns conflict, does not poison the FSM", func(t *testing.T) {
		logger, _ := logrustest.NewNullLogger()
		mgr := distributedtask.NewManager(distributedtask.ManagerParameters{
			Clock:            clockwork.NewFakeClock(),
			CompletedTaskTTL: time.Hour,
			Logger:           logger,
		})
		mgr.SetConflictDetectors(map[string]distributedtask.ConflictDetector{
			ReindexNamespace: &ReindexProvider{},
		})

		// First submit lands: no conflict against an empty task list.
		require.NoError(t, mgr.AddTask(rebuildAddTaskCmd(t, existPayload, "existing"), 1))

		// Second submit is rebuild-searchable on the same collection while
		// the first is STARTED. Pre-fix this panicked inside CheckConflict;
		// post-fix it must return a conflict error and leave the FSM intact.
		var addErr error
		require.NotPanics(t, func() {
			addErr = mgr.AddTask(rebuildAddTaskCmd(t, newPayload, "rebuild"), 2)
		})
		require.Error(t, addErr)
		require.Contains(t, addErr.Error(), "conflict")
	})
}

func rebuildAddTaskCmd(t *testing.T, payload []byte, id string) *cmd.ApplyRequest {
	t.Helper()
	sub, err := json.Marshal(&cmd.AddDistributedTaskRequest{
		Namespace:             ReindexNamespace,
		Id:                    id,
		Payload:               payload,
		UnitIds:               []string{"u-1"},
		SubmittedAtUnixMillis: 1,
	})
	require.NoError(t, err)
	return &cmd.ApplyRequest{SubCommand: sub}
}

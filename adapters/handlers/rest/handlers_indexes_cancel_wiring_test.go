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
	"context"
	"errors"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
)

// fakeClusterTaskCanceler is a scriptable reindexTaskCanceler test double.
type fakeClusterTaskCanceler struct {
	tasks     map[string][]*distributedtask.Task
	cancelErr error
}

func (f fakeClusterTaskCanceler) ListDistributedTasks(context.Context) (map[string][]*distributedtask.Task, error) {
	return f.tasks, nil
}

func (f fakeClusterTaskCanceler) CancelDistributedTask(context.Context, string, string, uint64) error {
	return f.cancelErr
}

// TestCancelReindexTask_ApplyRaceWiring pins: cancelReindexTask must route a
// failed cancel apply through cancelApplyErrorResponse, not an inline 500.
func TestCancelReindexTask_ApplyRaceWiring(t *testing.T) {
	payload := db.ReindexTaskPayload{
		MigrationType: db.ReindexTypeChangeTokenization,
		Collection:    "C",
		Properties:    []string{"title"},
	}

	tests := []struct {
		name       string
		cancelErr  error
		wantAccept bool   // true => 202 Accepted; false => 500
		wantStatus string // asserted Payload.Status when wantAccept
	}{
		{"apply race ErrTaskNotRunning -> 202 NO_OP", distributedtask.ErrTaskNotRunning, true, reindexCancelStatusNoOp},
		{"apply race ErrTaskDoesNotExist -> 202 NO_OP", distributedtask.ErrTaskDoesNotExist, true, reindexCancelStatusNoOp},
		{"successful cancel -> 202 CANCELLED", nil, true, "CANCELLED"},
		{"transient raft error -> 500", errors.New("raft: leadership lost while committing log"), false, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &indexesHandlers{
				appState: &state.State{Logger: logrus.New()},
				clusterTasks: fakeClusterTaskCanceler{
					tasks: map[string][]*distributedtask.Task{
						db.ReindexNamespace: {buildTask(t, "t1", distributedtask.TaskStatusStarted, payload, nil)},
					},
					cancelErr: tt.cancelErr,
				},
			}

			resp := h.cancelReindexTask(context.Background(), "C", "title", "searchable",
				&models.Principal{Username: "u1"})

			if tt.wantAccept {
				accepted, ok := resp.(*schema.SchemaObjectsIndexesUpdateAccepted)
				require.Truef(t, ok, "want 202 Accepted, got %T", resp)
				require.Equal(t, tt.wantStatus, accepted.Payload.Status)
			} else {
				_, ok := resp.(*schema.SchemaObjectsIndexesUpdateInternalServerError)
				require.Truef(t, ok, "want 500 InternalServerError, got %T", resp)
			}
		})
	}
}

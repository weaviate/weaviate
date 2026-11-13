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

package distributedtask

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
)

// A peer's failure committing before a slower node's finalize must not flip the task to FINISHED.
func TestManager_MarkTaskFinalizedLeavesEndedTasksUnchanged(t *testing.T) {
	for _, tc := range []struct {
		status  TaskStatus
		wantErr error
	}{
		{status: TaskStatusFailed, wantErr: ErrTaskNotInFinalizingState},
		{status: TaskStatusCancelled, wantErr: ErrTaskNotInFinalizingState},
		{status: TaskStatusFinished},
	} {
		t.Run(string(tc.status), func(t *testing.T) {
			h := newTestHarness(t).init(t)
			ns, id, version := fixtureInStatus(t, h, tc.status)
			finishedAt := h.manager.tasks[ns][id].FinishedAt
			h.clock.Advance(time.Minute)

			err := h.manager.MarkTaskFinalized(toCmd(t, &cmd.MarkTaskFinalizedRequest{
				Namespace:             ns,
				Id:                    id,
				Version:               version,
				FinalizedAtUnixMillis: h.clock.Now().UnixMilli(),
			}))

			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.status, h.manager.tasks[ns][id].Status)
			require.Equal(t, finishedAt, h.manager.tasks[ns][id].FinishedAt)
		})
	}
}

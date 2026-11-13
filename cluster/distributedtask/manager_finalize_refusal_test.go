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

	"github.com/stretchr/testify/require"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
)

// A finalize proposed by a node that has not yet seen the failure or cancel
// must not turn the task FINISHED.
func TestManager_MarkTaskFinalizedRefusesFailedAndCancelled(t *testing.T) {
	for _, status := range []TaskStatus{TaskStatusFailed, TaskStatusCancelled} {
		t.Run(string(status), func(t *testing.T) {
			h := newTestHarness(t).init(t)
			ns, id, version := fixtureInStatus(t, h, status)

			err := h.manager.MarkTaskFinalized(toCmd(t, &cmd.MarkTaskFinalizedRequest{
				Namespace:             ns,
				Id:                    id,
				Version:               version,
				FinalizedAtUnixMillis: h.clock.Now().UnixMilli(),
			}))

			require.ErrorIs(t, err, ErrTaskNotInFinalizingState)
			require.Equal(t, status, h.manager.tasks[ns][id].Status)
		})
	}
}

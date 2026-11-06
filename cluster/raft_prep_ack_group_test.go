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

package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
)

// Pins that this build's PREP ack names its group, the single-tenant group "" included.
func TestPreparationAckDecidesItsGroup(t *testing.T) {
	for _, tc := range []struct {
		name   string
		groups []string
		want   distributedtask.TaskStatus
	}{
		{name: "one tenant of two", groups: []string{"g", "h"}, want: distributedtask.TaskStatusStarted},
		{name: "a single-tenant run", groups: []string{""}, want: distributedtask.TaskStatusSwapping},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, _ := newBarrierTestStore(t)
			ctx := context.Background()
			const ns, id = "ns", "T"
			var specs []distributedtask.UnitSpec
			for i, g := range tc.groups {
				specs = append(specs, distributedtask.UnitSpec{ID: string(rune('a' + i)), GroupID: g})
			}
			require.NoError(t, srv.AddDistributedTaskWithGroupsBarrier(ctx, ns, id, struct{}{}, specs, true))
			task := srv.LocalDistributedTasks()[ns][0]
			require.NoError(t, srv.RecordDistributedTaskUnitCompletion(ctx, ns, id, task.Version, "Node-1", "a"))
			require.NoError(t, srv.RecordDistributedTaskPreparationCompleteAck(ctx, ns, id, task.Version, tc.groups[0], "Node-1", true, ""))

			task = srv.LocalDistributedTasks()[ns][0]
			require.Contains(t, task.SwapDecidedGroups, tc.groups[0])
			require.Equal(t, tc.want, task.Status)
		})
	}
}

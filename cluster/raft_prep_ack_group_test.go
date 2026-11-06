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

// Pins that a PREP ack this build writes names its group, so it decides that
// group's swap instead of taking the rule for acks written before groups existed.
func TestPreparationAckDecidesItsGroup(t *testing.T) {
	srv, _ := newBarrierTestStore(t)
	ctx := context.Background()
	const ns, id = "ns", "T"

	require.NoError(t, srv.AddDistributedTaskWithGroupsBarrier(ctx, ns, id, struct{}{},
		[]distributedtask.UnitSpec{{ID: "a", GroupID: "g"}, {ID: "b", GroupID: "h"}}, true))
	task := srv.LocalDistributedTasks()[ns][0]
	require.NoError(t, srv.RecordDistributedTaskUnitCompletion(ctx, ns, id, task.Version, "Node-1", "a"))
	require.NoError(t, srv.RecordDistributedTaskPreparationCompleteAck(ctx, ns, id, task.Version, "g", "Node-1", true, ""))

	task = srv.LocalDistributedTasks()[ns][0]
	require.Contains(t, task.SwapDecidedGroups, "g")
	require.Equal(t, distributedtask.TaskStatusStarted, task.Status)
}

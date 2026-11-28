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

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
)

// TestShouldRetainCompletedTask asks the TTL sweep's retainer about one ended task each.
func TestShouldRetainCompletedTask(t *testing.T) {
	for _, tc := range []struct {
		name       string
		status     distributedtask.TaskStatus
		generation uint64
		tenants    []string
		switched   bool
		badBytes   bool
		want       bool
	}{
		{name: "failed with a generation and no switched tenant", status: distributedtask.TaskStatusFailed, generation: 2, want: true},
		{name: "cancelled with a generation and switched tenants", status: distributedtask.TaskStatusCancelled, generation: 2, switched: true, want: true},
		{name: "cancelled with a generation and no switched tenant", status: distributedtask.TaskStatusCancelled, generation: 2, want: true},
		{name: "finished scoped run with switched tenants", status: distributedtask.TaskStatusFinished, generation: 2, tenants: []string{"t1"}, switched: true, want: true},
		{name: "finished with no switched tenant", status: distributedtask.TaskStatusFinished, generation: 2},
		{name: "failed without a generation", status: distributedtask.TaskStatusFailed},
		{name: "cancelled partial run without a generation", status: distributedtask.TaskStatusCancelled, switched: true},
		{name: "unreadable payload", status: distributedtask.TaskStatusFailed, badBytes: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			task := buildTask(t, "a", tc.status, db.ReindexTaskPayload{
				MigrationType: db.ReindexTypeEnableFilterable, Collection: "C", Properties: []string{"foo"},
				Tenants: tc.tenants, ViewServed: tc.generation > 0, Generation: tc.generation,
			}, nil)
			if tc.switched {
				task.SwapDecidedGroups = map[string]time.Time{"t1": {}}
			}
			if tc.badBytes {
				task.Payload = []byte("{")
				task = withHeader(task)
			}
			retainer := db.NewReindexProvider(nil, nil, nil, logrus.New(), "", nil, nil)
			require.Equal(t, tc.want, retainer.ShouldRetainCompletedTask(task, nil))
		})
	}
}

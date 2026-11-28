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
	"fmt"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/distributed_tasks"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/cluster/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

// A quiet new leader refuses task lists until a query starts its catch-up, so each caller retries.
func TestTaskListCallersRetryANewLeadersRefusal(t *testing.T) {
	notCaughtUp := fmt.Errorf("%w: catching up", types.ErrFSMNotCaughtUp)
	tasks := map[string][]*distributedtask.Task{db.DropVectorIndexNamespace: {{
		Namespace:      db.DropVectorIndexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "d", Version: 1},
		Payload:        mustDropPayload(t, "C", "v1"),
		Status:         distributedtask.TaskStatusStarted,
	}}}
	for _, tc := range []struct {
		name string
		call func(t *testing.T, refusals *atomic.Int32)
	}{
		{name: "GET /v1/tasks", call: func(t *testing.T, refusals *atomic.Int32) {
			authorizer := authorization.NewMockAuthorizer(t)
			authorizer.EXPECT().Authorize(mock.Anything, mock.Anything, authorization.READ, authorization.Cluster()).Return(nil)
			api := &operations.WeaviateAPI{}
			setupDistributedTasksHandlers(api, authorizer, fakeTaskLister{tasks: tasks, err: notCaughtUp, refusals: refusals})
			resp := api.DistributedTasksDistributedTasksGetHandler.Handle(
				distributed_tasks.DistributedTasksGetParams{HTTPRequest: httptest.NewRequest("GET", "/v1/tasks", nil)},
				&models.Principal{Username: "cluster-reader"})
			ok, isOK := resp.(*distributed_tasks.DistributedTasksGetOK)
			require.True(t, isOK, "got %T", resp)
			require.Len(t, ok.Payload[db.DropVectorIndexNamespace], 1)
		}},
		{name: "re-issued vector index DELETE", call: func(t *testing.T, refusals *atomic.Int32) {
			enq := &dropVectorIndexEnqueuer{clusterService: &fakeClusterDropClient{
				tasks: tasks, listErr: notCaughtUp, listRefusals: refusals,
			}}
			active, err := enq.HasActiveDrop(context.Background(), "C", "v1")
			require.NoError(t, err)
			require.True(t, active)
		}},
		{name: "property index DELETE's run check", call: func(t *testing.T, refusals *atomic.Int32) {
			h := &schemaHandlers{reindexTaskLister: fakeTaskLister{tasks: tasks, err: notCaughtUp, refusals: refusals}}
			require.False(t, h.reindexRunServesIndex(context.Background(), "C", "title", "filterable"),
				"a refusal answers that a run serves the index")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			refusals := &atomic.Int32{}
			refusals.Store(1)
			tc.call(t, refusals)
			require.Equal(t, int32(-1), refusals.Load(), "refused once, then answered")
		})
	}
}

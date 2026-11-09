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
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/distributed_tasks"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

// A payload the extractor rejects can still name a collection the caller cannot read.
func TestGetTasksHidesCollectionOfUnparseablePayload(t *testing.T) {
	payload := []byte(`{"collection":"Hidden","tenants":42}`)
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.EXPECT().
		Authorize(mock.Anything, mock.Anything, authorization.READ, authorization.Cluster()).
		Return(nil)
	api := &operations.WeaviateAPI{}
	setupDistributedTasksHandlers(api, authorizer, fakeTaskLister{tasks: map[string][]*distributedtask.Task{
		db.ReindexNamespace:         {{TaskDescriptor: distributedtask.TaskDescriptor{ID: "reindex"}, Payload: payload}},
		db.DropVectorIndexNamespace: {{TaskDescriptor: distributedtask.TaskDescriptor{ID: "drop"}, Payload: payload}},
	}})

	resp := api.DistributedTasksDistributedTasksGetHandler.Handle(
		distributed_tasks.DistributedTasksGetParams{HTTPRequest: httptest.NewRequest("GET", "/v1/tasks", nil)},
		&models.Principal{Username: "cluster-reader"})
	ok, isOK := resp.(*distributed_tasks.DistributedTasksGetOK)
	require.True(t, isOK, "got %T", resp)

	for _, namespace := range []string{db.ReindexNamespace, db.DropVectorIndexNamespace} {
		require.Len(t, ok.Payload[namespace], 1, "the task stays listed so a blocking task is visible")
		require.Equal(t, map[string]interface{}{}, ok.Payload[namespace][0].Payload)
	}
	body, err := json.Marshal(ok.Payload)
	require.NoError(t, err)
	require.NotContains(t, string(body), "Hidden")
}

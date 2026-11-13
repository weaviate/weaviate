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
	"encoding/json"
	"fmt"
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
	autherrs "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
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

func TestGetTasksDenialNamesNoHiddenCollection(t *testing.T) {
	principal := &models.Principal{Username: "reader"}
	clusterDenied := autherrs.NewForbidden(principal, authorization.READ, authorization.Cluster())
	tests := []struct {
		name          string
		clusterErr    error
		filterDenies  bool   // filter refuses like adminlist, naming every resource asked about
		wantForbidden string // 403 message; empty means 200 with the task hidden
	}{
		{
			name:          "no cluster read gets a 403 naming only the cluster permission",
			clusterErr:    clusterDenied,
			wantForbidden: clusterDenied.Error(),
		},
		{
			name:         "a filter refusal hides the task instead of naming its collection",
			filterDenies: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			authorizer.EXPECT().
				Authorize(mock.Anything, mock.Anything, authorization.READ, authorization.Cluster()).
				Return(tt.clusterErr)
			if tt.filterDenies {
				authorizer.EXPECT().
					FilterAuthorizedResources(mock.Anything, mock.Anything, authorization.READ, mock.Anything).
					RunAndReturn(func(_ context.Context, p *models.Principal, verb string, res ...string) ([]string, error) {
						return nil, fmt.Errorf("adminlist: %w", autherrs.NewForbidden(p, verb, res...))
					})
			}
			api := &operations.WeaviateAPI{}
			setupDistributedTasksHandlers(api, authorizer, fakeTaskLister{tasks: map[string][]*distributedtask.Task{
				db.ReindexNamespace: {{
					TaskDescriptor: distributedtask.TaskDescriptor{ID: "reindex"},
					Payload:        []byte(`{"collection":"SecretCollection"}`),
				}},
			}})

			resp := api.DistributedTasksDistributedTasksGetHandler.Handle(
				distributed_tasks.DistributedTasksGetParams{HTTPRequest: httptest.NewRequest("GET", "/v1/tasks", nil)},
				principal)
			var payload any
			switch r := resp.(type) {
			case *distributed_tasks.DistributedTasksGetForbidden:
				payload = r.Payload
			case *distributed_tasks.DistributedTasksGetOK:
				payload = r.Payload
			default:
				t.Fatalf("got %T", resp)
			}
			body, err := json.Marshal(payload)
			require.NoError(t, err)
			require.NotContains(t, string(body), "SecretCollection")

			if tt.wantForbidden == "" {
				ok, isOK := resp.(*distributed_tasks.DistributedTasksGetOK)
				require.True(t, isOK, "got %T", resp)
				require.Empty(t, ok.Payload[db.ReindexNamespace])
				return
			}
			forbidden, isForbidden := resp.(*distributed_tasks.DistributedTasksGetForbidden)
			require.True(t, isForbidden, "got %T", resp)
			require.NotNil(t, forbidden.Payload, "the 403 must carry the ErrorResponse the spec declares")
			require.Len(t, forbidden.Payload.Error, 1)
			require.Equal(t, tt.wantForbidden, forbidden.Payload.Error[0].Message)
		})
	}
}

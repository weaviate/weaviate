//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package distributedtask

import (
	"context"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestHandler_ListTasks(t *testing.T) {
	var (
		authorizer = authorization.NewMockAuthorizer(t)
		now        = time.Now()

		namespace = "testNamespace"
		lister    = taskListerStub{
			items: map[string][]*distributedtask.Task{
				namespace: {
					{
						Namespace: namespace,
						TaskDescriptor: distributedtask.TaskDescriptor{
							ID:      "test-task-1",
							Version: 10,
						},
						Payload:    []byte(`{"hello": "world"}`),
						Status:     distributedtask.TaskStatusFailed,
						StartedAt:  now.Add(-time.Hour),
						FinishedAt: now,
						Error:      "server is on fire",
						FinishedNodes: map[string]bool{
							"node1": true,
							"node2": true,
						},
					},
				},
			},
		}
		h = NewHandler(authorizer, lister)
	)

	authorizer.EXPECT().Authorize(mock.Anything, mock.Anything, authorization.READ, authorization.Cluster()).Return(nil)

	tasks, err := h.ListTasks(context.Background(), &models.Principal{})
	require.NoError(t, err)

	require.Equal(t, models.DistributedTasks{
		"testNamespace": []models.DistributedTask{
			{
				ID:            "test-task-1",
				Version:       10,
				Status:        "FAILED",
				Error:         "server is on fire",
				StartedAt:     strfmt.DateTime(now.Add(-time.Hour)),
				FinishedAt:    strfmt.DateTime(now),
				FinishedNodes: []string{"node1", "node2"},
				Payload:       map[string]interface{}{"hello": "world"},
			},
		},
	}, tasks)
}

type taskListerStub struct {
	items map[string][]*distributedtask.Task
}

func (t taskListerStub) ListDistributedTasks(ctx context.Context) (map[string][]*distributedtask.Task, error) {
	return t.items, nil
}

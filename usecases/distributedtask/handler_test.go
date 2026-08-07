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
	"context"
	"encoding/json"
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
				ID:         "test-task-1",
				Version:    10,
				Status:     "FAILED",
				Error:      "server is on fire",
				StartedAt:  strfmt.DateTime(now.Add(-time.Hour)),
				FinishedAt: strfmt.DateTime(now),
				Payload:    map[string]interface{}{"hello": "world"},
				Units:      []*models.DistributedTaskUnit{},
			},
		},
	}, tasks)
}

// An in-flight task carries no finish time, and strfmt.DateTime is a value
// type whose MarshalJSON always writes something — so `finishedAt` renders as
// the zero time rather than being absent or empty. Pinned because the swagger
// description promises exactly this string, and a client testing for an empty
// value would otherwise render "finished 2024 years ago" for every running
// migration.
func TestHandler_ListTasks_InFlightTaskRendersTheZeroFinishedAt(t *testing.T) {
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.EXPECT().Authorize(mock.Anything, mock.Anything, authorization.READ, authorization.Cluster()).Return(nil)

	h := NewHandler(authorizer, taskListerStub{items: map[string][]*distributedtask.Task{
		"ns": {{
			Namespace:      "ns",
			TaskDescriptor: distributedtask.TaskDescriptor{ID: "t1", Version: 1},
			Payload:        []byte(`{}`),
			Status:         distributedtask.TaskStatusSwapping,
			StartedAt:      time.Now().Add(-time.Hour),
		}},
	}})

	tasks, err := h.ListTasks(context.Background(), &models.Principal{})
	require.NoError(t, err)

	rendered, err := json.Marshal(tasks["ns"][0])
	require.NoError(t, err)
	require.Contains(t, string(rendered), `"finishedAt":"0001-01-01T00:00:00.000Z"`,
		"the swagger description documents this exact value for a non-terminal task")
}

type taskListerStub struct {
	items map[string][]*distributedtask.Task
}

func (t taskListerStub) ListDistributedTasks(ctx context.Context) (map[string][]*distributedtask.Task, error) {
	return t.items, nil
}

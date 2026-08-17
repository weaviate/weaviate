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
	const namespace = "testNamespace"

	var (
		now      = time.Now()
		anHourGo = now.Add(-time.Hour)
		dt       = func(t time.Time) *strfmt.DateTime {
			d := strfmt.DateTime(t)
			return &d
		}
	)

	tests := []struct {
		name string
		task *distributedtask.Task
		want models.DistributedTask
		// wantJSON pins that an absent finishedAt is never serialized as the zero value.
		wantJSON string
	}{
		{
			name: "terminal task carries finishedAt",
			task: &distributedtask.Task{
				Namespace:      namespace,
				TaskDescriptor: distributedtask.TaskDescriptor{ID: "test-task-1", Version: 10},
				Payload:        []byte(`{"hello": "world"}`),
				Status:         distributedtask.TaskStatusFailed,
				StartedAt:      anHourGo,
				FinishedAt:     now,
				Error:          "server is on fire",
			},
			want: models.DistributedTask{
				ID:         "test-task-1",
				Version:    10,
				Status:     "FAILED",
				Error:      "server is on fire",
				StartedAt:  strfmt.DateTime(anHourGo),
				FinishedAt: dt(now),
				Payload:    map[string]interface{}{"hello": "world"},
				Units:      []*models.DistributedTaskUnit{},
			},
			wantJSON: `"finishedAt":"` + strfmt.DateTime(now).String() + `"`,
		},
		{
			name: "task mid-coordination omits finishedAt, and so does its unfinished unit",
			task: &distributedtask.Task{
				Namespace:      namespace,
				TaskDescriptor: distributedtask.TaskDescriptor{ID: "test-task-2", Version: 11},
				Payload:        []byte(`{}`),
				Status:         distributedtask.TaskStatusSwapping,
				StartedAt:      anHourGo,
				Units: map[string]*distributedtask.Unit{
					"u-done": {ID: "u-done", NodeID: "n1", Status: distributedtask.UnitStatusCompleted, Progress: 1, UpdatedAt: now, FinishedAt: now},
					"u-live": {ID: "u-live", NodeID: "n2", Status: distributedtask.UnitStatusInProgress, Progress: 0.5, UpdatedAt: now},
					"u-idle": {ID: "u-idle", NodeID: "n3", Status: distributedtask.UnitStatusPending},
				},
			},
			want: models.DistributedTask{
				ID:        "test-task-2",
				Version:   11,
				Status:    "SWAPPING",
				StartedAt: strfmt.DateTime(anHourGo),
				Payload:   map[string]interface{}{},
				Units: []*models.DistributedTaskUnit{
					{ID: "u-done", NodeID: "n1", Status: "COMPLETED", Progress: 1, UpdatedAt: dt(now), FinishedAt: dt(now)},
					{ID: "u-idle", NodeID: "n3", Status: "PENDING"},
					{ID: "u-live", NodeID: "n2", Status: "IN_PROGRESS", Progress: 0.5, UpdatedAt: dt(now)},
				},
			},
			wantJSON: `{"id":"u-idle","nodeId":"n3","status":"PENDING"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			authorizer.EXPECT().
				Authorize(mock.Anything, mock.Anything, authorization.READ, authorization.Cluster()).
				Return(nil)
			h := NewHandler(authorizer, taskListerStub{
				items: map[string][]*distributedtask.Task{namespace: {tt.task}},
			})

			tasks, err := h.ListTasks(context.Background(), &models.Principal{})
			require.NoError(t, err)
			require.Equal(t, models.DistributedTasks{namespace: []models.DistributedTask{tt.want}}, tasks)

			body, err := json.Marshal(tasks)
			require.NoError(t, err)
			require.Contains(t, string(body), tt.wantJSON)
			require.NotContains(t, string(body), "0001-01-01T00:00:00.000Z",
				"an unset timestamp must be absent, not serialized as the zero value")
		})
	}
}

type taskListerStub struct {
	items map[string][]*distributedtask.Task
}

func (t taskListerStub) ListDistributedTasks(ctx context.Context) (map[string][]*distributedtask.Task, error) {
	return t.items, nil
}

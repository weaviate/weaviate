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
	autherrs "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
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
		// authzErr makes the authorizer refuse; the row then asserts the
		// refusal instead of the response shape.
		authzErr error
		want     models.DistributedTask
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
		{
			// The lister holds a task, so a refusal that logged and fell
			// through would hand it to a caller without permission.
			name: "a refused caller gets the error and no tasks",
			task: &distributedtask.Task{
				Namespace:      namespace,
				TaskDescriptor: distributedtask.TaskDescriptor{ID: "test-task-3", Version: 12},
				Payload:        []byte(`{}`),
				Status:         distributedtask.TaskStatusStarted,
				StartedAt:      anHourGo,
			},
			authzErr: autherrs.NewForbidden(&models.Principal{Username: "nobody"},
				authorization.READ, authorization.Cluster()),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			authorizer.EXPECT().
				Authorize(mock.Anything, mock.Anything, authorization.READ, authorization.Cluster()).
				Return(tt.authzErr)
			h := NewHandler(authorizer, rbacconf.Config{}, taskListerStub{
				items: map[string][]*distributedtask.Task{namespace: {tt.task}},
			}, nil)

			tasks, err := h.ListTasks(context.Background(), &models.Principal{})
			if tt.authzErr != nil {
				var forbidden autherrs.Forbidden
				require.ErrorAs(t, err, &forbidden)
				require.Nil(t, tasks)
				return
			}
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

func TestHandler_ListTasksFiltersByCollectionPermissions(t *testing.T) {
	const (
		boundNamespace        = "bound"
		unboundNamespace      = "unbound"
		noCollectionNamespace = "no-collection"
		failed                = `shard "tenant1" failed`
	)
	taskOn := func(collection string) *distributedtask.Task {
		return &distributedtask.Task{
			Namespace:      boundNamespace,
			TaskDescriptor: distributedtask.TaskDescriptor{ID: collection},
			Payload: []byte(`{"collection":"` + collection + `","migrationType":"repair-filterable",` +
				`"tenants":["tenant1"],"unitToShard":{"tenant1__node1":"tenant1"}}`),
			Error: failed,
			Units: map[string]*distributedtask.Unit{
				"tenant1__node1": {ID: "tenant1__node1", Error: failed},
			},
		}
	}
	fullPayload := func(collection string) map[string]interface{} {
		return map[string]interface{}{
			"collection": collection, "migrationType": "repair-filterable",
			"tenants": []interface{}{"tenant1"}, "unitToShard": map[string]interface{}{"tenant1__node1": "tenant1"},
		}
	}
	redactedPayload := func(collection string) map[string]interface{} {
		return map[string]interface{}{"collection": collection, "migrationType": "repair-filterable"}
	}

	type visibleTask struct {
		payload                  map[string]interface{}
		unitID, taskErr, unitErr string
	}
	tests := []struct {
		name   string
		denied []string
		want   map[string]visibleTask
	}{
		{
			name: "full permissions see every task unchanged",
			want: map[string]visibleTask{
				"A": {fullPayload("A"), "tenant1__node1", failed, failed},
				"B": {fullPayload("B"), "tenant1__node1", failed, failed},
			},
		},
		{
			name: "read on collection A only hides B's tasks",
			denied: []string{
				authorization.CollectionsMetadata("B")[0], authorization.ShardsMetadata("B")[0],
			},
			want: map[string]visibleTask{"A": {fullPayload("A"), "tenant1__node1", failed, failed}},
		},
		{
			name: "collection read without tenant read redacts tenant and shard names",
			denied: []string{
				authorization.ShardsMetadata("A")[0], authorization.ShardsMetadata("B")[0],
			},
			want: map[string]visibleTask{
				"A": {redactedPayload("A"), "", redactedError, redactedError},
				"B": {redactedPayload("B"), "", redactedError, redactedError},
			},
		},
	}

	extractCollection := func(payload []byte) (string, bool) {
		var p struct{ Collection string }
		err := json.Unmarshal(payload, &p)
		return p.Collection, err == nil && p.Collection != ""
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := mocks.NewMockAuthorizer()
			authorizer.Deny(tt.denied...)
			unbound := &distributedtask.Task{
				Namespace:      unboundNamespace,
				TaskDescriptor: distributedtask.TaskDescriptor{ID: "unbound"},
				Payload:        []byte(`{"targets":["t"]}`),
			}
			h := NewHandler(authorizer, rbacconf.Config{Enabled: true}, taskListerStub{
				items: map[string][]*distributedtask.Task{
					boundNamespace:   {taskOn("A"), taskOn("B")},
					unboundNamespace: {unbound},
					noCollectionNamespace: {{
						Namespace: noCollectionNamespace,
						Payload:   []byte(`{"collection":"","migrationType":"repair-filterable","tenants":["tenant1"]}`),
					}},
				},
			}, map[string]distributedtask.CollectionExtractor{
				boundNamespace:        extractCollection,
				noCollectionNamespace: extractCollection,
			})

			tasks, err := h.ListTasks(context.Background(), &models.Principal{})
			require.NoError(t, err)

			got := map[string]visibleTask{}
			for _, task := range tasks[boundNamespace] {
				got[task.ID] = visibleTask{task.Payload.(map[string]interface{}), task.Units[0].ID, task.Error, task.Units[0].Error}
			}
			require.Equal(t, tt.want, got)
			require.Equal(t, redactedPayload(""), tasks[noCollectionNamespace][0].Payload,
				"a payload without a readable collection hides tenant names")
			require.Len(t, tasks[unboundNamespace], 1, "a task bound to no collection stays visible to cluster readers")
			require.Equal(t, map[string]interface{}{"targets": []interface{}{"t"}}, tasks[unboundNamespace][0].Payload)
		})
	}
}

type taskListerStub struct {
	items map[string][]*distributedtask.Task
}

func (t taskListerStub) ListDistributedTasks(ctx context.Context) (map[string][]*distributedtask.Task, error) {
	return t.items, nil
}

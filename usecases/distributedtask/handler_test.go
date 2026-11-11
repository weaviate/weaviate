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
	"path/filepath"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	autherrs "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
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
			h := NewHandler(authorizer, taskListerStub{
				items: map[string][]*distributedtask.Task{namespace: {tt.task}},
			}, nil, nil)

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
			Payload: []byte(`{"collection":"` + collection + `","migrationType":"repair-filterable","note":"tenant1",` +
				`"tenants":["tenant1"],"unitToShard":{"tenant1__node1":"tenant1"}}`),
			Error: failed,
			Units: map[string]*distributedtask.Unit{
				"tenant1__node1": {ID: "tenant1__node1", Error: failed},
			},
		}
	}
	fullPayload := func(collection string) map[string]interface{} {
		return map[string]interface{}{
			"collection": collection, "migrationType": "repair-filterable", "note": "tenant1",
			"tenants": []interface{}{"tenant1"}, "unitToShard": map[string]interface{}{"tenant1__node1": "tenant1"},
		}
	}
	redactedPayload := func(collection string) map[string]interface{} {
		return map[string]interface{}{"collection": collection, "migrationType": "repair-filterable"}
	}
	readCollection := func(c string) *models.Permission {
		return &models.Permission{
			Action:      authorization.String(authorization.ReadCollections),
			Collections: &models.PermissionCollections{Collection: &c},
		}
	}
	readTenants := func(c string) *models.Permission {
		return &models.Permission{
			Action:  authorization.String(authorization.ReadTenants),
			Tenants: &models.PermissionTenants{Collection: &c, Tenant: authorization.String("*")},
		}
	}

	type visibleTask struct {
		payload                  map[string]interface{}
		unitID, taskErr, unitErr string
	}
	tests := []struct {
		name        string
		collections []string
		permissions []*models.Permission
		want        map[string]visibleTask
	}{
		{
			name:        "read on collection A only hides B's tasks",
			collections: []string{"A", "B"},
			permissions: []*models.Permission{readCollection("A"), readTenants("A")},
			want:        map[string]visibleTask{"A": {fullPayload("A"), "tenant1__node1", failed, failed}},
		},
		{
			name:        "collection read without tenant read redacts tenant and shard names",
			collections: []string{"A", "B"},
			permissions: []*models.Permission{readCollection("A"), readCollection("B")},
			want: map[string]visibleTask{
				"A": {redactedPayload("A"), "", redactedError, redactedError},
				"B": {redactedPayload("B"), "", redactedError, redactedError},
			},
		},
		{
			// Guards against a shared-wildcard check matching the tenant grant too.
			name:        "tenant read without collection read hides the only task",
			collections: []string{"A"},
			permissions: []*models.Permission{readTenants("A")},
			want:        map[string]visibleTask{},
		},
	}

	extractCollection := func(payload []byte) (string, bool) {
		var p struct{ Collection string }
		err := json.Unmarshal(payload, &p)
		return p.Collection, err == nil && p.Collection != ""
	}
	principal := &models.Principal{Username: "user", UserType: models.UserTypeInputDb}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := logrustest.NewNullLogger()
			authorizer, err := rbac.New(filepath.Join(t.TempDir(), "policy.csv"), rbacconf.Config{Enabled: true},
				config.Authentication{}, false, nil, logger)
			require.NoError(t, err)
			policies, err := conv.RolesToPolicies(&models.Role{
				Name:        authorization.String("role"),
				Permissions: append([]*models.Permission{{Action: authorization.String(authorization.ReadCluster)}}, tt.permissions...),
			})
			require.NoError(t, err)
			require.NoError(t, authorizer.CreateRolesPermissions(policies))
			require.NoError(t, authorizer.AddRolesForUser(conv.UserNameWithTypeFromPrincipal(principal), []string{"role"}))

			bound := make([]*distributedtask.Task, 0, len(tt.collections))
			for _, c := range tt.collections {
				bound = append(bound, taskOn(c))
			}
			unbound := &distributedtask.Task{
				Namespace:      unboundNamespace,
				TaskDescriptor: distributedtask.TaskDescriptor{ID: "unbound"},
				Payload:        []byte(`{"targets":["t"]}`),
			}
			h := NewHandler(authorizer, taskListerStub{
				items: map[string][]*distributedtask.Task{
					boundNamespace:   bound,
					unboundNamespace: {unbound},
					noCollectionNamespace: {{
						Namespace: noCollectionNamespace,
						Payload:   []byte(`{"collection":"","migrationType":"repair-filterable","tenants":["tenant1"]}`),
						Units:     map[string]*distributedtask.Unit{"tenant1__node1": {ID: "tenant1__node1"}},
					}},
				},
			}, map[string]distributedtask.CollectionExtractor{
				boundNamespace:        extractCollection,
				noCollectionNamespace: extractCollection,
			}, map[string][]string{
				boundNamespace:        {"collection", "migrationType"},
				noCollectionNamespace: {"collection", "migrationType"},
			})

			tasks, err := h.ListTasks(context.Background(), principal)
			require.NoError(t, err)

			got := map[string]visibleTask{}
			for _, task := range tasks[boundNamespace] {
				got[task.ID] = visibleTask{task.Payload.(map[string]interface{}), task.Units[0].ID, task.Error, task.Units[0].Error}
			}
			require.Equal(t, tt.want, got)
			noCollection := tasks[noCollectionNamespace][0]
			require.Equal(t, visibleTask{map[string]interface{}{}, "", "", ""},
				visibleTask{noCollection.Payload.(map[string]interface{}), noCollection.Units[0].ID, noCollection.Error, noCollection.Units[0].Error},
				"a payload the extractor rejects shows no fields, and a healthy task must not read as failed")
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

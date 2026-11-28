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
	"errors"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db"
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
			}, nil, listOnce)

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
			Summary: &distributedtask.UnitSummary{
				Groups: 1, Counts: map[distributedtask.UnitStatus]int{distributedtask.UnitStatusFailed: 1}, Errors: []string{failed},
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
		payload                              map[string]interface{}
		unitID, taskErr, unitErr, summaryErr string
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
			want:        map[string]visibleTask{"A": {fullPayload("A"), "tenant1__node1", failed, failed, failed}},
		},
		{
			name:        "collection read without tenant read redacts tenant and shard names",
			collections: []string{"A", "B"},
			permissions: []*models.Permission{readCollection("A"), readCollection("B")},
			want: map[string]visibleTask{
				"A": {redactedPayload("A"), "", redactedError, redactedError, redactedError},
				"B": {redactedPayload("B"), "", redactedError, redactedError, redactedError},
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

	principal := &models.Principal{Username: "user", UserType: models.UserTypeInputDb}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := rbacAuthorizer(t, principal, tt.permissions...)

			stored := make([]*distributedtask.Task, 0, len(tt.collections)+2)
			for _, c := range tt.collections {
				stored = append(stored, taskOn(c))
			}
			stored = append(stored, &distributedtask.Task{
				Namespace:      unboundNamespace,
				TaskDescriptor: distributedtask.TaskDescriptor{ID: "unbound"},
				Payload:        []byte(`{"targets":["t"]}`),
			}, &distributedtask.Task{
				Namespace: noCollectionNamespace,
				Payload:   []byte(`{"collection":"","migrationType":"repair-filterable","tenants":["tenant1"]}`),
				Units:     map[string]*distributedtask.Unit{"tenant1__node1": {ID: "tenant1__node1"}},
			})
			h := NewHandler(authorizer, restoredLister(t, map[string]distributedtask.ConflictDetector{
				boundNamespace: collectionDecoder{}, noCollectionNamespace: collectionDecoder{},
			}, stored...), []string{boundNamespace, noCollectionNamespace}, listOnce)

			tasks, err := h.ListTasks(context.Background(), principal)
			require.NoError(t, err)

			got := map[string]visibleTask{}
			for _, task := range tasks[boundNamespace] {
				require.Equal(t, map[string]int64{"FAILED": 1}, task.UnitSummary.Counts, "every caller sees the counts")
				got[task.ID] = visibleTask{
					task.Payload.(map[string]interface{}), task.Units[0].ID, task.Error, task.Units[0].Error, task.UnitSummary.Errors[0],
				}
				if got[task.ID].unitID == "" {
					rendered, err := json.Marshal(task)
					require.NoError(t, err)
					require.NotContains(t, string(rendered), "tenant1", "a redacted task names no tenant")
				}
			}
			require.Equal(t, tt.want, got)
			noCollection := tasks[noCollectionNamespace][0]
			require.Equal(t, visibleTask{map[string]interface{}{}, "", "", "", ""},
				visibleTask{noCollection.Payload.(map[string]interface{}), noCollection.Units[0].ID, noCollection.Error, noCollection.Units[0].Error, ""},
				"a payload with no header shows no fields, and a healthy task must not read as failed")
			require.Len(t, tasks[unboundNamespace], 1, "a task bound to no collection stays visible to cluster readers")
			require.Equal(t, map[string]interface{}{"targets": []interface{}{"t"}}, tasks[unboundNamespace][0].Payload)
		})
	}
}

// A caller without tenant read gets the header's visible fields, and its listing decodes no payload.
func TestHandler_RedactedPayloadComesFromTheHeader(t *testing.T) {
	padding := strings.Repeat("x", 50<<20)
	for _, tc := range []struct {
		name      string
		namespace string
		detector  distributedtask.ConflictDetector
		payload   any
		want      map[string]any
	}{
		{
			name: "reindex task with a large hidden field", namespace: db.ReindexNamespace, detector: db.ReindexDetector{},
			payload: map[string]any{
				"collection": "A", "migrationType": "change-tokenization", "targetTokenization": "field",
				"originalTokenization": "word", "bucketStrategy": "mapcollection",
				"tenants": []string{"tenant1"}, "padding": padding,
			},
			want: map[string]any{
				"collection": "A", "migrationType": "change-tokenization", "targetTokenization": "field",
				"originalTokenization": "word", "bucketStrategy": "mapcollection",
			},
		},
		{
			name: "drop-vector task", namespace: db.DropVectorIndexNamespace, detector: db.NewDropVectorIndexDetector(logrus.New()),
			payload: db.DropVectorIndexTaskPayload{
				Collection: "A", Targets: []string{"v"}, OpID: "op", DropEpochID: "epoch", VerifyOnly: true,
				CleanedShards: []string{"tenant1"},
			},
			want: map[string]any{"collection": "A", "opId": "op", "dropEpochId": "epoch", "verifyOnly": true},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			principal := &models.Principal{Username: "user", UserType: models.UserTypeInputDb}
			c := "A"
			authorizer := rbacAuthorizer(t, principal, &models.Permission{
				Action: authorization.String(authorization.ReadCollections), Collections: &models.PermissionCollections{Collection: &c},
			})
			raw, err := json.Marshal(tc.payload)
			require.NoError(t, err)
			h := NewHandler(authorizer, restoredLister(t, map[string]distributedtask.ConflictDetector{tc.namespace: tc.detector},
				&distributedtask.Task{Namespace: tc.namespace, TaskDescriptor: distributedtask.TaskDescriptor{ID: "T", Version: 1}, Payload: raw},
			), []string{tc.namespace}, listOnce)

			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			tasks, err := h.ListTasks(context.Background(), principal)
			runtime.ReadMemStats(&after)
			require.NoError(t, err)
			require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(1<<20))
			require.Equal(t, tc.want, tasks[tc.namespace][0].Payload)
		})
	}
}

func rbacAuthorizer(t *testing.T, principal *models.Principal, permissions ...*models.Permission) authorization.Authorizer {
	t.Helper()
	logger, _ := logrustest.NewNullLogger()
	authorizer, err := rbac.New(filepath.Join(t.TempDir(), "policy.csv"), rbacconf.Config{Enabled: true},
		config.Authentication{}, false, nil, logger)
	require.NoError(t, err)
	policies, err := conv.RolesToPolicies(&models.Role{
		Name:        authorization.String("role"),
		Permissions: append([]*models.Permission{{Action: authorization.String(authorization.ReadCluster)}}, permissions...),
	})
	require.NoError(t, err)
	require.NoError(t, authorizer.CreateRolesPermissions(policies))
	require.NoError(t, authorizer.AddRolesForUser(conv.UserNameWithTypeFromPrincipal(principal), []string{"role"}))
	return authorizer
}

// restoredLister lists tasks the way a node holds them: each with the header its namespace's detector decodes.
func restoredLister(t *testing.T, detectors map[string]distributedtask.ConflictDetector, tasks ...*distributedtask.Task) distributedtask.TaskLister {
	t.Helper()
	byNamespace := map[string][]*distributedtask.Task{}
	for _, task := range tasks {
		byNamespace[task.Namespace] = append(byNamespace[task.Namespace], task)
	}
	snapshot, err := json.Marshal(map[string]any{"tasks": byNamespace})
	require.NoError(t, err)
	m := distributedtask.NewManager(distributedtask.ManagerParameters{Logger: logrus.New(), ConflictDetectors: detectors})
	require.NoError(t, m.Restore(snapshot))
	return m
}

// collectionDecoder gives a payload naming a collection a header showing its collection and migration type.
type collectionDecoder struct{}

type collectionHeader struct {
	Name          string `json:"collection"`
	MigrationType string `json:"migrationType"`
}

func (h collectionHeader) Collection() string { return h.Name }

func (h collectionHeader) VisiblePayload() map[string]any {
	return map[string]any{"collection": h.Name, "migrationType": h.MigrationType}
}

func (collectionDecoder) CheckConflict(distributedtask.TaskHeader, []*distributedtask.Task) error {
	return nil
}

func (collectionDecoder) DecodeHeader(payload []byte) (distributedtask.TaskHeader, error) {
	var h collectionHeader
	if err := json.Unmarshal(payload, &h); err != nil || h.Name == "" {
		return nil, errors.New("payload names no collection")
	}
	return h, nil
}

func (d collectionDecoder) DecodeCollection(payload []byte) (string, error) {
	h, err := d.DecodeHeader(payload)
	if err != nil {
		return "", err
	}
	return h.Collection(), nil
}

type taskListerStub struct {
	items map[string][]*distributedtask.Task
}

func (t taskListerStub) ListDistributedTasks(ctx context.Context) (map[string][]*distributedtask.Task, error) {
	return t.items, nil
}

func listOnce(ctx context.Context, list func(context.Context) (map[string][]*distributedtask.Task, error),
) (map[string][]*distributedtask.Task, error) {
	return list(ctx)
}

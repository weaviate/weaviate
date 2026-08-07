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

// countingTaskLister records whether the handler got as far as reading the
// cluster's task list, which a status-code assertion cannot tell apart from a
// denial issued after the read.
type countingTaskLister struct {
	calls int
}

func (l *countingTaskLister) ListDistributedTasks(context.Context) (map[string][]*distributedtask.Task, error) {
	l.calls++
	return map[string][]*distributedtask.Task{}, nil
}

// GET /v1/tasks exposes every namespace's task list cluster-wide, payloads
// included. Making this handler ignore its denial leaves the rest of the
// package green, so these arms are the only pin on the check.
func TestHandler_ListTasks_Authorization(t *testing.T) {
	t.Run("a denied caller gets the denial and no task read", func(t *testing.T) {
		denied := errors.New("forbidden")
		authorizer := authorization.NewMockAuthorizer(t)
		authorizer.EXPECT().
			Authorize(mock.Anything, mock.Anything, authorization.READ, authorization.Cluster()).
			Return(denied)
		lister := &countingTaskLister{}

		tasks, err := NewHandler(authorizer, lister).ListTasks(context.Background(), &models.Principal{})

		require.Zero(t, lister.calls,
			"a denied caller made the handler read the cluster's task list")
		require.ErrorIs(t, err, denied)
		require.Nil(t, tasks)
	})

	// The allow arm is what makes the deny arm discriminate: it proves the read
	// does happen when the check passes.
	t.Run("an allowed caller reaches the task read", func(t *testing.T) {
		authorizer := authorization.NewMockAuthorizer(t)
		authorizer.EXPECT().
			Authorize(mock.Anything, mock.Anything, authorization.READ, authorization.Cluster()).
			Return(nil)
		lister := &countingTaskLister{}

		_, err := NewHandler(authorizer, lister).ListTasks(context.Background(), &models.Principal{})

		require.NoError(t, err)
		require.Equal(t, 1, lister.calls,
			"this is the observation the deny arm requires to be absent")
	})
}

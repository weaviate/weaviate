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

package reindex

import (
	"bytes"
	"context"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	dbreindex "github.com/weaviate/weaviate/adapters/repos/db/reindex"
	"github.com/weaviate/weaviate/cluster/distributedtask"
)

// staticTaskLister serves a fixed task set to the conflict pre-flight.
type staticTaskLister struct {
	tasks map[string][]*distributedtask.Task
}

func (s staticTaskLister) ListDistributedTasks(context.Context) (map[string][]*distributedtask.Task, error) {
	return s.tasks, nil
}

func (s staticTaskLister) AddDistributedTaskWithBarrier(context.Context, string, string, any, []string, bool) error {
	return nil
}

func (s staticTaskLister) AddDistributedTaskWithGroupsBarrier(context.Context, string, string, any, []distributedtask.UnitSpec, bool) error {
	return nil
}

func (s staticTaskLister) CancelDistributedTask(context.Context, string, string, uint64) error {
	return nil
}

// TestPropertyMutationConflict_RedactsForeignTaskID pins that a
// foreign-namespace task ID is never returned to the caller (only logged
// server-side) — StripErrorMessage only strips the caller's own namespace.
func TestPropertyMutationConflict_RedactsForeignTaskID(t *testing.T) {
	var logBuf bytes.Buffer
	logger := logrus.New()
	logger.SetOutput(&logBuf)

	const foreignID = "victimNamespace:SecretCollection:change-tokenization:secret:ffff"
	svc := New(Deps{Cluster: staticTaskLister{tasks: map[string][]*distributedtask.Task{
		dbreindex.ReindexNamespace: {{
			Namespace:      dbreindex.ReindexNamespace,
			TaskDescriptor: distributedtask.TaskDescriptor{ID: foreignID},
			Status:         distributedtask.TaskStatusStarted,
			Payload:        []byte("{not valid json"),
		}},
	}}}, logger)

	msg := svc.PropertyMutationConflict(context.Background(), "Movies", "title")
	require.NotEmpty(t, msg, "an undecodable in-flight task must still refuse the mutation")
	require.NotContains(t, msg, foreignID, "the foreign task ID must not leak to the caller")
	require.NotContains(t, msg, "SecretCollection", "no fragment of the foreign namespace may leak")
	require.Contains(t, msg, "Movies", "the caller's own class name is safe to echo")
	require.Contains(t, logBuf.String(), foreignID,
		"the task ID must be logged server-side so an operator can find the task")
}

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

package distributedtasks

import (
	"encoding/json"
	"fmt"
)

type Manager struct {
	// TODO: add synchronization
	runningTasks map[string]map[string]TaskHandle

	providers map[string]TaskProvider
}

func (m *Manager) AddTask(taskType, taskID string, payload []byte) error {
	// TODO: check if not already running

	provider, ok := m.providers[taskType]
	if !ok {
		return fmt.Errorf("unknown task type: %s", taskType)
	}

	handle, err := provider.StartTask(taskID, payload, func() {
		m.onTaskFinished(taskID, taskType)
	})
	if err != nil {
		return fmt.Errorf("could not start task %s: %s", taskID, err)
	}

	// TODO: do a proper task map initialization
	m.runningTasks[taskType] = map[string]TaskHandle{taskID: handle}

	return nil
}
func (m *Manager) onTaskFinished(taskType, taskID string) {
	// TODO: add an entry to the RAFT log
}

func (m *Manager) CancelTask(taskType, taskID string) error {
	// TODO: check for existance
	m.runningTasks[taskType][taskID].Cancel()
	return nil
}

type TaskFinishedCallback func()

type TaskHandle interface {
	Cancel()
}

type TaskProvider interface {
	StartTask(id string, payload []byte, finishedCallback TaskFinishedCallback) (TaskHandle, error)
}

type RevectorizationTaskPayload struct {
	CollectionName string
	TargetVector   string
}

type revectorizationTask struct {
	payload  RevectorizationTaskPayload
	cancelCh chan struct{}
}

func (t *revectorizationTask) Cancel() {
	close(t.cancelCh)
}

func (t *revectorizationTask) work() {
	// TODO: check if a bucket creates on disk, otherwise create it
	// TODO: create a cursor

	select {
	case <-t.cancelCh:
		return
	}
}

type RevectorizationTaskProvider struct {
}

func (r *RevectorizationTaskProvider) StartTask(id string, payloadBytes []byte, finishedCallback TaskFinishedCallback) (TaskHandle, error) {
	var payload RevectorizationTaskPayload
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		return nil, fmt.Errorf("could not unmarshal revectorization payload: %s", err)
	}

	// TODO: do a validation on input. Probably higher in the stack, e.g. rest handler

	var task *revectorizationTask
	go task.work()

	return task, nil
}

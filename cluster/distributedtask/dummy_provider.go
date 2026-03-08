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
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

const DummyProviderNamespace = "dummy-test"

type DummyProviderPayload struct {
	FailSubUnitID string `json:"failSubUnitId,omitempty"`
}

// DummyProvider is a test-only [SubUnitAwareProvider] used by acceptance tests to exercise
// the sub-unit lifecycle end-to-end. It is registered in configure_api.go behind the
// DISTRIBUTED_TASKS_TEST_PROVIDER_ENABLED env var and exposed via a debug HTTP endpoint
// on port 6060.
//
// On StartTask, it spawns a goroutine that iterates sub-units sequentially, reports 50%
// progress, then completes each one. Set FailSubUnitID in the payload to make one sub-unit
// fail instead, which triggers the task-level fail-fast behavior.
type DummyProvider struct {
	mu       sync.Mutex
	recorder TaskCompletionRecorder
	tasks    []TaskDescriptor
	nodeID   string
	logger   logrus.FieldLogger

	completedTasks   map[TaskDescriptor]bool
	completedTasksMu sync.Mutex

	finalizedSubUnits   map[TaskDescriptor][]string
	finalizedSubUnitsMu sync.Mutex
}

func NewDummyProvider(nodeID string, logger logrus.FieldLogger) *DummyProvider {
	return &DummyProvider{
		nodeID:            nodeID,
		logger:            logger,
		completedTasks:    make(map[TaskDescriptor]bool),
		finalizedSubUnits: make(map[TaskDescriptor][]string),
	}
}

func (p *DummyProvider) SetCompletionRecorder(recorder TaskCompletionRecorder) {
	p.recorder = recorder
}

func (p *DummyProvider) GetLocalTasks() []TaskDescriptor {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]TaskDescriptor{}, p.tasks...)
}

func (p *DummyProvider) CleanupTask(desc TaskDescriptor) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	filtered := p.tasks[:0]
	for _, d := range p.tasks {
		if d != desc {
			filtered = append(filtered, d)
		}
	}
	p.tasks = filtered
	return nil
}

func (p *DummyProvider) StartTask(task *Task) (TaskHandle, error) {
	p.mu.Lock()
	p.tasks = append(p.tasks, task.TaskDescriptor)
	p.mu.Unlock()

	handle := &dummyTaskHandle{stopCh: make(chan struct{})}

	if task.HasSubUnits() {
		enterrors.GoWrapper(func() {
			p.processSubUnits(task, handle)
		}, p.logger)
	} else {
		enterrors.GoWrapper(func() {
			p.processLegacyTask(task, handle)
		}, p.logger)
	}

	return handle, nil
}

func (p *DummyProvider) OnSubUnitsCompleted(task *Task, localSubUnitIDs []string) {
	p.finalizedSubUnitsMu.Lock()
	p.finalizedSubUnits[task.TaskDescriptor] = append(
		p.finalizedSubUnits[task.TaskDescriptor], localSubUnitIDs...,
	)
	p.finalizedSubUnitsMu.Unlock()

	// Write marker files for each finalized sub-unit
	dir := filepath.Join("/tmp/dtm-finalize", task.ID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		p.logger.WithError(err).Error("dummy provider: failed to create marker dir")
		return
	}
	for _, suID := range localSubUnitIDs {
		path := filepath.Join(dir, suID)
		if err := os.WriteFile(path, []byte(fmt.Sprintf("finalized by %s", p.nodeID)), 0o644); err != nil {
			p.logger.WithError(err).Error("dummy provider: failed to write marker file")
		}
	}

	p.logger.WithField("taskID", task.ID).WithField("localSubUnitIDs", localSubUnitIDs).
		Info("dummy provider: OnSubUnitsCompleted fired")
}

func (p *DummyProvider) GetFinalizedSubUnits(desc TaskDescriptor) []string {
	p.finalizedSubUnitsMu.Lock()
	defer p.finalizedSubUnitsMu.Unlock()
	return append([]string{}, p.finalizedSubUnits[desc]...)
}

func (p *DummyProvider) OnTaskCompleted(task *Task) {
	p.completedTasksMu.Lock()
	defer p.completedTasksMu.Unlock()
	p.completedTasks[task.TaskDescriptor] = true
	p.logger.WithField("taskID", task.ID).WithField("status", task.Status).
		Info("dummy provider: OnTaskCompleted fired")
}

func (p *DummyProvider) IsTaskCompleted(desc TaskDescriptor) bool {
	p.completedTasksMu.Lock()
	defer p.completedTasksMu.Unlock()
	return p.completedTasks[desc]
}

func (p *DummyProvider) processSubUnits(task *Task, handle *dummyTaskHandle) {
	var payload DummyProviderPayload
	if len(task.Payload) > 0 {
		_ = json.Unmarshal(task.Payload, &payload)
	}

	for suID, su := range task.SubUnits {
		select {
		case <-handle.stopCh:
			return
		default:
		}

		nodeID := su.NodeID
		if nodeID == "" {
			nodeID = p.nodeID
		}

		// Only process sub-units assigned to this node (or unassigned)
		if nodeID != p.nodeID {
			continue
		}

		ctx := context.Background()

		// Report initial progress
		_ = p.recorder.UpdateDistributedTaskSubUnitProgress(
			ctx, task.Namespace, task.ID, task.Version, p.nodeID, suID, 0.5,
		)

		time.Sleep(100 * time.Millisecond)

		if payload.FailSubUnitID == suID {
			_ = p.recorder.RecordDistributedTaskSubUnitFailure(
				ctx, task.Namespace, task.ID, task.Version, p.nodeID, suID, "dummy failure",
			)
			return // stop processing after failure
		}

		_ = p.recorder.RecordDistributedTaskSubUnitCompletion(
			ctx, task.Namespace, task.ID, task.Version, p.nodeID, suID,
		)
	}
}

func (p *DummyProvider) processLegacyTask(task *Task, handle *dummyTaskHandle) {
	select {
	case <-handle.stopCh:
		return
	case <-time.After(100 * time.Millisecond):
	}

	_ = p.recorder.RecordDistributedTaskNodeCompletion(
		context.Background(), task.Namespace, task.ID, task.Version,
	)
}

type dummyTaskHandle struct {
	once   sync.Once
	stopCh chan struct{}
}

func (h *dummyTaskHandle) Terminate() {
	h.once.Do(func() { close(h.stopCh) })
}

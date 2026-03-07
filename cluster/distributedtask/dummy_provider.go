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
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// DummyProviderNamespace is the task namespace used by [DummyProvider] for testing.
const DummyProviderNamespace = "dummy-test"

// DummyProviderPayload is the payload for a dummy task.
type DummyProviderPayload struct {
	FailSubUnitID string `json:"failSubUnitId,omitempty"`
}

// DummyProvider is a test-only provider that automatically processes sub-units
// when a task is started. It reports progress and completes each sub-unit.
// One sub-unit can be configured to fail via the task payload.
type DummyProvider struct {
	mu       sync.Mutex
	recorder TaskCompletionRecorder
	tasks    []TaskDescriptor
	nodeID   string
	logger   logrus.FieldLogger

	completedTasks   map[TaskDescriptor]bool
	completedTasksMu sync.Mutex
}

// NewDummyProvider creates a new [DummyProvider] for the given node.
func NewDummyProvider(nodeID string, logger logrus.FieldLogger) *DummyProvider {
	return &DummyProvider{
		nodeID:         nodeID,
		logger:         logger,
		completedTasks: make(map[TaskDescriptor]bool),
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

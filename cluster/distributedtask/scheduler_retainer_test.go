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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/require"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
)

// retainingTestProvider implements the optional CompletedTaskRetainer on top
// of the standard test provider, recording the namespace map each call
// received.
type retainingTestProvider struct {
	*testTaskProvider
	retain atomic.Bool

	mu       sync.Mutex
	lastSeen []TaskDescriptor
}

func (p *retainingTestProvider) ShouldRetainCompletedTask(_ *Task, namespaceTasks map[TaskDescriptor]*Task) bool {
	p.mu.Lock()
	p.lastSeen = p.lastSeen[:0]
	for desc := range namespaceTasks {
		p.lastSeen = append(p.lastSeen, desc)
	}
	p.mu.Unlock()
	return p.retain.Load()
}

// TestCompletedTaskRetainerVetoesTTLCleanup pins the retainer veto in the
// scheduler's TTL sweep: a provider that reports a completed record as still
// load-bearing (drop-vector coverage records while their marker is pending)
// keeps it past CompletedTaskTTL — the strict cleaner mock fails the test on
// any cleanup call — and the record is swept on the first tick after the
// veto lifts. Every other test provider fails the optional type assertion,
// so without this the veto branch never diverges from old behavior in CI.
func TestCompletedTaskRetainerVetoesTTLCleanup(t *testing.T) {
	defer leaktest.Check(t)()

	h := newTestHarness(t)
	rp := &retainingTestProvider{testTaskProvider: newTestTaskProvider(t, nil)}
	rp.retain.Store(true)
	h.provider = rp.testTaskProvider
	h.registeredProviders = map[string]Provider{h.tasksNamespace: rp}
	h = h.init(t)

	h.startScheduler(t)
	defer h.Close()

	var (
		taskID         = "1234"
		version uint64 = 10
	)
	err := h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             h.tasksNamespace,
		Id:                    taskID,
		Payload:               []byte("payload"),
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
		UnitIds:               []string{"su-1"},
	}), version)
	require.NoError(t, err)
	h.advanceClock(h.schedulerTickInterval)

	recvWithTimeout(t, h.provider.startedCh).Complete()
	completeUnit(t, h, h.tasksNamespace, taskID, version, h.localNodeID, "su-1")
	require.Equal(t, taskID, recvWithTimeout(t, h.provider.completedCh).ID)
	h.advanceClock(h.schedulerTickInterval)

	// A second, still-ACTIVE task joins the namespace before the sweep.
	const siblingID = "5678"
	err = h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             h.tasksNamespace,
		Id:                    siblingID,
		Payload:               []byte("payload"),
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
		UnitIds:               []string{"su-2"},
	}), version+1)
	require.NoError(t, err)

	// Well past the TTL: the veto must keep the record.
	h.advanceClock(h.completedTaskTTL + time.Minute)
	recvWithTimeout(t, h.provider.startedCh).Complete() // the sibling's execution; its record stays STARTED
	require.Len(t, h.listManagerTasks(t)[h.tasksNamespace], 2,
		"retained record must survive TTL expiry while the veto holds")

	// The retainer's contract: it receives the FULL namespace map — expired,
	// fresh, and active siblings alike. Relative judgments (drop-vector's
	// newest-matching-record anchor) are only sound against the complete set;
	// a pre-filtered map would silently break them.
	rp.mu.Lock()
	seen := append([]TaskDescriptor(nil), rp.lastSeen...)
	rp.mu.Unlock()
	require.Contains(t, seen, TaskDescriptor{ID: taskID, Version: version})
	require.Contains(t, seen, TaskDescriptor{ID: siblingID, Version: version + 1},
		"an ACTIVE sibling must be visible to the retainer")

	// Veto lifts: the next tick sweeps the expired record; the active sibling
	// is not TTL-eligible and stays.
	rp.retain.Store(false)
	h.expectCleanUpTask(t, h.tasksNamespace, taskID, version)
	h.advanceClock(h.schedulerTickInterval)
	remaining := h.listManagerTasks(t)[h.tasksNamespace]
	require.Len(t, remaining, 1)
	require.Equal(t, siblingID, remaining[0].ID)
}

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
	"encoding/json"
	"fmt"
	"testing"
	"time"

	logrustest "github.com/sirupsen/logrus/hooks/test"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
)

// Each per-tenant path must cost the tenant, not the task, so these run at
// 10k and 1M tenants with three replicas spread over nine nodes.
var benchTenantCounts = []int{10_000, 1_000_000}

const (
	benchNodes    = 9
	benchReplicas = 3
)

func benchTask(tenants int, status TaskStatus, decided bool) *Task {
	units := make(map[string]*Unit, tenants*benchReplicas)
	task := &Task{
		Namespace: "ns", TaskDescriptor: TaskDescriptor{ID: "T", Version: 1},
		Status: status, NeedsPreparationBarrier: true,
	}
	if decided {
		task.SwapDecidedGroups = make(map[string]time.Time, tenants)
	}
	for i := range tenants {
		group := fmt.Sprintf("tenant-%d", i)
		for r := range benchReplicas {
			id := fmt.Sprintf("%s-%d", group, r)
			units[id] = &Unit{ID: id, GroupID: group, NodeID: fmt.Sprintf("node-%d", (i+r)%benchNodes), Status: UnitStatusCompleted}
		}
		if decided {
			task.SwapDecidedGroups[group] = time.Time{}
		}
	}
	task.SetUnits(units)
	return task
}

func benchSizes(b *testing.B, run func(b *testing.B, tenants int)) {
	for _, tenants := range benchTenantCounts {
		b.Run(fmt.Sprintf("tenants=%d", tenants), func(b *testing.B) {
			b.ReportAllocs()
			run(b, tenants)
		})
	}
}

func BenchmarkPrepAckApply(b *testing.B) {
	benchSizes(b, func(b *testing.B, tenants int) {
		m := NewManager(ManagerParameters{})
		reset := func() { m.tasks["ns"] = map[string]*Task{"T": benchTask(tenants, TaskStatusPreparing, false)} }
		reset()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ack := i % (tenants * benchReplicas)
			if ack == 0 && i > 0 {
				b.StopTimer()
				reset()
				b.StartTimer()
			}
			tenant, replica := ack%tenants, ack/tenants
			raw, _ := json.Marshal(&cmd.RecordDistributedTaskPreparationCompleteAckRequest{
				Namespace: "ns", Id: "T", Version: 1, GroupId: fmt.Sprintf("tenant-%d", tenant),
				NodeId: fmt.Sprintf("node-%d", (tenant+replica)%benchNodes), Success: true,
			})
			if err := m.RecordPreparationCompleteAck(&cmd.ApplyRequest{SubCommand: raw}); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func benchScheduler() *Scheduler {
	logger, _ := logrustest.NewNullLogger()
	return NewScheduler(SchedulerParams{Logger: logger, LocalNode: "node-0", TickInterval: time.Second})
}

// A tick after the first, once every decided group has swapped or has
// nothing on this node.
func BenchmarkSwapPhaseTick(b *testing.B) {
	benchSizes(b, func(b *testing.B, tenants int) {
		s, task := benchScheduler(), benchTask(tenants, TaskStatusCancelled, true)
		provider := newBarrierRecordingProvider(nil)
		s.runSwapPhase("ns", task.TaskDescriptor, task, provider)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			s.runSwapPhase("ns", task.TaskDescriptor, task, provider)
		}
	})
}

func BenchmarkPostCompletionGate(b *testing.B) {
	benchSizes(b, func(b *testing.B, tenants int) {
		s, task := benchScheduler(), benchTask(tenants, TaskStatusSwapping, true)
		state := s.perTaskStateLockedOrInit(task.TaskDescriptor)
		state.postCompletionGroupErrors = map[string]error{}
		for _, group := range task.Groups() {
			state.postCompletionGroupErrors[group] = nil
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if !s.allLocalGroupsReturnedLocked(task, task.TaskDescriptor) {
				b.Fatal("every local group returned")
			}
		}
	})
}

func BenchmarkStatusEntry(b *testing.B) {
	benchSizes(b, func(b *testing.B, tenants int) {
		task := benchTask(tenants, TaskStatusCancelled, true)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			task.StatusEntryFor("tenant-0")
		}
	})
}

// The scheduler's per-tick copy of every task.
func BenchmarkTaskClone(b *testing.B) {
	benchSizes(b, func(b *testing.B, tenants int) {
		task := benchTask(tenants, TaskStatusStarted, false)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			task.Clone()
		}
	})
}

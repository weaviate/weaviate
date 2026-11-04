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

var (
	allDecided  = func(int) bool { return true }
	noneDecided = func(int) bool { return false }
	halfDecided = func(i int) bool { return i%2 == 0 }
)

func benchTask(tenants int, status TaskStatus, decided func(tenant int) bool, unitStatus UnitStatus) *Task {
	units := make(map[string]*Unit, tenants*benchReplicas)
	task := &Task{
		Namespace: "ns", TaskDescriptor: TaskDescriptor{ID: "T", Version: 1},
		Status: status, NeedsPreparationBarrier: true, SwapDecidedGroups: map[string]time.Time{},
	}
	for i := range tenants {
		group := fmt.Sprintf("tenant-%d", i)
		for r := range benchReplicas {
			id := fmt.Sprintf("%s-%d", group, r)
			units[id] = &Unit{ID: id, GroupID: group, NodeID: fmt.Sprintf("node-%d", (i+r)%benchNodes), Status: unitStatus}
		}
		if decided(i) {
			task.SwapDecidedGroups[group] = time.Time{}
		}
	}
	task.SetUnits(units)
	return task
}

// withReindexPayload gives task a payload the size of a reindex run's: every
// tenant, and two maps with one entry per unit.
func withReindexPayload(task *Task) *Task {
	payload := struct {
		Tenants     []string          `json:"tenants"`
		UnitToNode  map[string]string `json:"unitToNode"`
		UnitToShard map[string]string `json:"unitToShard"`
	}{UnitToNode: map[string]string{}, UnitToShard: map[string]string{}}
	payload.Tenants = append(payload.Tenants, task.Groups()...)
	for id, u := range task.Units() {
		payload.UnitToNode[id], payload.UnitToShard[id] = u.NodeID, u.GroupID
	}
	task.Payload, _ = json.Marshal(payload)
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
		reset := func() {
			m.tasks["ns"] = map[string]*Task{"T": benchTask(tenants, TaskStatusPreparing, noneDecided, UnitStatusCompleted)}
		}
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

// A tick after the first, for a task whose tenants are settled or still running.
func BenchmarkSwapPhaseTick(b *testing.B) {
	for name, task := range map[string]func(tenants int) *Task{
		"settled":                 func(n int) *Task { return benchTask(n, TaskStatusCancelled, allDecided, UnitStatusCompleted) },
		"cancelled-half-decided":  func(n int) *Task { return benchTask(n, TaskStatusCancelled, halfDecided, UnitStatusCompleted) },
		"running-nothing-decided": func(n int) *Task { return benchTask(n, TaskStatusStarted, noneDecided, UnitStatusInProgress) },
	} {
		b.Run(name, func(b *testing.B) {
			benchSizes(b, func(b *testing.B, tenants int) {
				s, task := benchScheduler(), task(tenants)
				provider := newBarrierRecordingProvider(nil)
				s.runDecidedSwapPhase("ns", task.TaskDescriptor, task, provider)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					s.runDecidedSwapPhase("ns", task.TaskDescriptor, task, provider)
				}
			})
		})
	}
}

// A tick after the first, while tenants still run and once all are done.
func BenchmarkPrepPhaseTick(b *testing.B) {
	for name, task := range map[string]func(tenants int) *Task{
		"settled": func(n int) *Task { return benchTask(n, TaskStatusPreparing, noneDecided, UnitStatusCompleted) },
		"running": func(n int) *Task { return benchTask(n, TaskStatusStarted, noneDecided, UnitStatusInProgress) },
	} {
		b.Run(name, func(b *testing.B) {
			benchSizes(b, func(b *testing.B, tenants int) {
				s, task := benchScheduler(), task(tenants)
				provider := newBarrierRecordingProvider(nil)
				s.runPreparationPhase("ns", task.TaskDescriptor, task, provider)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					s.runPreparationPhase("ns", task.TaskDescriptor, task, provider)
				}
			})
		})
	}
}

// The leader's encode of the task list every node fetches per tick.
func BenchmarkTaskListEncode(b *testing.B) {
	benchSizes(b, func(b *testing.B, tenants int) {
		m := NewManager(ManagerParameters{})
		m.tasks["ns"] = map[string]*Task{"T": withReindexPayload(benchTask(tenants, TaskStatusFinished, allDecided, UnitStatusCompleted))}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := m.ListDistributedTasksPayload(b.Context()); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// A node's decode of that list, on a node that already holds the run.
func BenchmarkTaskListDecode(b *testing.B) {
	benchSizes(b, func(b *testing.B, tenants int) {
		m := NewManager(ManagerParameters{})
		m.tasks["ns"] = map[string]*Task{"T": withReindexPayload(benchTask(tenants, TaskStatusFinished, allDecided, UnitStatusCompleted))}
		raw, err := m.ListDistributedTasksPayload(b.Context())
		if err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := m.DecodeTaskList(raw); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// One tenant delete on a retained finished run, inside the RAFT apply.
func BenchmarkTenantDelete(b *testing.B) {
	benchSizes(b, func(b *testing.B, tenants int) {
		task := benchTask(tenants, TaskStatusFinished, allDecided, UnitStatusCompleted)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			task.forgetGroups([]string{fmt.Sprintf("tenant-%d", i%tenants)})
		}
	})
}

func BenchmarkPostCompletionGate(b *testing.B) {
	benchSizes(b, func(b *testing.B, tenants int) {
		s, task := benchScheduler(), benchTask(tenants, TaskStatusSwapping, allDecided, UnitStatusCompleted)
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
		task := benchTask(tenants, TaskStatusCancelled, allDecided, UnitStatusCompleted)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			task.StatusEntryFor("tenant-0")
		}
	})
}

// The scheduler's per-tick copy of every task.
func BenchmarkTaskClone(b *testing.B) {
	benchSizes(b, func(b *testing.B, tenants int) {
		task := benchTask(tenants, TaskStatusStarted, noneDecided, UnitStatusCompleted)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			task.Clone()
		}
	})
}


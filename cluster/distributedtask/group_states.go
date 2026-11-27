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
	"cmp"
	"slices"
	"strings"
	"time"

	"github.com/weaviate/weaviate/usecases/sharding"
)

// TaskRules shapes a namespace's tasks inside the applies. Its methods must be
// pure functions of their arguments, so every replica reaches the same result.
type TaskRules interface {
	// Admit may rewrite the task before AddTask stores it, and names the ended task it takes over.
	Admit(task *Task, existing []*Task, live map[string]map[string]uint64) (takesOver *Task, err error)
	Ended(task *Task, existing []*Task) EndRuling
	BornSwitched(task *Task) bool
	// Units lists the units of a task submitted without any, from the sharding state at the task's log index,
	// in an order that never follows a map, so every replica stores the same task.
	Units(header TaskHeader, state *sharding.State) ([]UnitSpec, error)
}

// SwitchedByView is a header whose decisions every replica serves from its own
// view, so a decision owes no swap.
type SwitchedByView interface {
	SwitchedByView() bool
}

func switchedByView(h TaskHeader) bool {
	v, ok := h.(SwitchedByView)
	return ok && v.SwitchedByView()
}

type EndRuling struct {
	Compact bool
	Forget  map[*Task][]string
	Remove  []*Task
}

// TakeOver costs one pass over the units.
func (t *Task) TakeOver(kept *Task) {
	for group, at := range kept.SwapDecidedGroups {
		if t.SwapDecidedGroups == nil {
			t.SwapDecidedGroups = make(map[string]time.Time, len(kept.SwapDecidedGroups))
		}
		t.SwapDecidedGroups[group] = at
		for _, id := range t.groups[group] {
			delete(t.Units, id)
		}
	}
	t.IndexGroups()
}

func (t *Task) SetHeader(payload []byte, header TaskHeader) {
	t.Payload = payload
	t.header = header
}

func byVersion(a, b *Task) int { return cmp.Compare(a.Version, b.Version) }

// applyEndRulesWithLock runs once per apply that can end a task, before the lock is released.
func (m *Manager) applyEndRulesWithLock() {
	for namespace := range m.taskRules {
		var ended []*Task
		for _, t := range m.tasks[namespace] {
			if t.Status.IsTerminal() && !t.ended {
				ended = append(ended, t)
			}
		}
		slices.SortFunc(ended, byVersion)
		for _, t := range ended {
			t.ended = true
			if m.tasks[namespace][t.ID] == t {
				m.endTaskWithLock(t)
			}
		}
	}
}

func (m *Manager) endTaskWithLock(task *Task) {
	ruling := m.taskRules[task.Namespace].Ended(task, m.namespaceTasksWithLock(task.Namespace))
	for other, groups := range ruling.Forget {
		change := m.markWithLock(other.Namespace, other.ID)
		for _, g := range groups {
			delete(other.SwapDecidedGroups, g)
			change.group(g)
		}
		m.notifyStateWithLock(other, groups)
	}
	removed := false
	for _, t := range ruling.Remove {
		delete(m.tasks[t.Namespace], t.ID)
		m.markWithLock(t.Namespace, t.ID).addedOrRemoved = true
		m.notifyStateWithLock(t, nil)
		removed = removed || t == task
	}
	if removed {
		return
	}
	if ruling.Compact {
		task.compactInPlace()
		m.markWithLock(task.Namespace, task.ID).addedOrRemoved = true
	}
	m.notifyStateWithLock(task, nil)
}

// MarkTenantsBornSwitched switches new tenants in running tasks whose rules say they hold nothing to build.
func (m *Manager) MarkTenantsBornSwitched(collection string, tenants []string) {
	defer m.fireStates()
	m.mu.Lock()
	defer m.mu.Unlock()
	for namespace, rules := range m.taskRules {
		for _, task := range m.tasks[namespace] {
			if task.Status.IsTerminal() || !strings.EqualFold(task.collection(), collection) || !rules.BornSwitched(task) {
				continue
			}
			if task.SwapDecidedGroups == nil {
				task.SwapDecidedGroups = make(map[string]time.Time, len(tenants))
			}
			change := m.markWithLock(namespace, task.ID)
			for _, tenant := range tenants {
				task.SwapDecidedGroups[tenant] = task.StartedAt
				change.group(tenant)
			}
			m.notifyStateWithLock(task, tenants)
		}
	}
}

// StateListener hears every change to what a task makes shards serve. Nil
// groups means every shard of the collection. It runs after the apply
// released the Manager's lock.
type StateListener func(namespace string, header TaskHeader, groups []string)

func (m *Manager) SetStateListener(l StateListener) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stateListener = l
}

type stateNote struct {
	namespace string
	header    TaskHeader
	groups    []string
}

func (m *Manager) notifyStateWithLock(task *Task, groups []string) {
	if m.stateListener != nil && task.header != nil {
		m.pendingStates = append(m.pendingStates, stateNote{task.Namespace, task.header, groups})
	}
}

func (m *Manager) fireStates() {
	m.mu.Lock()
	notes, listener := m.pendingStates, m.stateListener
	m.pendingStates = nil
	m.mu.Unlock()
	for _, n := range notes {
		listener(n.namespace, n.header, n.groups)
	}
}

// ShardTaskState is one task as one shard replica sees it.
type ShardTaskState struct {
	TaskDescriptor
	Status                              TaskStatus
	Header                              TaskHeader
	Switched, HasUnit, LocalUnitStarted bool
}

// ShardTaskStates decodes no payload and copies no unit map.
func (m *Manager) ShardTaskStates(namespace, collection, shard, node string) []ShardTaskState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []ShardTaskState
	for _, task := range m.namespaceTasksWithLock(namespace) {
		if !strings.EqualFold(task.collection(), collection) {
			continue
		}
		group := shard
		if task.hasGroup("") || task.GroupSwapDecided("") {
			group = ""
		}
		st := ShardTaskState{
			TaskDescriptor: task.TaskDescriptor, Status: task.Status, Header: task.header,
			Switched: task.GroupSwapDecided(group),
		}
		for _, id := range task.groups[group] {
			u := task.Units[id]
			if u == nil || u.Shard != shard {
				continue
			}
			st.HasUnit = true
			st.LocalUnitStarted = st.LocalUnitStarted || (u.NodeID == node && u.Status != UnitStatusPending)
		}
		out = append(out, st)
	}
	return out
}

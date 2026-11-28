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
	"strings"

	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

// compactInPlace keeps an ended task's switched groups and replaces its units
// with a summary, so a kept task costs memory per switched tenant, not per unit.
func (t *Task) compactInPlace() {
	t.Summary = t.summarizeUnits()
	t.Units = nil
	t.groups, t.groupCount, t.decidedWithUnits = nil, 0, 0
	t.unitCounts = nil
	t.PreparedGroups = nil
	t.NodePreparationAcks = nil
	t.PostCompletionAcks = nil
}

// CollectionRenamer lets a restore rename the collection a HeaderDecoder's payload names.
type CollectionRenamer interface {
	RenameCollection(payload []byte, collection string) ([]byte, error)
}

const restoredRunningError = "restored from a backup taken while the task ran"

// RestoreCollectionTasks installs class's captured tasks that have a generation, as ended tasks.
// It drops switched tenants hasTenant rejects, and keeps a held task of a higher version.
func (m *Manager) RestoreCollectionTasks(raw []byte, class string, hasTenant func(string) bool) error {
	if len(raw) == 0 {
		return nil
	}
	var tasks []*Task
	if err := json.Unmarshal(raw, &tasks); err != nil {
		return fmt.Errorf("decode the reindex tasks restored with class %s: %w", class, err)
	}

	defer m.fireStates()
	m.mu.Lock()
	defer m.mu.Unlock()
	defer m.applyEndRulesWithLock()

	for _, task := range tasks {
		header := m.headerFor(task.Namespace, task.Payload)
		if !switchedByView(header) || !capturedFor(header.Collection(), class) {
			continue
		}
		if held := m.tasks[task.Namespace][task.ID]; held != nil && held.Version > task.Version {
			continue
		}
		if err := m.renameCollectionWithLock(task, class); err != nil {
			return err
		}
		if !task.Status.IsTerminal() {
			task.markTerminal(TaskStatusFailed, task.StartedAt)
			task.Error = strings.TrimPrefix(task.Error+"; "+restoredRunningError, "; ")
		}
		for group := range task.SwapDecidedGroups {
			if group != "" && !hasTenant(group) {
				delete(task.SwapDecidedGroups, group)
			}
		}
		task.IndexGroups()
		m.setTaskWithLock(task)
		m.markWithLock(task.Namespace, task.ID).addedOrRemoved = true
		m.notifyStateWithLock(task, nil)
	}
	return nil
}

func capturedFor(collection, class string) bool {
	return strings.EqualFold(collection, class) || strings.EqualFold(namespacing.StripQualification(collection), class)
}

func (m *Manager) renameCollectionWithLock(task *Task, class string) error {
	renamer, ok := m.headerDecoders[task.Namespace].(CollectionRenamer)
	if !ok {
		return fmt.Errorf("namespace %s cannot restore task %s under class %s", task.Namespace, task.ID, class)
	}
	payload, err := renamer.RenameCollection(task.Payload, class)
	if err != nil {
		return fmt.Errorf("restore task %s/%s under class %s: %w", task.Namespace, task.ID, class, err)
	}
	task.Payload, task.header = payload, m.headerFor(task.Namespace, payload)
	return nil
}

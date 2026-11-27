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

import "time"

// TaskRules shapes a namespace's tasks inside the applies. Its methods must be
// pure functions of their arguments, so every replica reaches the same result.
type TaskRules interface {
	// Admit may rewrite the task before AddTask stores it, and names the ended task it takes over.
	Admit(task *Task, existing []*Task, live map[string]map[string]uint64) (takesOver *Task, err error)
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

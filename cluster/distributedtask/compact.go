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

type UnitSummary struct {
	Groups int                `json:"groups"`
	Counts map[UnitStatus]int `json:"counts"`
	Errors []string           `json:"errors,omitempty"`
}

// CompactEndedTasks returns the ended tasks a restore can serve from. Only a task with a generation
// serves, and no task has one until reindex serves from generations, which fills this in.
func (m *Manager) CompactEndedTasks(namespace, collection string) ([]*Task, error) {
	return nil, nil
}

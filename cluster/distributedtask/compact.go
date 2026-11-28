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

// LocalEndedTasks returns no task: no Task records the index generation a restore needs.
func (m *Manager) LocalEndedTasks(namespace, collection string) ([]*Task, error) {
	return nil, nil
}

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

package models

import "github.com/go-openapi/strfmt"

// DistributedTaskSubUnit holds the execution state of a single sub-unit within a
// distributed task. Sub-unit tracking is enabled at task creation time.
type DistributedTaskSubUnit struct {
	// The identifier of this sub-unit within the task.
	ID string `json:"id"`

	// The lifecycle status of this sub-unit: PENDING, IN_PROGRESS, COMPLETED, or FAILED.
	Status string `json:"status"`

	// Fractional completion progress in [0.0, 1.0].
	Progress float64 `json:"progress,omitempty"`

	// The ID of the node that last reported on this sub-unit.
	NodeID string `json:"nodeID,omitempty"`

	// Failure reason when Status == FAILED.
	Error string `json:"error,omitempty"`

	// The time of the last state or progress update.
	// Format: date-time
	UpdatedAt strfmt.DateTime `json:"updatedAt,omitempty"`
}

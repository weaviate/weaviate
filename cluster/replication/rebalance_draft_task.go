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

package replication

// DraftUnitStatus is the status of one listed node's draft unit.
type DraftUnitStatus string

const (
	DraftUnitPending    DraftUnitStatus = "PENDING"
	DraftUnitInProgress DraftUnitStatus = "IN_PROGRESS"
	DraftUnitCompleted  DraftUnitStatus = "COMPLETED"
	DraftUnitFailed     DraftUnitStatus = "FAILED"
)

// DraftTask is a rebalance draft task as one node's FSM holds it. The task ID
// is the plan ID.
type DraftTask struct {
	PlanID   string
	Version  uint64
	Terminal bool
	Error    string
	// Units holds each listed node's unit status, keyed by node name.
	Units map[string]DraftUnitStatus
}

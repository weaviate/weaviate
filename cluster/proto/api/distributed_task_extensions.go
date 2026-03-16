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

package api

// Additional ApplyRequest_Type constants for distributed task sub-unit operations.
// These extend the generated protobuf enum.
const (
	ApplyRequest_TYPE_DISTRIBUTED_TASK_RECORD_SUB_UNIT_COMPLETED ApplyRequest_Type = 304
	ApplyRequest_TYPE_DISTRIBUTED_TASK_RECORD_SUB_UNIT_PROGRESS  ApplyRequest_Type = 305
)

// AddDistributedTaskRequestWithSubUnits extends AddDistributedTaskRequest with optional
// sub-unit IDs for sub-unit tracking mode.
// It is JSON-serialized as the SubCommand of ApplyRequest_TYPE_DISTRIBUTED_TASK_ADD.
// JSON field names match the protobuf JSON encoding of AddDistributedTaskRequest for backward
// compatibility — existing commands without sub_unit_ids decode with SubUnitIds == nil.
type AddDistributedTaskRequestWithSubUnits struct {
	Namespace             string   `json:"namespace,omitempty"`
	Id                    string   `json:"id,omitempty"`
	Payload               []byte   `json:"payload,omitempty"`
	SubmittedAtUnixMillis int64    `json:"submitted_at_unix_millis,omitempty"`
	SubUnitIds            []string `json:"sub_unit_ids,omitempty"`
}

// RecordDistributedTaskSubUnitCompletedRequest is the sub-command for recording a sub-unit
// completion (success or failure). An absent Error field means success.
type RecordDistributedTaskSubUnitCompletedRequest struct {
	Namespace            string  `json:"namespace,omitempty"`
	Id                   string  `json:"id,omitempty"`
	Version              uint64  `json:"version,omitempty"`
	SubUnitId            string  `json:"sub_unit_id,omitempty"`
	NodeId               string  `json:"node_id,omitempty"`
	Error                *string `json:"error,omitempty"`
	FinishedAtUnixMillis int64   `json:"finished_at_unix_millis,omitempty"`
}

// RecordDistributedTaskSubUnitProgressRequest is the sub-command for recording a sub-unit
// progress fraction. Updates may be throttled by the Manager.
type RecordDistributedTaskSubUnitProgressRequest struct {
	Namespace string  `json:"namespace,omitempty"`
	Id        string  `json:"id,omitempty"`
	Version   uint64  `json:"version,omitempty"`
	SubUnitId string  `json:"sub_unit_id,omitempty"`
	NodeId    string  `json:"node_id,omitempty"`
	Progress  float64 `json:"progress,omitempty"`
}

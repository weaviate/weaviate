//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package schema

import "time"

type taskStatus string

const (
	taskStatusStarted   taskStatus = "STARTED"
	taskStatusFinished  taskStatus = "FINISHED"
	taskStatusCancelled taskStatus = "CANCELLED"
)

type distributedTask struct {
	TaskType    string `json:"taskType"`
	TaskID      string `json:"taskID"`
	TaskPayload []byte `json:"taskPayload"`

	Status     taskStatus `json:"status"`
	StartedAt  time.Time  `json:"startedAt"`
	FinishedAt time.Time  `json:"finishedAt,omitempty"`

	FinishedNodes map[string]struct{} `json:"finishedNodes"`
}

// TODO: task should have a version number, so that once we start to clean up it, we don't accidentally delete a new task.
//	Or a task can be cleaned up only if it is in the right status and TTL has passed.
// TODO: scenario when a task is completed and then resubmitted again before the clean up. We should allow to resubmit,
// 	only if the current task is finished. Otherwise, we should return an error.

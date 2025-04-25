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

package revectorization

import (
	"fmt"
	"time"

	"github.com/weaviate/weaviate/cluster/distributedtask"
)

const (
	DistributedTasksNamespace = "revectorization"
)

type TaskPayload struct {
	CollectionName       string `json:"collectionName"`
	TargetVector         string `json:"targetVector,omitempty"`
	TenantFilter         string `json:"tenantFilter,omitempty"`
	MaximumErrorsPerNode int    `json:"maximumErrorsPerNode"`
}

type processedShard struct {
	Name       string       `json:"name"`
	FinishedAt time.Time    `json:"finishedAt"`
	Stats      persistStats `json:"stats"`
}

type ObjectError struct {
	ObjectUUID string `json:"objectUUID"`
	Err        string `json:"err"`
}

func (e ObjectError) Error() string {
	return fmt.Sprintf("Object UUID: %s, Err: %s", e.ObjectUUID, e.Err)
}

type LocalTaskState struct {
	distributedtask.TaskDescriptor `json:",inline"`

	Stats           persistStats     `json:"stats"`
	StartedAt       time.Time        `json:"startedAt,omitempty"`
	ObjectErrors    []ObjectError    `json:"objectErrors"`
	ProcessedShards []processedShard `json:"processedShards"`
}

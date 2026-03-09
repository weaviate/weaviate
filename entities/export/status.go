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

package export

// Status represents the state of a node or export-level operation.
type Status string

const (
	Started      Status = "STARTED"
	Transferring Status = "TRANSFERRING"
	Success      Status = "SUCCESS"
	Failed       Status = "FAILED"
	Cancelled    Status = "CANCELLED"
)

// ShardStatus represents the state of a single shard's export.
// It extends Status with Skipped, which only applies at the shard level.
type ShardStatus string

const (
	ShardTransferring ShardStatus = "TRANSFERRING"
	ShardSuccess      ShardStatus = "SUCCESS"
	ShardFailed       ShardStatus = "FAILED"
	ShardSkipped      ShardStatus = "SKIPPED"
)

type CreateMeta struct {
	Path   string
	Status Status
}

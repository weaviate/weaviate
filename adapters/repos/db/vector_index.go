//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package db

import (
	"context"

	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/entities/schema"
)

// VectorIndex is anything that indexes vectors efficiently. For an example
// look at ./vector/hsnw/index.go
type VectorIndex interface {
	Dump(labels ...string)
	Add(id uint64, vector []float32) error
	Delete(id uint64) error
	SearchByVector(vector []float32, k int, allow helpers.AllowList) ([]uint64, []float32, error)
	SearchByVectorDistance(vector []float32, dist float32,
		maxLimit int64, allow helpers.AllowList) ([]uint64, []float32, error)
	UpdateUserConfig(updated schema.VectorIndexConfig) error
	Drop(ctx context.Context) error
	Shutdown(ctx context.Context) error
	Flush() error
	PauseMaintenance(ctx context.Context) error
	SwitchCommitLogs(ctx context.Context) error
	ListFiles(ctx context.Context) ([]string, error)
	ResumeMaintenance(ctx context.Context) error
}

// File: adapters/repos/db/sorter/query_planner.go
//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package sorter

import (
	"context"
	"fmt"
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config/runtime"
)

const (
	// Will involve some random disk access
	FixedCostIndexSeek = 200

	FixedCostRowRead = 100

	FixedCostJSONUnmarshal = 500

	// FixedCostObjectsBucketRow is roughly made up of the cost of identifying
	// the correct key in the segment index (which is multiple IOPS), then
	// reading the whole row, which contains the full object (including the
	// vector). We are applying a 10x read penalty compared to inverted index
	// rows which tend to be fairly tiny. Last but not least
	FixedCostObjectsBucketRow = 1000
)

type QueryResponse struct {
	Data       interface{}            `json:"data"`
	Extensions map[string]interface{} `json:"extensions,omitempty"`
}

type QueryPlanner struct {
	index           *Index
	classSearcher   ClassSearcher
	vectorIndex     VectorIndexSearcher
	schema          schema.Schema
	config          runtime.NodeConfig
	secondaryLookup *SecondaryLookup
}

func NewQueryPlanner(index *Index, classSearcher ClassSearcher,
	vectorIndex VectorIndexSearcher, schema schema.Schema,
	config runtime.NodeConfig, secondaryLookup *SecondaryLookup,
) *QueryPlanner {
	return &QueryPlanner{
		index:           index,
		classSearcher:   classSearcher,
		vectorIndex:     vectorIndex,
		schema:          schema,
		config:          config,
		secondaryLookup: secondaryLookup,
	}
}
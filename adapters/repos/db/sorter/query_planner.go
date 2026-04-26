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
	Data       interface{}              `json:"data"`
	Extensions map[string]interface{}   `json:"extensions,omitempty"`
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

func (qp *QueryPlanner) Build(query *Query) (*QueryResponse, error) {
	startTime := time.Now()

	// execute the actual search
	results, err := qp.classSearcher.Search(context.Background(), query.Filters, query.SearchVector, query.Limit)
	if err != nil {
		return nil, fmt.Errorf("class searcher: %w", err)
	}

	// convert results to response format
	data := convertResultsToResponse(results)

	// build extensions
	extensions := make(map[string]interface{})

	// include query vector if requested
	if query.IncludeQueryVector {
		extensions["query_vector"] = query.SearchVector
	}

	// include other metadata
	extensions["response_time_ms"] = time.Since(startTime).Milliseconds()

	return &QueryResponse{
		Data:       data,
		Extensions: extensions,
	}, nil
}

func convertResultsToResponse(results []*Result) interface{} {
	// convert results to the expected response format
	// this is a simplified placeholder
	return results
}
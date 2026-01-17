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

package filtersampling

import "github.com/weaviate/weaviate/entities/schema"

// Params represents the parameters for a filter sampling request
type Params struct {
	ClassName    schema.ClassName
	PropertyName string
	SampleCount  int
	Tenant       string
}

// Sample represents a single sampled value from the index with its cardinality
type Sample struct {
	Value       any // string, int64, float64, bool, or time.Time
	Cardinality uint64
}

// Result represents the result of a filter sampling operation
type Result struct {
	Samples                 []Sample
	TotalObjects            uint64
	EstimatedPercentCovered float64
}

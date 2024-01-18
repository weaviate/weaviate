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

// Package descriptions provides the descriptions as used by the graphql endpoint for Weaviate
package descriptions

// AGGREGATE
const (
	AggregateProperty = "Aggregate this property"
	AggregateObjects  = "Aggregate Objects on a local Weaviate"
)

const GroupBy = "Specify which properties to group by"

const (
	AggregatePropertyObject = "An object containing Aggregation information about this property"
)

const AggregateObjectsObj = "An object allowing Aggregation of %ss on a local Weaviate"

const (
	AggregateMean      = "Aggregate on the mean of numeric property values"
	AggregateSum       = "Aggregate on the sum of numeric property values"
	AggregateMedian    = "Aggregate on the median of numeric property values"
	AggregateMode      = "Aggregate on the mode of numeric property values"
	AggregateMin       = "Aggregate on the minimum of numeric property values"
	AggregateMax       = "Aggregate on the maximum of numeric property values"
	AggregateCount     = "Aggregate on the total amount of found property values"
	AggregateGroupedBy = "Indicates the group of returned data"
)

const AggregateNumericObj = "An object containing the %s of numeric properties"

const AggregateCountObj = "An object containing countable properties"

const AggregateGroupedByObj = "An object containing the path and value of the grouped property"

const (
	AggregateGroupedByGroupedByPath  = "The path of the grouped property"
	AggregateGroupedByGroupedByValue = "The value of the grouped property"
)

// NETWORK
const NetworkAggregateWeaviateObj = "An object containing Get Objects fields for network Weaviate instance: "

const NetworkAggregate = "Perform Aggregation of Objects"

const (
	NetworkAggregateObj            = "An object allowing Aggregation of Objects"
	NetworkAggregatePropertyObject = "An object containing Aggregation information about this property"
)

const NetworkAggregateThingsActionsObj = "An object allowing Aggregation of %ss on a network Weaviate"

const (
	NetworkAggregateMean      = "Aggregate on the mean of numeric property values"
	NetworkAggregateSum       = "Aggregate on the sum of numeric property values"
	NetworkAggregateMedian    = "Aggregate on the median of numeric property values"
	NetworkAggregateMode      = "Aggregate on the mode of numeric property values"
	NetworkAggregateMin       = "Aggregate on the minimum of numeric property values"
	NetworkAggregateMax       = "Aggregate on the maximum of numeric property values"
	NetworkAggregateCount     = "Aggregate on the total amount of found property values"
	NetworkAggregateGroupedBy = "Indicates the group of returned data"
)

const NetworkAggregateNumericObj = "An object containing the %s of numeric properties"

const NetworkAggregateCountObj = "An object containing countable properties"

const NetworkAggregateGroupedByObj = "An object containing the path and value of the grouped property"

const (
	NetworkAggregateGroupedByGroupedByPath  = "The path of the grouped property"
	NetworkAggregateGroupedByGroupedByValue = "The value of the grouped property"
)

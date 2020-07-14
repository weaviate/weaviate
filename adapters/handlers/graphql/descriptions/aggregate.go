//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

// Package descriptions provides the descriptions as used by the graphql endpoint for Weaviate
package descriptions

// AGGREGATE
const AggregateProperty = "Aggregate this property"
const AggregateThings = "Aggregate Things on a local Weaviate"
const AggregateActions = "Aggregate Things on a local Weaviate"

const GroupBy = "Specify which properties to group by"

const AggregateObj = "An object allowing Aggregation of Things and Actions"
const AggregatePropertyObject = "An object containing Aggregation information about this property"

const AggregateThingsActionsObj = "An object allowing Aggregation of %ss on a local Weaviate"

const AggregateMean = "Aggregate on the mean of numeric property values"
const AggregateSum = "Aggregate on the sum of numeric property values"
const AggregateMedian = "Aggregate on the median of numeric property values"
const AggregateMode = "Aggregate on the mode of numeric property values"
const AggregateMin = "Aggregate on the minimum of numeric property values"
const AggregateMax = "Aggregate on the maximum of numeric property values"
const AggregateCount = "Aggregate on the total amount of found property values"
const AggregateGroupedBy = "Indicates the group of returned data"

const AggregateNumericObj = "An object containing the %s of numeric properties"

const AggregateCountObj = "An object containing countable properties"

const AggregateGroupedByObj = "An object containing the path and value of the grouped property"

const AggregateGroupedByGroupedByPath = "The path of the grouped property"
const AggregateGroupedByGroupedByValue = "The value of the grouped property"

// NETWORK
const NetworkAggregateWeaviateObj = "An object containing Get Things and Actions fields for network Weaviate instance: "

const NetworkAggregate = "Perform Aggregation of Things and Actions"

const NetworkAggregateThings = "Aggregate Things on a network Weaviate"
const NetworkAggregateActions = "Aggregate Things on a network Weaviate"

const NetworkAggregateObj = "An object allowing Aggregation of Things and Actions"
const NetworkAggregatePropertyObject = "An object containing Aggregation information about this property"

const NetworkAggregateThingsActionsObj = "An object allowing Aggregation of %ss on a network Weaviate"

const NetworkAggregateMean = "Aggregate on the mean of numeric property values"
const NetworkAggregateSum = "Aggregate on the sum of numeric property values"
const NetworkAggregateMedian = "Aggregate on the median of numeric property values"
const NetworkAggregateMode = "Aggregate on the mode of numeric property values"
const NetworkAggregateMin = "Aggregate on the minimum of numeric property values"
const NetworkAggregateMax = "Aggregate on the maximum of numeric property values"
const NetworkAggregateCount = "Aggregate on the total amount of found property values"
const NetworkAggregateGroupedBy = "Indicates the group of returned data"

const NetworkAggregateNumericObj = "An object containing the %s of numeric properties"

const NetworkAggregateCountObj = "An object containing countable properties"

const NetworkAggregateGroupedByObj = "An object containing the path and value of the grouped property"

const NetworkAggregateGroupedByGroupedByPath = "The path of the grouped property"
const NetworkAggregateGroupedByGroupedByValue = "The value of the grouped property"

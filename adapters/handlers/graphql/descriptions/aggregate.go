//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

// Package descriptions provides the descriptions as used by the graphql endpoint for Weaviate
package descriptions

// AGGREGATE
const AggregateProperty string = "Aggregate this property"
const LocalAggregateThings string = "Aggregate Things on a local Weaviate"
const LocalAggregateActions string = "Aggregate Things on a local Weaviate"

const GroupBy string = "Specify which properties to group by"

const LocalAggregateObj string = "An object allowing Aggregation of Things and Actions"
const LocalAggregatePropertyObject string = "An object containing Aggregation information about this property"

const LocalAggregateThingsActionsObj string = "An object allowing Aggregation of %ss on a local Weaviate"

const LocalAggregateMean string = "Aggregate on the mean of numeric property values"
const LocalAggregateSum string = "Aggregate on the sum of numeric property values"
const LocalAggregateMedian string = "Aggregate on the median of numeric property values"
const LocalAggregateMode string = "Aggregate on the mode of numeric property values"
const LocalAggregateMin string = "Aggregate on the minimum of numeric property values"
const LocalAggregateMax string = "Aggregate on the maximum of numeric property values"
const LocalAggregateCount string = "Aggregate on the total amount of found property values"
const LocalAggregateGroupedBy string = "Indicates the group of returned data"

const LocalAggregateNumericObj string = "An object containing the %s of numeric properties"

const LocalAggregateCountObj string = "An object containing countable properties"

const LocalAggregateGroupedByObj string = "An object containing the path and value of the grouped property"

const LocalAggregateGroupedByGroupedByPath string = "The path of the grouped property"
const LocalAggregateGroupedByGroupedByValue string = "The value of the grouped property"

// NETWORK
const NetworkAggregateWeaviateObj string = "An object containing Get Things and Actions fields for network Weaviate instance: "

const NetworkAggregate string = "Perform Aggregation of Things and Actions"

const NetworkAggregateThings string = "Aggregate Things on a network Weaviate"
const NetworkAggregateActions string = "Aggregate Things on a network Weaviate"

const NetworkAggregateObj string = "An object allowing Aggregation of Things and Actions"
const NetworkAggregatePropertyObject string = "An object containing Aggregation information about this property"

const NetworkAggregateThingsActionsObj string = "An object allowing Aggregation of %ss on a network Weaviate"

const NetworkAggregateMean string = "Aggregate on the mean of numeric property values"
const NetworkAggregateSum string = "Aggregate on the sum of numeric property values"
const NetworkAggregateMedian string = "Aggregate on the median of numeric property values"
const NetworkAggregateMode string = "Aggregate on the mode of numeric property values"
const NetworkAggregateMin string = "Aggregate on the minimum of numeric property values"
const NetworkAggregateMax string = "Aggregate on the maximum of numeric property values"
const NetworkAggregateCount string = "Aggregate on the total amount of found property values"
const NetworkAggregateGroupedBy string = "Indicates the group of returned data"

const NetworkAggregateNumericObj string = "An object containing the %s of numeric properties"

const NetworkAggregateCountObj string = "An object containing countable properties"

const NetworkAggregateGroupedByObj string = "An object containing the path and value of the grouped property"

const NetworkAggregateGroupedByGroupedByPath string = "The path of the grouped property"
const NetworkAggregateGroupedByGroupedByValue string = "The value of the grouped property"

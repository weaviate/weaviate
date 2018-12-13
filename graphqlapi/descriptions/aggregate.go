/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */

// Package descriptions provides the descriptions as used by the graphql endpoint for Weaviate
package descriptions

import ()

// AGGREGATE
const LocalAggregateThingsDesc string = "Aggregate Things on a local Weaviate"
const LocalAggregateActionsDesc string = "Aggregate Things on a local Weaviate"

const GroupByDesc string = "Specify which properties to group by"

const LocalAggregateObjDesc string = "An object allowing Aggregation of Things and Actions"

const LocalAggregateThingsActionsObjDesc string = "An object allowing Aggregation of %ss on a local Weaviate"

const LocalAggregateMeanDesc string = "Aggregate on the mean of numeric property values"
const LocalAggregateSumDesc string = "Aggregate on the sum of numeric property values"
const LocalAggregateMedianDesc string = "Aggregate on the median of numeric property values"
const LocalAggregateModeDesc string = "Aggregate on the mode of numeric property values"
const LocalAggregateMinDesc string = "Aggregate on the minimum of numeric property values"
const LocalAggregateMaxDesc string = "Aggregate on the maximum of numeric property values"
const LocalAggregateCountDesc string = "Aggregate on the total amount of found property values"
const LocalAggregateGroupedByDesc string = "Aggregate on the path and value" // TODO check this

const LocalAggregateNumericObj string = "An object containing the %s of numeric properties"

const LocalAggregateCountObj string = "An object containing countable properties"

const LocalAggregateGroupedByObjDesc string = "An object containing the path and value of the grouped property"

const LocalAggregateGroupedByGroupedByPathDesc string = "The path of the grouped property"
const LocalAggregateGroupedByGroupedByValueDesc string = "The value of the grouped property"

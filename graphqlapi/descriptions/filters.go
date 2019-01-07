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

// Where filter elements
const LocalGetWhereDesc string = "Filter options for a local Get query, used to convert the result to the specified filters"
const LocalGetWhereInpObjDesc string = "An object containing filter options for a local Get query, used to convert the result to the specified filters"

const LocalGetMetaWhereDesc string = "Filter options for a local GetMeta query, used to convert the result to the specified filters"
const LocalGetMetaWhereInpObjDesc string = "An object containing filter options for a local GetMeta query, used to convert the result to the specified filters"

const LocalAggregateWhereDesc string = "Filter options for a local Aggregate query, used to convert the result to the specified filters"
const LocalAggregateWhereInpObjDesc string = "An object containing filter options for a local Aggregate query, used to convert the result to the specified filters"

const NetworkGetWhereDesc string = "Filter options for a network Get query, used to convert the result to the specified filters"
const NetworkGetWhereInpObjDesc string = "An object containing filter options for a network Get query, used to convert the result to the specified filters"

const NetworkGetMetaWhereDesc string = "Filter options for a network GetMeta query, used to convert the result to the specified filters"
const NetworkGetMetaWhereInpObjDesc string = "An object containing filter options for a network GetMeta query, used to convert the result to the specified filters"

const NetworkAggregateWhereDesc string = "Filter options for a network Aggregate query, used to convert the result to the specified filters"
const NetworkAggregateWhereInpObjDesc string = "An object containing filter options for a network Aggregate query, used to convert the result to the specified filters"

const WhereOperandsDesc string = "Contains the Operands that can be applied to a 'where' filter"
const WhereOperandsInpObjDesc string = "An object containing the Operands that can be applied to a 'where' filter"

const WhereOperatorDesc string = "Contains the Operators that can be applied to a 'where' filter"
const WhereOperatorEnumDesc string = "An object containing the Operators that can be applied to a 'where' filter"

const WherePathDesc string = "Specify the path from the Things or Actions fields to the property name (e.g. ['Things', 'City', 'population'] leads to the 'population' property of a 'City' object)"

const WhereValueIntDesc string = "Specify an Integer value that the target property will be compared to"
const WhereValueNumberDesc string = "Specify a Float value that the target property will be compared to"
const WhereValueBooleanDesc string = "Specify a Boolean value that the target property will be compared to"
const WhereValueStringDesc string = "Specify a String value that the target property will be compared to"
const WhereValueTextDesc string = "Specify a Text value that the target property will be compared to"
const WhereValueDateDesc string = "Specify a Date value that the target property will be compared to"

// Properties and Classes filter elements (used by Fetch and Introspect Where filters)
const WherePropertiesDesc string = "Specify which properties to filter on"
const WherePropertiesObjDesc string = "Specify which properties to filter on"

const WherePropertiesPropertyNameDesc string = "Specify which property name to filter properties on"
const WhereCertaintyDesc string = "Specify the required degree of similarity between an object's characteristics and the provided filter values on a scale of 0-1"
const WhereNameDesc string = "Specify the name of the property to filter on"

const WhereKeywordsDesc string = "Specify which keywords to filter on"
const WhereKeywordsInpObjDesc string = "Specify the value and the weight of a keyword"

const WhereKeywordsValueDesc string = "Specify the value of the keyword"
const WhereKeywordsWeightDesc string = "Specify the weight of the keyword"

const WhereClassDesc string = "Specify which classes to filter on"
const WhereInpObjDesc string = "Specify which classes and properties to filter on"

// Unique Fetch filter elements
const FetchWhereFilterFieldsDesc string = "An object containing filter options for a network Fetch search, used to convert the result to the specified filters"
const FetchWhereFilterFieldsInpObjDesc string = "Filter options for a network Fetch search, used to convert the result to the specified filters"

const FetchFuzzyValueDesc string = "Specify the concept that will be used to fetch Things or Actions on the network (e.g. 'Airplane', or 'City')"
const FetchFuzzyCertaintyDesc string = "Specify how much a Beacon's characteristics must match the provided concept on a scale of 0 to 1"

// Unique Introspect filter elements
const IntrospectWhereFilterFieldsDesc string = "An object containing filter options for a network Fetch search, used to convert the result to the specified filters"
const IntrospectWhereFilterFieldsInpObjDesc string = "Filter options for a network Fetch search, used to convert the result to the specified filters"
const IntrospectBeaconIdDesc string = "The id of the Beacon"

// GroupBy filter elements
const GroupByGroupDesc string = "Specify the property of the class to group by"
const GroupByCountDesc string = "Get the number of instances of a property in a group"
const GroupBySumDesc string = "Get the sum of the values of a property in a group"
const GroupByMinDesc string = "Get the lowest occuring value of a property in a group"
const GroupByMaxDesc string = "Get the highest occuring value of a property in a group"
const GroupByMeanDesc string = "Get the average value of a property in a group"
const GroupByMedianDesc string = "Get the median of a property in a group"
const GroupByModeDesc string = "Get the mode of a property in a group"

// Request timeout filter elements
const NetworkTimeoutDesc string = "Specify the time in seconds after which an unresolved request automatically fails"

// Pagination filter elements
const FirstDesc string = "Show the first x results (pagination option)"
const AfterDesc string = "Show the results after the first x results (pagination option)"

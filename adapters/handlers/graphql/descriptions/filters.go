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

// Where filter elements
const GetWhere = "Filter options for a local Get query, used to convert the result to the specified filters"
const GetWhereInpObj = "An object containing filter options for a local Get query, used to convert the result to the specified filters"

const LocalMetaWhere = "Filter options for a local Meta query, used to convert the result to the specified filters"
const LocalMetaWhereInpObj = "An object containing filter options for a local Meta query, used to convert the result to the specified filters"

const AggregateWhere = "Filter options for a local Aggregate query, used to convert the result to the specified filters"
const AggregateWhereInpObj = "An object containing filter options for a local Aggregate query, used to convert the result to the specified filters"

const NetworkGetWhere = "Filter options for a network Get query, used to convert the result to the specified filters"
const NetworkGetWhereInpObj = "An object containing filter options for a network Get query, used to convert the result to the specified filters"

const NetworkMetaWhere = "Filter options for a network Meta query, used to convert the result to the specified filters"
const NetworkMetaWhereInpObj = "An object containing filter options for a network Meta query, used to convert the result to the specified filters"

const NetworkAggregateWhere = "Filter options for a network Aggregate query, used to convert the result to the specified filters"
const NetworkAggregateWhereInpObj = "An object containing filter options for a network Aggregate query, used to convert the result to the specified filters"

const WhereOperands = "Contains the Operands that can be applied to a 'where' filter"
const WhereOperandsInpObj = "An object containing the Operands that can be applied to a 'where' filter"

const WhereOperator = "Contains the Operators that can be applied to a 'where' filter"
const WhereOperatorEnum = "An object containing the Operators that can be applied to a 'where' filter"

const WherePath = "Specify the path from the Things or Actions fields to the property name (e.g. ['Things', 'City', 'population'] leads to the 'population' property of a 'City' object)"

const WhereValueInt = "Specify an Integer value that the target property will be compared to"
const WhereValueNumber = "Specify a Float value that the target property will be compared to"
const WhereValueBoolean = "Specify a Boolean value that the target property will be compared to"
const WhereValueString = "Specify a String value that the target property will be compared to"
const WhereValueRange = "Specify both geo-coordinates (latitude and longitude as decimals) and a maximum distance from the described coordinates. The search will return any result which is located less than or equal to the specified maximum distance in km away from the specified point."
const WhereValueRangeGeoCoordinates = "The geoCoordinates that form the center point of the search."
const WhereValueRangeGeoCoordinatesLatitude = "The latitude (in decimal format) of the geoCoordinates to search around."
const WhereValueRangeGeoCoordinatesLongitude = "The longitude (in decimal format) of the geoCoordinates to search around."
const WhereValueRangeDistance = "The distance from the point specified via geoCoordinates."
const WhereValueRangeDistanceMax = "The maximum distance from the point specified geoCoordinates."
const WhereValueText = "Specify a Text value that the target property will be compared to"
const WhereValueDate = "Specify a Date value that the target property will be compared to"

// Properties and Classes filter elements (used by Fetch and Introspect Where filters)
const WhereProperties = "Specify which properties to filter on"
const WherePropertiesObj = "Specify which properties to filter on"

const WherePropertiesPropertyName = "Specify which property name to filter properties on"
const WhereCertainty = "Specify the required degree of similarity between an object's characteristics and the provided filter values on a scale of 0-1"
const WhereName = "Specify the name of the property to filter on"

const WhereKeywords = "Specify which keywords to filter on"
const WhereKeywordsInpObj = "Specify the value and the weight of a keyword"

const WhereKeywordsValue = "Specify the value of the keyword"
const WhereKeywordsWeight = "Specify the weight of the keyword"

const WhereClass = "Specify which classes to filter on"
const WhereInpObj = "Specify which classes and properties to filter on"

// Unique Fetch filter elements
const FetchWhereFilterFields = "An object containing filter options for a network Fetch search, used to convert the result to the specified filters"
const FetchWhereFilterFieldsInpObj = "Filter options for a network Fetch search, used to convert the result to the specified filters"

const FetchFuzzyValue = "Specify the concept that will be used to fetch Things or Actions on the network (e.g. 'Airplane', or 'City')"
const FetchFuzzyCertainty = "Specify how much a Beacon's characteristics must match the provided concept on a scale of 0 to 1"

// Unique Introspect filter elements
const IntrospectWhereFilterFields = "An object containing filter options for a network Fetch search, used to convert the result to the specified filters"
const IntrospectWhereFilterFieldsInpObj = "Filter options for a network Fetch search, used to convert the result to the specified filters"
const IntrospectBeaconId = "The id of the Beacon"

// GroupBy filter elements
const GroupByGroup = "Specify the property of the class to group by"
const GroupByCount = "Get the number of instances of a property in a group"
const GroupBySum = "Get the sum of the values of a property in a group"
const GroupByMin = "Get the minimum occurring value of a property in a group"
const GroupByMax = "Get the maximum occurring value of a property in a group"
const GroupByMean = "Get the mean value of a property in a group"
const GroupByMedian = "Get the median of a property in a group"
const GroupByMode = "Get the mode of a property in a group"

// Request timeout filter elements
const NetworkTimeout = "Specify the time in seconds after which an unresolved request automatically fails"

// Pagination filter elements
const First = "Show the first x results (pagination option)"
const After = "Show the results after the first x results (pagination option)"

//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
//  DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

// Package descriptions provides the descriptions as used by the graphql endpoint for Weaviate
package descriptions

// Where filter elements
const LocalGetWhere string = "Filter options for a local Get query, used to convert the result to the specified filters"
const LocalGetWhereInpObj string = "An object containing filter options for a local Get query, used to convert the result to the specified filters"

const LocalGetMetaWhere string = "Filter options for a local GetMeta query, used to convert the result to the specified filters"
const LocalGetMetaWhereInpObj string = "An object containing filter options for a local GetMeta query, used to convert the result to the specified filters"

const LocalAggregateWhere string = "Filter options for a local Aggregate query, used to convert the result to the specified filters"
const LocalAggregateWhereInpObj string = "An object containing filter options for a local Aggregate query, used to convert the result to the specified filters"

const NetworkGetWhere string = "Filter options for a network Get query, used to convert the result to the specified filters"
const NetworkGetWhereInpObj string = "An object containing filter options for a network Get query, used to convert the result to the specified filters"

const NetworkGetMetaWhere string = "Filter options for a network GetMeta query, used to convert the result to the specified filters"
const NetworkGetMetaWhereInpObj string = "An object containing filter options for a network GetMeta query, used to convert the result to the specified filters"

const NetworkAggregateWhere string = "Filter options for a network Aggregate query, used to convert the result to the specified filters"
const NetworkAggregateWhereInpObj string = "An object containing filter options for a network Aggregate query, used to convert the result to the specified filters"

const WhereOperands string = "Contains the Operands that can be applied to a 'where' filter"
const WhereOperandsInpObj string = "An object containing the Operands that can be applied to a 'where' filter"

const WhereOperator string = "Contains the Operators that can be applied to a 'where' filter"
const WhereOperatorEnum string = "An object containing the Operators that can be applied to a 'where' filter"

const WherePath string = "Specify the path from the Things or Actions fields to the property name (e.g. ['Things', 'City', 'population'] leads to the 'population' property of a 'City' object)"

const WhereValueInt string = "Specify an Integer value that the target property will be compared to"
const WhereValueNumber string = "Specify a Float value that the target property will be compared to"
const WhereValueBoolean string = "Specify a Boolean value that the target property will be compared to"
const WhereValueString string = "Specify a String value that the target property will be compared to"
const WhereValueRange string = "Specify both geo-coordinates (latitude and longitude as decimals) and a maximum distance from the described coordinates. The search will return any result which is located less than or equal to the specified maximum distance in km away from the specified point."
const WhereValueRangeGeoCoordinates string = "The geoCoordinates that form the center point of the search."
const WhereValueRangeGeoCoordinatesLatitude string = "The latitude (in decimal format) of the geoCoordinates to search around."
const WhereValueRangeGeoCoordinatesLongitude string = "The longitude (in decimal format) of the geoCoordinates to search around."
const WhereValueRangeDistance string = "The distance from the point specified via geoCoordinates."
const WhereValueRangeDistanceMax string = "The maximum distance from the point specified geoCoordinates."
const WhereValueText string = "Specify a Text value that the target property will be compared to"
const WhereValueDate string = "Specify a Date value that the target property will be compared to"

// Properties and Classes filter elements (used by Fetch and Introspect Where filters)
const WhereProperties string = "Specify which properties to filter on"
const WherePropertiesObj string = "Specify which properties to filter on"

const WherePropertiesPropertyName string = "Specify which property name to filter properties on"
const WhereCertainty string = "Specify the required degree of similarity between an object's characteristics and the provided filter values on a scale of 0-1"
const WhereName string = "Specify the name of the property to filter on"

const WhereKeywords string = "Specify which keywords to filter on"
const WhereKeywordsInpObj string = "Specify the value and the weight of a keyword"

const WhereKeywordsValue string = "Specify the value of the keyword"
const WhereKeywordsWeight string = "Specify the weight of the keyword"

const WhereClass string = "Specify which classes to filter on"
const WhereInpObj string = "Specify which classes and properties to filter on"

// Unique Fetch filter elements
const FetchWhereFilterFields string = "An object containing filter options for a network Fetch search, used to convert the result to the specified filters"
const FetchWhereFilterFieldsInpObj string = "Filter options for a network Fetch search, used to convert the result to the specified filters"

const FetchFuzzyValue string = "Specify the concept that will be used to fetch Things or Actions on the network (e.g. 'Airplane', or 'City')"
const FetchFuzzyCertainty string = "Specify how much a Beacon's characteristics must match the provided concept on a scale of 0 to 1"

// Unique Introspect filter elements
const IntrospectWhereFilterFields string = "An object containing filter options for a network Fetch search, used to convert the result to the specified filters"
const IntrospectWhereFilterFieldsInpObj string = "Filter options for a network Fetch search, used to convert the result to the specified filters"
const IntrospectBeaconId string = "The id of the Beacon"

// GroupBy filter elements
const GroupByGroup string = "Specify the property of the class to group by"
const GroupByCount string = "Get the number of instances of a property in a group"
const GroupBySum string = "Get the sum of the values of a property in a group"
const GroupByMin string = "Get the minimum occuring value of a property in a group"
const GroupByMax string = "Get the maximum occuring value of a property in a group"
const GroupByMean string = "Get the mean value of a property in a group"
const GroupByMedian string = "Get the median of a property in a group"
const GroupByMode string = "Get the mode of a property in a group"

// Request timeout filter elements
const NetworkTimeout string = "Specify the time in seconds after which an unresolved request automatically fails"

// Pagination filter elements
const First string = "Show the first x results (pagination option)"
const After string = "Show the results after the first x results (pagination option)"

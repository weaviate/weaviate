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

// Local
const (
	LocalMetaObj = "An object used to Get Meta information about Objects on a local Weaviate"
	LocalMeta    = "Get Meta information about Objects on a local Weaviate"
)

const (
	MetaPropertyType                 = "The datatype of this property"
	MetaPropertyCount                = "The total amount of found instances for this property" // TODO check this with @lauraham
	MetaPropertyTopOccurrences       = "An object containing data about the most frequently occurring values for this property"
	MetaPropertyTopOccurrencesValue  = "The most frequently occurring value for this property"
	MetaPropertyTopOccurrencesOccurs = "How often the most frequently occurring value for this property occurs" // TODO check this with @lauraham
	MetaPropertyMinimum              = "The minimum value for this property"
	MetaPropertyMaximum              = "The maximum value for this property"
	MetaPropertyMean                 = "The mean of all values for this property"
	MetaPropertySum                  = "The sum of all values for this property"
	MetaPropertyObject               = "An object containing meta information about this property"
)

const (
	AggregatePropertyType                 = "The datatype of this property"
	AggregatePropertyCount                = "The total amount of found instances for this property" // TODO check this with @lauraham
	AggregatePropertyTopOccurrences       = "An object containing data about the most frequently occurring values for this property"
	AggregatePropertyTopOccurrencesValue  = "The most frequently occurring value for this property"
	AggregatePropertyTopOccurrencesOccurs = "How often the most frequently occurring value for this property occurs" // TODO check this with @lauraham
	AggregatePropertyMinimum              = "The minimum value for this property"
	AggregatePropertyMaximum              = "The maximum value for this property"
	AggregatePropertyMean                 = "The mean of all values for this property"
	AggregatePropertySum                  = "The sum of all values for this property"
)

// Network
const (
	NetworkMeta            = "Get meta information about Objects from a Weaviate in a network"
	NetworkMetaObj         = "An object used to Get meta information about Objects from a Weaviate in a network"
	NetworkMetaWeaviateObj = "An object containing the Meta Objects fields for network Weaviate instance: "
)

const (
	MetaMetaProperty = "Meta information about the object"
	MetaProperty     = "Meta information about the property "
)

const (
	MetaClassPropertyTotalTrue      = "How often this boolean property's value is true in the dataset"
	MetaClassPropertyPercentageTrue = "The percentage of true values for this boolean property in the dataset"
)

const (
	MetaClassPropertyTotalFalse      = "How often this boolean property's value is false in the dataset"
	MetaClassPropertyPercentageFalse = "The percentage of false values for this boolean property in the dataset"
)

const (
	MetaClassPropertyPointingTo = "The classes that this object contains a reference to"
	MetaClassMetaCount          = "The total amount of found instances for a class"
	MetaClassMetaObj            = "An object containing Meta information about a class"
)

const (
	AggregateClassPropertyTotalTrue      = "How often this boolean property's value is true in the dataset"
	AggregateClassPropertyPercentageTrue = "The percentage of true values for this boolean property in the dataset"
)

const (
	AggregateClassPropertyTotalFalse      = "How often this boolean property's value is false in the dataset"
	AggregateClassPropertyPercentageFalse = "The percentage of false values for this boolean property in the dataset"
)

const (
	AggregateClassPropertyPointingTo = "The classes that this object contains a reference to"
	AggregateClassAggregateCount     = "The total amount of found instances for a class"
	AggregateClassAggregateObj       = "An object containing Aggregate information about a class"
)

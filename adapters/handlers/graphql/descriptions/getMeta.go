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

// Local
const LocalMetaActions = "Get Meta information about Actions on a local Weaviate"
const LocalMetaThings = "Get Meta information about Things on a local Weaviate"

const LocalMetaThingsObj = "An object used to Get Meta information about Things on a local Weaviate"
const LocalMetaActionsObj = "An object used to Get Meta information about Actions on a local Weaviate"

const LocalMetaObj = "An object used to Get Meta information about Things or Actions on a local Weaviate"
const LocalMeta = "Get Meta information about Things or Actions on a local Weaviate"

const MetaPropertyType = "The datatype of this property"
const MetaPropertyCount = "The total amount of found instances for this property" // TODO check this with @lauraham
const MetaPropertyTopOccurrences = "An object containing data about the most frequently occuring values for this property"
const MetaPropertyTopOccurrencesValue = "The most frequently occurring value for this property"
const MetaPropertyTopOccurrencesOccurs = "How often the most frequently occuring value for this property occurs" // TODO check this with @lauraham
const MetaPropertyMinimum = "The minimum value for this property"
const MetaPropertyMaximum = "The maximum value for this property"
const MetaPropertyMean = "The mean of all values for this property"
const MetaPropertySum = "The sum of all values for this property"
const MetaPropertyObject = "An object containing meta information about this property"

const AggregatePropertyType = "The datatype of this property"
const AggregatePropertyCount = "The total amount of found instances for this property" // TODO check this with @lauraham
const AggregatePropertyTopOccurrences = "An object containing data about the most frequently occuring values for this property"
const AggregatePropertyTopOccurrencesValue = "The most frequently occurring value for this property"
const AggregatePropertyTopOccurrencesOccurs = "How often the most frequently occuring value for this property occurs" // TODO check this with @lauraham
const AggregatePropertyMinimum = "The minimum value for this property"
const AggregatePropertyMaximum = "The maximum value for this property"
const AggregatePropertyMean = "The mean of all values for this property"
const AggregatePropertySum = "The sum of all values for this property"

// Network
const NetworkMeta = "Get meta information about Things or Actions from a Weaviate in a network"
const NetworkMetaObj = "An object used to Get meta information about Things or Actions from a Weaviate in a network"
const NetworkMetaWeaviateObj = "An object containing the Meta Things and Actions fields for network Weaviate instance: "

const NetworkMetaActions = "Get Meta information about Actions from a network Weaviate"
const NetworkMetaThings = "Get Meta information about Things from a network Weaviate"

const NetworkMetaThingsObj = "An object used to Get Meta information about Things on a network Weaviate"
const NetworkMetaActionsObj = "An object used to Get Meta information about Actions on a network Weaviate"

const MetaMetaProperty = "Meta information about the object"
const MetaProperty = "Meta information about the property "

const MetaClassPropertyTotalTrue = "How often this boolean property's value is true in the dataset"
const MetaClassPropertyPercentageTrue = "The percentage of true values for this boolean property in the dataset"

const MetaClassPropertyTotalFalse = "How often this boolean property's value is false in the dataset"
const MetaClassPropertyPercentageFalse = "The percentage of false values for this boolean property in the dataset"

const MetaClassPropertyPointingTo = "The classes that this object contains a reference to"
const MetaClassMetaCount = "The total amount of found instances for a class"
const MetaClassMetaObj = "An object containing Meta information about a class"

const AggregateClassPropertyTotalTrue = "How often this boolean property's value is true in the dataset"
const AggregateClassPropertyPercentageTrue = "The percentage of true values for this boolean property in the dataset"

const AggregateClassPropertyTotalFalse = "How often this boolean property's value is false in the dataset"
const AggregateClassPropertyPercentageFalse = "The percentage of false values for this boolean property in the dataset"

const AggregateClassPropertyPointingTo = "The classes that this object contains a reference to"
const AggregateClassAggregateCount = "The total amount of found instances for a class"
const AggregateClassAggregateObj = "An object containing Aggregate information about a class"

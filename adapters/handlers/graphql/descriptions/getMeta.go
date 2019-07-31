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

// Local
const LocalMetaActions string = "Get Meta information about Actions on a local Weaviate"
const LocalMetaThings string = "Get Meta information about Things on a local Weaviate"

const LocalMetaThingsObj string = "An object used to Get Meta information about Things on a local Weaviate"
const LocalMetaActionsObj string = "An object used to Get Meta information about Actions on a local Weaviate"

const LocalMetaObj string = "An object used to Get Meta information about Things or Actions on a local Weaviate"
const LocalMeta string = "Get Meta information about Things or Actions on a local Weaviate"

const MetaPropertyType string = "The datatype of this property"
const MetaPropertyCount string = "The total amount of found instances for this property" // TODO check this with @lauraham
const MetaPropertyTopOccurrences string = "An object containing data about the most frequently occuring values for this property"
const MetaPropertyTopOccurrencesValue string = "The most frequently occurring value for this property"
const MetaPropertyTopOccurrencesOccurs string = "How often the most frequently occuring value for this property occurs" // TODO check this with @lauraham
const MetaPropertyMinimum string = "The minimum value for this property"
const MetaPropertyMaximum string = "The maximum value for this property"
const MetaPropertyMean string = "The mean of all values for this property"
const MetaPropertySum string = "The sum of all values for this property"
const MetaPropertyObject string = "An object containing meta information about this property"

// Network
const NetworkMeta string = "Get meta information about Things or Actions from a Weaviate in a network"
const NetworkMetaObj string = "An object used to Get meta information about Things or Actions from a Weaviate in a network"
const NetworkMetaWeaviateObj string = "An object containing the Meta Things and Actions fields for network Weaviate instance: "

const NetworkMetaActions string = "Get Meta information about Actions from a network Weaviate"
const NetworkMetaThings string = "Get Meta information about Things from a network Weaviate"

const NetworkMetaThingsObj string = "An object used to Get Meta information about Things on a network Weaviate"
const NetworkMetaActionsObj string = "An object used to Get Meta information about Actions on a network Weaviate"

const MetaMetaProperty string = "Meta information about the object"
const MetaProperty string = "Meta information about the property "

const MetaClassPropertyTotalTrue string = "How often this boolean property's value is true in the dataset"
const MetaClassPropertyPercentageTrue string = "The percentage of true values for this boolean property in the dataset"

const MetaClassPropertyTotalFalse string = "How often this boolean property's value is false in the dataset"
const MetaClassPropertyPercentageFalse string = "The percentage of false values for this boolean property in the dataset"

const MetaClassPropertyPointingTo string = "The classes that this object contains a reference to"
const MetaClassMetaCount string = "The total amount of found instances for a class"
const MetaClassMetaObj string = "An object containing Meta information about a class"

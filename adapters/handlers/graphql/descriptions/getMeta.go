/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */

// Package descriptions provides the descriptions as used by the graphql endpoint for Weaviate
package descriptions

// Local
const LocalGetMetaActions string = "Get Meta information about Actions on a local Weaviate"
const LocalGetMetaThings string = "Get Meta information about Things on a local Weaviate"

const LocalGetMetaThingsObj string = "An object used to Get Meta information about Things on a local Weaviate"
const LocalGetMetaActionsObj string = "An object used to Get Meta information about Actions on a local Weaviate"

const LocalGetMetaObj string = "An object used to Get Meta information about Things or Actions on a local Weaviate"
const LocalGetMeta string = "Get Meta information about Things or Actions on a local Weaviate"

const GetMetaPropertyType string = "The datatype of this property"
const GetMetaPropertyCount string = "The total amount of found instances for this property" // TODO check this with @lauraham
const GetMetaPropertyTopOccurrences string = "An object containing data about the most frequently occuring values for this property"
const GetMetaPropertyTopOccurrencesValue string = "The most frequently occurring value for this property"
const GetMetaPropertyTopOccurrencesOccurs string = "How often the most frequently occuring value for this property occurs" // TODO check this with @lauraham
const GetMetaPropertyMinimum string = "The minimum value for this property"
const GetMetaPropertyMaximum string = "The maximum value for this property"
const GetMetaPropertyMean string = "The mean of all values for this property"
const GetMetaPropertySum string = "The sum of all values for this property"
const GetMetaPropertyObject string = "An object containing meta information about this property"

// Network
const NetworkGetMeta string = "Get meta information about Things or Actions from a Weaviate in a network"
const NetworkGetMetaObj string = "An object used to Get meta information about Things or Actions from a Weaviate in a network"
const NetworkGetMetaWeaviateObj string = "An object containing the GetMeta Things and Actions fields for network Weaviate instance: "

const NetworkGetMetaActions string = "Get Meta information about Actions from a network Weaviate"
const NetworkGetMetaThings string = "Get Meta information about Things from a network Weaviate"

const NetworkGetMetaThingsObj string = "An object used to Get Meta information about Things on a network Weaviate"
const NetworkGetMetaActionsObj string = "An object used to Get Meta information about Actions on a network Weaviate"

const GetMetaMetaProperty string = "Meta information about the object"
const GetMetaProperty string = "Meta information about the property "

const GetMetaClassPropertyTotalTrue string = "How often this boolean property's value is true in the dataset"
const GetMetaClassPropertyPercentageTrue string = "The percentage of true values for this boolean property in the dataset"

const GetMetaClassPropertyTotalFalse string = "How often this boolean property's value is false in the dataset"
const GetMetaClassPropertyPercentageFalse string = "The percentage of false values for this boolean property in the dataset"

const GetMetaClassPropertyPointingTo string = "The classes that this object contains a reference to"
const GetMetaClassMetaCount string = "The total amount of found instances for a class"
const GetMetaClassMetaObj string = "An object containing Meta information about a class"

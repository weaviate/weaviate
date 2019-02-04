/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */

// Package descriptions provides the descriptions as used by the graphql endpoint for Weaviate
package descriptions

import ()

// Local
const LocalGetMetaActionsDesc string = "Get Meta information about Actions on a local Weaviate"
const LocalGetMetaThingsDesc string = "Get Meta information about Things on a local Weaviate"

const LocalGetMetaThingsObjDesc string = "An object used to Get Meta information about Things on a local Weaviate"
const LocalGetMetaActionsObjDesc string = "An object used to Get Meta information about Actions on a local Weaviate"

const LocalGetMetaObjDesc string = "An object used to Get Meta information about Things or Actions on a local Weaviate"
const LocalGetMetaDesc string = "Get Meta information about Things or Actions on a local Weaviate"

const GetMetaPropertyTypeDesc string = "The datatype of this property"
const GetMetaPropertyCountDesc string = "The total amount of found instances for this property" // TODO check this with @lauraham
const GetMetaPropertyTopOccurrencesDesc string = "An object containing data about the most frequently occuring values for this property"
const GetMetaPropertyTopOccurrencesValueDesc string = "The most frequently occurring value for this property"
const GetMetaPropertyTopOccurrencesOccursDesc string = "How often the most frequently occuring value for this property occurs" // TODO check this with @lauraham
const GetMetaPropertyMinimumDesc string = "The minimum value for this property"
const GetMetaPropertyMaximumDesc string = "The maximum value for this property"
const GetMetaPropertyMeanDesc string = "The mean of all values for this property"
const GetMetaPropertySumDesc string = "The sum of all values for this property"
const GetMetaPropertyObjectDesc string = "An object containing meta information about this property"

// Network
const NetworkGetMetaDesc string = "Get meta information about Things or Actions from a Weaviate in a network"
const NetworkGetMetaObjDesc string = "An object used to Get meta information about Things or Actions from a Weaviate in a network"
const NetworkGetMetaWeaviateObjDesc string = "An object containing the GetMeta Things and Actions fields for network Weaviate instance: "

const NetworkGetMetaActionsDesc string = "Get Meta information about Actions from a network Weaviate"
const NetworkGetMetaThingsDesc string = "Get Meta information about Things from a network Weaviate"

const NetworkGetMetaThingsObjDesc string = "An object used to Get Meta information about Things on a network Weaviate"
const NetworkGetMetaActionsObjDesc string = "An object used to Get Meta information about Actions on a network Weaviate"

const GetMetaMetaPropertyDesc string = "Meta information about the object"
const GetMetaPropertyDesc string = "Meta information about the property "

const GetMetaClassPropertyTotalTrueDesc string = "How often this boolean property's value is true in the dataset"
const GetMetaClassPropertyPercentageTrueDesc string = "The percentage of true values for this boolean property in the dataset"

const GetMetaClassPropertyTotalFalseDesc string = "How often this boolean property's value is false in the dataset"
const GetMetaClassPropertyPercentageFalseDesc string = "The percentage of false values for this boolean property in the dataset"

const GetMetaClassPropertyPointingToDesc string = "The classes that this object contains a reference to"
const GetMetaClassMetaCountDesc string = "The total amount of found instances for a class"
const GetMetaClassMetaObjDesc string = "An object containing Meta information about a class"

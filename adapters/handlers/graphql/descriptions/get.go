//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

// Package descriptions provides the descriptions as used by the graphql endpoint for Weaviate
package descriptions

// Local
const GetActions = "Get Actions on a local Weaviate"
const GetThings = "Get Things on a local Weaviate"

const GetObj = "An object used to Get Things or Actions on a local Weaviate"
const Get = "Get Things or Actions on a local Weaviate"

const GetThingsActionsObj = "An object used to get %ss on a local Weaviate"

const GetClassUUID = "The UUID of a Thing or Action, assigned by its local Weaviate"

// Network
const NetworkGet = "Get Things or Actions from a Weaviate in a network"
const NetworkGetObj = "An object used to Get Things or Actions from a Weaviate in a network"

const NetworkGetWeaviateObj = "An object containing Get Things and Actions fields for network Weaviate instance: "

const NetworkGetActions = "Get Actions from a Weaviate in a network"
const NetworkGetThings = "Get Things from a Weaviate in a network"

const NetworkGetActionsObj = "An object containing the Actions objects on this network Weaviate instance."
const NetworkGetThingsObj = "An object containing the Things objects on this network Weaviate instance."

const NetworkGetClassUUID = "The UUID of a Thing or Action, assigned by the Weaviate network" // TODO check this with @lauraham

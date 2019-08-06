//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

// Package descriptions provides the descriptions as used by the graphql endpoint for Weaviate
package descriptions

// Local
const LocalMergeActions = "Merge Actions on a local Weaviate"
const LocalMergeThings = "Merge Things on a local Weaviate"

const LocalMergeObj = "An object used to Merge Things or Actions on a local Weaviate"
const LocalMerge = "Merge Things or Actions on a local Weaviate"

const LocalMergeThingsActionsObj = "An object used to get %ss on a local Weaviate"

const LocalMergeClassUUID = "The UUID of a Thing or Action, assigned by its local Weaviate"

// Network
const NetworkMerge = "Merge Things or Actions from a Weaviate in a network"
const NetworkMergeObj = "An object used to Merge Things or Actions from a Weaviate in a network"

const NetworkMergeWeaviateObj = "An object containing Merge Things and Actions fields for network Weaviate instance: "

const NetworkMergeActions = "Merge Actions from a Weaviate in a network"
const NetworkMergeThings = "Merge Things from a Weaviate in a network"

const NetworkMergeActionsObj = "An object containing the Actions objects on this network Weaviate instance."
const NetworkMergeThingsObj = "An object containing the Things objects on this network Weaviate instance."

const NetworkMergeClassUUID = "The UUID of a Thing or Action, assigned by the Weaviate network" // TODO check this with @lauraham

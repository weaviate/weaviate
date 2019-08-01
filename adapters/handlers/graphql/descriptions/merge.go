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
const LocalMergeActions string = "Merge Actions on a local Weaviate"
const LocalMergeThings string = "Merge Things on a local Weaviate"

const LocalMergeObj string = "An object used to Merge Things or Actions on a local Weaviate"
const LocalMerge string = "Merge Things or Actions on a local Weaviate"

const LocalMergeThingsActionsObj string = "An object used to get %ss on a local Weaviate"

const LocalMergeClassUUID string = "The UUID of a Thing or Action, assigned by its local Weaviate"

// Network
const NetworkMerge string = "Merge Things or Actions from a Weaviate in a network"
const NetworkMergeObj string = "An object used to Merge Things or Actions from a Weaviate in a network"

const NetworkMergeWeaviateObj string = "An object containing Merge Things and Actions fields for network Weaviate instance: "

const NetworkMergeActions string = "Merge Actions from a Weaviate in a network"
const NetworkMergeThings string = "Merge Things from a Weaviate in a network"

const NetworkMergeActionsObj string = "An object containing the Actions objects on this network Weaviate instance."
const NetworkMergeThingsObj string = "An object containing the Things objects on this network Weaviate instance."

const NetworkMergeClassUUID string = "The UUID of a Thing or Action, assigned by the Weaviate network" // TODO check this with @lauraham

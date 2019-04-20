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

// Local
const LocalGetActions string = "Get Actions on a local Weaviate"
const LocalGetThings string = "Get Things on a local Weaviate"

const LocalGetObj string = "An object used to Get Things or Actions on a local Weaviate"
const LocalGet string = "Get Things or Actions on a local Weaviate"

const LocalGetThingsActionsObj string = "An object used to get %ss on a local Weaviate"

const LocalGetClassUUID string = "The UUID of a Thing or Action, assigned by its local Weaviate"

// Network
const NetworkGet string = "Get Things or Actions from a Weaviate in a network"
const NetworkGetObj string = "An object used to Get Things or Actions from a Weaviate in a network"

const NetworkGetWeaviateObj string = "An object containing Get Things and Actions fields for network Weaviate instance: "

const NetworkGetActions string = "Get Actions from a Weaviate in a network"
const NetworkGetThings string = "Get Things from a Weaviate in a network"

const NetworkGetWeaviateActionsObj string = "An object containing the Actions objects on this network Weaviate instance."
const NetworkGetWeaviateThingsObj string = "An object containing the Things objects on this network Weaviate instance."

const NetworkGetClassUUID string = "The UUID of a Thing or Action, assigned by the Weaviate network" // TODO check this with @lauraham

/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */

// Package descriptions provides the descriptions as used by the graphql endpoint for Weaviate
package descriptions

import ()

// Local
const LocalGetActionsDesc string = "Get Actions on a local Weaviate"
const LocalGetThingsDesc string = "Get Things on a local Weaviate"

const LocalGetObjDesc string = "An object used to Get Things or Actions on a local Weaviate"
const LocalGetDesc string = "Get Things or Actions on a local Weaviate"

const LocalGetThingsActionsObjDesc string = "An object used to get %ss on a local Weaviate"

const LocalGetClassUUIDDesc string = "The UUID of a Thing or Action, assigned by its local Weaviate"

// Network
const NetworkGetDesc string = "Get Things or Actions from a Weaviate in a network"
const NetworkGetObjDesc string = "An object used to Get Things or Actions from a Weaviate in a network"

const NetworkGetWeaviateObjDesc string = "An object containing Get Things and Actions fields for network Weaviate instance: "

const NetworkGetActionsDesc string = "Get Actions from a Weaviate in a network"
const NetworkGetThingsDesc string = "Get Things from a Weaviate in a network"

const NetworkGetWeaviateActionsObjDesc string = "An object containing the Actions objects on this network Weaviate instance."
const NetworkGetWeaviateThingsObjDesc string = "An object containing the Things objects on this network Weaviate instance."

const NetworkGetClassUUIDDesc string = "The UUID of a Thing or Action, assigned by the Weaviate network" // TODO check this with @lauraham

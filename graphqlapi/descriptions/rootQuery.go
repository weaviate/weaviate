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

// ROOT
const WeaviateObjDesc string = "The location of the root query"
const WeaviateLocalDesc string = "Query a local Weaviate"
const WeaviateNetworkDesc string = "Query a Weaviate network"

// LOCAL
const LocalObjDesc string = "A query on a local Weaviate"

// NETWORK
const NetworkWeaviateDesc string = "An object for the network Weaviate instance: "
const NetworkObjDesc string = "An object used to perform queries on a Weaviate network"

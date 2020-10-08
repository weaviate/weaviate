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

// ROOT
const (
	WeaviateObj     = "The location of the root query"
	WeaviateNetwork = "Query a Weaviate network"
)

// LOCAL
const LocalObj = "A query on a local Weaviate"

// NETWORK
const (
	NetworkWeaviate = "An object for the network Weaviate instance: "
	NetworkObj      = "An object used to perform queries on a Weaviate network"
)

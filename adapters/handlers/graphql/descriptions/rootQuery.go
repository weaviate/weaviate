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

// ROOT
const WeaviateObj = "The location of the root query"
const WeaviateNetwork = "Query a Weaviate network"

// LOCAL
const LocalObj = "A query on a local Weaviate"

// NETWORK
const NetworkWeaviate = "An object for the network Weaviate instance: "
const NetworkObj = "An object used to perform queries on a Weaviate network"

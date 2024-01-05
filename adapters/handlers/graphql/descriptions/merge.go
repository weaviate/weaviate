//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

// Package descriptions provides the descriptions as used by the graphql endpoint for Weaviate
package descriptions

// Local
const (
	LocalMergeObj = "An object used to Merge Objects on a local Weaviate"
	LocalMerge    = "Merge Objects on a local Weaviate"
)

const LocalMergeClassUUID = "The UUID of a Object, assigned by its local Weaviate"

// Network
const (
	NetworkMerge    = "Merge Objects from a Weaviate in a network"
	NetworkMergeObj = "An object used to Merge Objects from a Weaviate in a network"
)

const NetworkMergeWeaviateObj = "An object containing Merge Objects fields for network Weaviate instance: "

const NetworkMergeClassUUID = "The UUID of a Thing or Action, assigned by the Weaviate network" // TODO check this with @lauraham

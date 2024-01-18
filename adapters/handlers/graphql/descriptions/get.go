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
	GetObjects = "Get Objects on a local Weaviate"
)

const (
	GetObj = "An object used to Get Objects on a local Weaviate"
	Get    = "Get Objects on a local Weaviate"
)

const GetObjectsActionsObj = "An object used to get %ss on a local Weaviate"

const GetClassUUID = "The UUID of a Object, assigned by its local Weaviate"

// Network
const (
	NetworkGet    = "Get Objects from a Weaviate in a network"
	NetworkGetObj = "An object used to Get Objects from a Weaviate in a network"
)

const NetworkGetWeaviateObj = "An object containing Get Objects fields for network Weaviate instance: "

const (
	NetworkGetObjects = "Get Objects from a Weaviate in a network"
)

const (
	NetworkGetObjectsObj = "An object containing the Objects objects on this network Weaviate instance."
)

const NetworkGetClassUUID = "The UUID of a Object, assigned by the Weaviate network" // TODO check this with @lauraham

const ConsistencyLevel = "Determines how many replicas must acknowledge a request " +
	"before it is considered successful. Can be 'ONE', 'QUORUM', or 'ALL'"

const Tenant = "The value by which a tenant is identified, specified in the class schema"

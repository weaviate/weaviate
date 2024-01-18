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

// NETWORK
const (
	NetworkIntrospect    = "Get Introspection information about Objects and/or Beacons in a Weaviate network"
	NetworkIntrospectObj = "An object used to perform an Introspection query on a Weaviate network"
)

const (
	NetworkIntrospectWeaviate  = "The Weaviate instance that the current Object or Beacon belongs to"
	NetworkIntrospectClassName = "The name of the current Object or Beacon's class"
	NetworkIntrospectCertainty = "The degree of similarity between a(n) Object or Beacon and the filter input"
)

const (
	NetworkIntrospectBeaconProperties             = "The properties of a Beacon"
	NetworkIntrospectBeaconPropertiesPropertyName = "The names of the properties of a Beacon"
)

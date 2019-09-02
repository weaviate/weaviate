//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

// Package descriptions provides the descriptions as used by the graphql endpoint for Weaviate
package descriptions

// NETWORK
const NetworkIntrospect = "Get Introspection information about Things, Actions and/or Beacons in a Weaviate network"
const NetworkIntrospectObj = "An object used to perform an Introspection query on a Weaviate network"

const NetworkIntrospectActions = "Introspect Actions in a Weaviate network"
const NetworkIntrospectThings = "Introspect Things in a Weaviate network"
const NetworkIntrospectBeacon = "Introspect Beacons in a Weaviate network"

const NetworkIntrospectWeaviate = "The Weaviate instance that the current Thing, Action or Beacon belongs to"
const NetworkIntrospectClassName = "The name of the current Thing, Action or Beacon's class"
const NetworkIntrospectCertainty = "The degree of similarity between a(n) Thing, Action or Beacon and the filter input"

const NetworkIntrospectActionsObj = "An object used to Introspect Actions on a Weaviate network"
const NetworkIntrospectThingsObj = "An object used to Introspect Things on a Weaviate network"
const NetworkIntrospectBeaconObj = "An object used to Introspect Beacons on a Weaviate network"

const NetworkIntrospectBeaconProperties = "The properties of a Beacon"
const NetworkIntrospectBeaconPropertiesPropertyName = "The names of the properties of a Beacon"

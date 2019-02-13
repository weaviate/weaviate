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

import ()

// NETWORK
const NetworkIntrospect string = "Get Introspection information about Things, Actions and/or Beacons in a Weaviate network"
const NetworkIntrospectObj string = "An object used to perform an Introspection query on a Weaviate network"

const NetworkIntrospectActions string = "Introspect Actions in a Weaviate network"
const NetworkIntrospectThings string = "Introspect Things in a Weaviate network"
const NetworkIntrospectBeacon string = "Introspect Beacons in a Weaviate network"

const NetworkIntrospectWeaviate string = "The Weaviate instance that the current Thing, Action or Beacon belongs to"
const NetworkIntrospectClassName string = "The name of the current Thing, Action or Beacon's class"
const NetworkIntrospectCertainty string = "The degree of similarity between a(n) Thing, Action or Beacon and the filter input"

const NetworkIntrospectActionsObj string = "An object used to Introspect Actions on a Weaviate network"
const NetworkIntrospectThingsObj string = "An object used to Introspect Things on a Weaviate network"
const NetworkIntrospectBeaconObj string = "An object used to Introspect Beacons on a Weaviate network"

const NetworkIntrospectBeaconProperties string = "The properties of a Beacon"
const NetworkIntrospectBeaconPropertiesPropertyName string = "The names of the properties of a Beacon"

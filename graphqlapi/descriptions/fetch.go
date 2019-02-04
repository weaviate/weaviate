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
const NetworkFetchDesc string = "Fetch Beacons that are similar to a specified concept from the Things and/or Actions subsets on a Weaviate network"
const NetworkFetchObjDesc string = "An object used to perform a Fuzzy Fetch to search for Things and Actions similar to a specified concept on a Weaviate network"

const NetworkFetchActionsDesc string = "Perform a Fuzzy Fetch to Fetch Beacons similar to a specified concept on a Weaviate network from the Actions subset"
const NetworkFetchThingsDesc string = "Perform a Fuzzy Fetch to Fetch Beacons similar to a specified concept on a Weaviate network from the Things subset"
const NetworkFetchFuzzyDesc string = "Perform a Fuzzy Fetch to Fetch Beacons similar to a specified concept on a Weaviate network from both the Things and Actions subsets"

const NetworkFetchActionBeaconDesc string = "A Beacon result from a network Weaviate Fetch query on the Actions subset"
const NetworkFetchActionCertaintyDesc string = "The degree of similarity on a scale of 0-1 between the Beacon's characteristics and the provided concept"
const NetworkFetchActionsObjDesc string = "An object used to Fetch Beacons from the Actions subset of the dataset"

const NetworkFetchThingBeaconDesc string = "A Beacon result from a network Weaviate Fetch query on the Things subset"
const NetworkFetchThingCertaintyDesc string = "The degree of similarity on a scale of 0-1 between the Beacon's characteristics and the provided concept"
const NetworkFetchThingsObjDesc string = "An object used to Fetch Beacons from the Things subset of the dataset"

const NetworkFetchFuzzyBeaconDesc string = "A Beacon result from a network Weaviate Fetch Fuzzy query from both the Things and Actions subsets"
const NetworkFetchFuzzyCertaintyDesc string = "The degree of similarity on a scale of 0-1 between the Beacon's characteristics and the provided concept"
const NetworkFetchFuzzyObjDesc string = "An object used to Fetch Beacons from both the Things and Actions subsets"

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

// Local
const LocalFetch string = "Fetch Beacons that are similar to a specified concept from the Things and/or Actions subsets on a Weaviate network"
const LocalFetchObj string = "An object used to perform a Fuzzy Fetch to search for Things and Actions similar to a specified concept on a Weaviate network"

const LocalFetchActions string = "Perform a Fuzzy Fetch to Fetch Beacons similar to a specified concept on a Weaviate network from the Actions subset"
const LocalFetchThings string = "Perform a Fuzzy Fetch to Fetch Beacons similar to a specified concept on a Weaviate network from the Things subset"
const LocalFetchFuzzy string = "Perform a Fuzzy Fetch to Fetch Beacons similar to a specified concept on a Weaviate network from both the Things and Actions subsets"

const LocalFetchBeacon string = "A Beacon result from a local Weaviate Local Fetch query"
const LocalFetchClassName string = "The class name of the result from a local Weaviate Local Fetch query"
const LocalFetchCertainty string = "The degree of similarity on a scale of 0-1 between the Beacon's characteristics and the provided concept"
const LocalFetchActionsObj string = "An object used to Fetch Beacons from the Actions subset of the dataset"

const LocalFetchThingsObj string = "An object used to Fetch Beacons from the Things subset of the dataset"

const LocalFetchFuzzyBeacon string = "A Beacon result from a local Weaviate Fetch Fuzzy query from both the Things and Actions subsets"
const LocalFetchFuzzyClassName string = "Class name of the result from a local Weaviate Fetch Fuzzy query from both the Things and Actions subsets"
const LocalFetchFuzzyCertainty string = "The degree of similarity on a scale of 0-1 between the Beacon's characteristics and the provided concept"
const LocalFetchFuzzyObj string = "An object used to Fetch Beacons from both the Things and Actions subsets"

// NETWORK
const NetworkFetch string = "Fetch Beacons that are similar to a specified concept from the Things and/or Actions subsets on a Weaviate network"
const NetworkFetchObj string = "An object used to perform a Fuzzy Fetch to search for Things and Actions similar to a specified concept on a Weaviate network"

const NetworkFetchActions string = "Perform a Fuzzy Fetch to Fetch Beacons similar to a specified concept on a Weaviate network from the Actions subset"
const NetworkFetchThings string = "Perform a Fuzzy Fetch to Fetch Beacons similar to a specified concept on a Weaviate network from the Things subset"
const NetworkFetchFuzzy string = "Perform a Fuzzy Fetch to Fetch Beacons similar to a specified concept on a Weaviate network from both the Things and Actions subsets"

const NetworkFetchActionClassName string = "Class name of the result from a network Weaviate Fetch query on the Actions subset"
const NetworkFetchActionBeacon string = "A Beacon result from a network Weaviate Fetch query on the Actions subset"
const NetworkFetchActionCertainty string = "The degree of similarity on a scale of 0-1 between the Beacon's characteristics and the provided concept"
const NetworkFetchActionsObj string = "An object used to Fetch Beacons from the Actions subset of the dataset"

const NetworkFetchThingClassName string = "Class name of the result from a network Weaviate Fetch query on the Things subset"
const NetworkFetchThingBeacon string = "A Beacon result from a network Weaviate Fetch query on the Things subset"
const NetworkFetchThingCertainty string = "The degree of similarity on a scale of 0-1 between the Beacon's characteristics and the provided concept"
const NetworkFetchThingsObj string = "An object used to Fetch Beacons from the Things subset of the dataset"

const NetworkFetchFuzzyClassName string = "The class name of the result from a network Weaviate Fetch Fuzzy query from both the Things and Actions subsets"
const NetworkFetchFuzzyBeacon string = "A Beacon result from a network Weaviate Fetch Fuzzy query from both the Things and Actions subsets"
const NetworkFetchFuzzyCertainty string = "The degree of similarity on a scale of 0-1 between the Beacon's characteristics and the provided concept"
const NetworkFetchFuzzyObj string = "An object used to Fetch Beacons from both the Things and Actions subsets"

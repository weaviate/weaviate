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

// Local
const LocalFetch = "Fetch Beacons that are similar to a specified concept from the Things and/or Actions subsets on a Weaviate network"
const LocalFetchObj = "An object used to perform a Fuzzy Fetch to search for Things and Actions similar to a specified concept on a Weaviate network"

const LocalFetchActions = "Perform a Fuzzy Fetch to Fetch Beacons similar to a specified concept on a Weaviate network from the Actions subset"
const LocalFetchThings = "Perform a Fuzzy Fetch to Fetch Beacons similar to a specified concept on a Weaviate network from the Things subset"
const LocalFetchFuzzy = "Perform a Fuzzy Fetch to Fetch Beacons similar to a specified concept on a Weaviate network from both the Things and Actions subsets"

const LocalFetchBeacon = "A Beacon result from a local Weaviate Local Fetch query"
const LocalFetchClassName = "The class name of the result from a local Weaviate Local Fetch query"
const LocalFetchCertainty = "The degree of similarity on a scale of 0-1 between the Beacon's characteristics and the provided concept"
const LocalFetchActionsObj = "An object used to Fetch Beacons from the Actions subset of the dataset"

const LocalFetchThingsObj = "An object used to Fetch Beacons from the Things subset of the dataset"

const LocalFetchFuzzyBeacon = "A Beacon result from a local Weaviate Fetch Fuzzy query from both the Things and Actions subsets"
const LocalFetchFuzzyClassName = "Class name of the result from a local Weaviate Fetch Fuzzy query from both the Things and Actions subsets"
const LocalFetchFuzzyCertainty = "The degree of similarity on a scale of 0-1 between the Beacon's characteristics and the provided concept"
const LocalFetchFuzzyObj = "An object used to Fetch Beacons from both the Things and Actions subsets"

// NETWORK
const NetworkFetch = "Fetch Beacons that are similar to a specified concept from the Things and/or Actions subsets on a Weaviate network"
const NetworkFetchObj = "An object used to perform a Fuzzy Fetch to search for Things and Actions similar to a specified concept on a Weaviate network"

const NetworkFetchActions = "Perform a Fuzzy Fetch to Fetch Beacons similar to a specified concept on a Weaviate network from the Actions subset"
const NetworkFetchThings = "Perform a Fuzzy Fetch to Fetch Beacons similar to a specified concept on a Weaviate network from the Things subset"
const NetworkFetchFuzzy = "Perform a Fuzzy Fetch to Fetch Beacons similar to a specified concept on a Weaviate network from both the Things and Actions subsets"

const NetworkFetchActionClassName = "Class name of the result from a network Weaviate Fetch query on the Actions subset"
const NetworkFetchActionBeacon = "A Beacon result from a network Weaviate Fetch query on the Actions subset"
const NetworkFetchActionCertainty = "The degree of similarity on a scale of 0-1 between the Beacon's characteristics and the provided concept"
const NetworkFetchActionsObj = "An object used to Fetch Beacons from the Actions subset of the dataset"

const NetworkFetchThingClassName = "Class name of the result from a network Weaviate Fetch query on the Things subset"
const NetworkFetchThingBeacon = "A Beacon result from a network Weaviate Fetch query on the Things subset"
const NetworkFetchThingCertainty = "The degree of similarity on a scale of 0-1 between the Beacon's characteristics and the provided concept"
const NetworkFetchThingsObj = "An object used to Fetch Beacons from the Things subset of the dataset"

const NetworkFetchFuzzyClassName = "The class name of the result from a network Weaviate Fetch Fuzzy query from both the Things and Actions subsets"
const NetworkFetchFuzzyBeacon = "A Beacon result from a network Weaviate Fetch Fuzzy query from both the Things and Actions subsets"
const NetworkFetchFuzzyCertainty = "The degree of similarity on a scale of 0-1 between the Beacon's characteristics and the provided concept"
const NetworkFetchFuzzyObj = "An object used to Fetch Beacons from both the Things and Actions subsets"

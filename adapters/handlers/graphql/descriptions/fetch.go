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
	LocalFetch    = "Fetch Beacons that are similar to a specified concept from the Objects subsets on a Weaviate network"
	LocalFetchObj = "An object used to perform a Fuzzy Fetch to search for Objects and Actions similar to a specified concept on a Weaviate network"
)

const (
	LocalFetchObjects = "Perform a Fuzzy Fetch to Fetch Beacons similar to a specified concept on a Weaviate network from the Objects subset"
	LocalFetchFuzzy   = "Perform a Fuzzy Fetch to Fetch Beacons similar to a specified concept on a Weaviate network from both the Objects subsets"
)

const (
	LocalFetchBeacon     = "A Beacon result from a local Weaviate Local Fetch query"
	LocalFetchClassName  = "The class name of the result from a local Weaviate Local Fetch query"
	LocalFetchCertainty  = "The degree of similarity on a scale of 0-1 between the Beacon's characteristics and the provided concept"
	LocalFetchActionsObj = "An object used to Fetch Beacons from the Actions subset of the dataset"
)

const (
	LocalFetchFuzzyBeacon    = "A Beacon result from a local Weaviate Fetch Fuzzy query from both the Objects subsets"
	LocalFetchFuzzyClassName = "Class name of the result from a local Weaviate Fetch Fuzzy query from both the Objects subsets"
	LocalFetchFuzzyCertainty = "The degree of similarity on a scale of 0-1 between the Beacon's characteristics and the provided concept"
	LocalFetchFuzzyObj       = "An object used to Fetch Beacons from both the Objects subsets"
)

// NETWORK
const (
	NetworkFetch    = "Fetch Beacons that are similar to a specified concept from the Objects subsets on a Weaviate network"
	NetworkFetchObj = "An object used to perform a Fuzzy Fetch to search for Objects similar to a specified concept on a Weaviate network"
)

const (
	NetworkFetchFuzzy = "Perform a Fuzzy Fetch to Fetch Beacons similar to a specified concept on a Weaviate network from both the Objects subsets"
)

const (
	NetworkFetchFuzzyClassName = "The class name of the result from a network Weaviate Fetch Fuzzy query from both the Objects subsets"
	NetworkFetchFuzzyBeacon    = "A Beacon result from a network Weaviate Fetch Fuzzy query from both the Objects subsets"
	NetworkFetchFuzzyCertainty = "The degree of similarity on a scale of 0-1 between the Beacon's characteristics and the provided concept"
	NetworkFetchFuzzyObj       = "An object used to Fetch Beacons from both the Objects subsets"
)

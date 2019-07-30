//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

// Package descriptions provides the descriptions as used by the graphql endpoint for Weaviate
package descriptions

// ROOT
const WeaviateObj string = "The location of the root query"
const WeaviateLocal string = "Query a local Weaviate"
const WeaviateNetwork string = "Query a Weaviate network"

// LOCAL
const LocalObj string = "A query on a local Weaviate"

// NETWORK
const NetworkWeaviate string = "An object for the network Weaviate instance: "
const NetworkObj string = "An object used to perform queries on a Weaviate network"

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

package common

import "fmt"

// SubQuery is an extracted query from the Network Query,
// it is intended for exactly one target instance and is
// formatted in a way where it can easily be transformed
// into a Local Query to be used with the remote instance's
// GraphQL API
type SubQuery string

// ParseSubQuery from a []byte
func ParseSubQuery(subQuery []byte) SubQuery {
	return SubQuery(string(subQuery))
}

// WrapInLocalQuery assumes the subquery can be sent as part of a
// Local-Query, i.e. it should start with `Get{ ... }`
func (s SubQuery) WrapInLocalQuery() SubQuery {
	return SubQuery(fmt.Sprintf("Local { %s }", s))
}

// WrapInFetchQuery is helpful for network fetch operations where we extract
// the query at the Things/Action level to distuingish it from Fuzzy. We
// therefore need to re-add the Fetch part.
func (s SubQuery) WrapInFetchQuery() SubQuery {
	return SubQuery(fmt.Sprintf("Fetch { %s }", s))
}

// WrapInBraces can be used to turn the subquery fragments into a root query
func (s SubQuery) WrapInBraces() SubQuery {
	return SubQuery(fmt.Sprintf("{ %s }", s))
}

// String representation of the query
func (s SubQuery) String() string {
	return string(s)
}

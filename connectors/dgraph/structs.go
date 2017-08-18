/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 Weaviate. All rights reserved.
 * LICENSE: https://github.com/weaviate/weaviate/blob/master/LICENSE
 * AUTHOR: Bob van Luijt (bob@weaviate.com)
 * See www.weaviate.com for details
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */

package dgraph

import ()

// DgraphClass is a representation of a class within the Dgraph Database.
type DgraphClass struct {
	Class string `dgraph:"class"`
	ID    uint64 `dgraph:"_uid_"`
}

// ClassResult the root of a Dgraph result a class
type ClassResult struct {
	Root *DgraphClass `dgraph:"class"`
}

// AllClassesResult the root of a Dgraph result with classes
type AllClassesResult struct {
	Root []*DgraphClass `dgraph:"classes"`
}

// TotalResultsResult is the root of a single node with the result count
type TotalResultsResult struct {
	Root Count `dgraph:"totalResults"`
}

// Count is the struct for the counter
type Count struct {
	Count int64 `dgraph:"count"`
}

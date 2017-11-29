/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@weaviate.com)
 * See www.weaviate.com for details
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */

package dgraph

import "github.com/go-openapi/strfmt"

// TotalResultsResult is the root of a single node with the result count
type TotalResultsResult struct {
	Root Count `dgraph:"totalResults"`
}

// TotalResultsRelatedResult is the root of a single node with the result count
type TotalResultsRelatedResult struct {
	Root RelatedCount `dgraph:"totalResults"`
}

// RelatedCount is the root of a single node with the result count
type RelatedCount struct {
	Related Count `dgraph:"related"`
}

// Count is the struct for the counter
type Count struct {
	Count int64 `dgraph:"count"`
}

// NodeIDResult is the struct for getting the thing with an UUID
type NodeIDResult struct {
	Root NodeID `dgraph:"node"`
}

// NodeID to get node-uid
type NodeID struct {
	UUID strfmt.UUID `dgraph:"uuid"`
	ID   uint64      `dgraph:"_uid_"`
	Type string      `dgraph:"_type_"`
}

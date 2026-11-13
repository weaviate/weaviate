//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package modulecapabilities

import "github.com/weaviate/weaviate/entities/schema"

// RerankPropertyDataTypes is implemented by a reranker module that reads only
// some property data types. The gRPC search rejects a rerank about a property
// of another type, or about a property the collection does not have, before
// the search. An empty list accepts every property, and the check does not
// run for GraphQL. Declared or not, a module only receives the value of a
// text, blob, blobHash, date, uuid or deprecated string property; any other
// type reaches it as an empty string.
type RerankPropertyDataTypes interface {
	RerankPropertyDataTypes() []schema.DataType
}

// RerankQueryValidator is implemented by a reranker module that rejects some
// values of rerank.query, such as an empty one. The gRPC search calls it
// before the search, so the request fails whether or not the search returns
// objects: the rank call does not run for an empty result set. A rerank
// without a query reaches it as an empty string. The check does not run for
// GraphQL, so the module's rank call must apply the same rule.
type RerankQueryValidator interface {
	ValidateRerankQuery(query string) error
}

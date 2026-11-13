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

// RerankQueryRequired is implemented by a reranker module that cannot rank
// without rerank.query. The gRPC search rejects a rerank without a query
// before the search, so the request fails whether or not the search returns
// objects. A module that returns false accepts a rerank without a query, and
// the check does not run for GraphQL.
type RerankQueryRequired interface {
	RerankQueryRequired() bool
}

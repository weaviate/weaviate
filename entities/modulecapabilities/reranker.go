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
// some property data types. The gRPC search rejects a rerank about a
// property of another type before the search. A reranker without it receives
// every property as text, as before.
type RerankPropertyDataTypes interface {
	RerankPropertyDataTypes() []schema.DataType
}

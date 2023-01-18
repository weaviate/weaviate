//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package local

import (
	"github.com/weaviate/weaviate/adapters/handlers/graphql/local/aggregate"
	get "github.com/weaviate/weaviate/adapters/handlers/graphql/local/get"
)

// Resolver for local GraphQL queries
type Resolver interface {
	get.Resolver
	aggregate.Resolver
}

//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
//  DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package local

import (
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/local/aggregate"
	get "github.com/semi-technologies/weaviate/adapters/handlers/graphql/local/get"
	getmeta "github.com/semi-technologies/weaviate/adapters/handlers/graphql/local/getmeta"
)

// Resolver for local GraphQL queries
type Resolver interface {
	get.Resolver
	getmeta.Resolver
	aggregate.Resolver
}

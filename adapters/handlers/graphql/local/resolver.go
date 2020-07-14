//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package local

import (
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/local/aggregate"
	get "github.com/semi-technologies/weaviate/adapters/handlers/graphql/local/get"
)

// Resolver for local GraphQL queries
type Resolver interface {
	get.Resolver
	aggregate.Resolver
}

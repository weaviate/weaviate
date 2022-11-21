//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package aggregate

import (
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/local/common_filters"
	"github.com/tailor-inc/graphql"
)

func nearVectorArgument(className string) *graphql.ArgumentConfig {
	return common_filters.NearVectorArgument("AggregateObjects", className)
}

func nearObjectArgument(className string) *graphql.ArgumentConfig {
	return common_filters.NearObjectArgument("AggregateObjects", className)
}

//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package modulecomponents

import (
	"fmt"

	"github.com/tailor-inc/graphql"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/usecases/modulecomponents/arguments/nearImage"
	"github.com/weaviate/weaviate/usecases/modulecomponents/arguments/nearText"
)

type ArgumentType int

const (
	Get ArgumentType = iota
	Aggregate
	Explore
)

func GetGenericArgument(name, className string, argumentType ArgumentType) *graphql.ArgumentConfig {
	switch name {
	case "nearText":
		return getGenericArgument(nearText.New(nil).Arguments()[name], className, argumentType)
	case "nearImage":
		return getGenericArgument(nearImage.New().Arguments()[name], className, argumentType)
	default:
		panic(fmt.Sprintf("Unknown generic argument: %s", name))
	}
}

func getGenericArgument(arg modulecapabilities.GraphQLArgument,
	className string, argumentType ArgumentType,
) *graphql.ArgumentConfig {
	switch argumentType {
	case Get:
		return arg.GetArgumentsFunction(className)
	case Aggregate:
		return arg.AggregateArgumentsFunction(className)
	case Explore:
		return arg.ExploreArgumentsFunction()
	default:
		panic(fmt.Sprintf("Unknown argument type: %v", argumentType))
	}
}

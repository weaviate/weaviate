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
	"github.com/weaviate/weaviate/usecases/modulecomponents/additional"
	"github.com/weaviate/weaviate/usecases/modulecomponents/arguments/nearAudio"
	"github.com/weaviate/weaviate/usecases/modulecomponents/arguments/nearDepth"
	"github.com/weaviate/weaviate/usecases/modulecomponents/arguments/nearImage"
	"github.com/weaviate/weaviate/usecases/modulecomponents/arguments/nearImu"
	"github.com/weaviate/weaviate/usecases/modulecomponents/arguments/nearText"
	"github.com/weaviate/weaviate/usecases/modulecomponents/arguments/nearThermal"
	"github.com/weaviate/weaviate/usecases/modulecomponents/arguments/nearVideo"
)

type ArgumentType int

const (
	Get ArgumentType = iota
	Aggregate
	Explore
)

func GetGenericArgument(name, className string, argumentType ArgumentType,
	nearTextTransformer modulecapabilities.TextTransform,
) *graphql.ArgumentConfig {
	switch name {
	case nearText.NAME:
		return getGenericArgument(nearText.New(nearTextTransformer).Arguments()[name], className, argumentType)
	case nearImage.NAME:
		return getGenericArgument(nearImage.New().Arguments()[name], className, argumentType)
	case nearAudio.NAME:
		return getGenericArgument(nearAudio.New().Arguments()[name], className, argumentType)
	case nearDepth.NAME:
		return getGenericArgument(nearDepth.New().Arguments()[name], className, argumentType)
	case nearImu.NAME:
		return getGenericArgument(nearImu.New().Arguments()[name], className, argumentType)
	case nearThermal.NAME:
		return getGenericArgument(nearThermal.New().Arguments()[name], className, argumentType)
	case nearVideo.NAME:
		return getGenericArgument(nearVideo.New().Arguments()[name], className, argumentType)
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

func GetGenericAdditionalProperty(name, className string) *modulecapabilities.AdditionalProperty {
	switch name {
	case "featureProjection":
		fp := additional.NewText2VecProvider().AdditionalProperties()["featureProjection"]
		return &fp
	default:
		return nil
	}
}

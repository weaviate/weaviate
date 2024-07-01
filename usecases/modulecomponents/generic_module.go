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

	"github.com/sirupsen/logrus"
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

const AdditionalPropertyGenerate = additional.PropertyGenerate

func GetGenericArgument(name, className string, argumentType ArgumentType,
	nearTextTransformer modulecapabilities.TextTransform,
) *graphql.ArgumentConfig {
	switch name {
	case nearText.Name:
		return getGenericArgument(nearText.New(nearTextTransformer).Arguments()[name], className, argumentType)
	case nearImage.Name:
		return getGenericArgument(nearImage.New().Arguments()[name], className, argumentType)
	case nearAudio.Name:
		return getGenericArgument(nearAudio.New().Arguments()[name], className, argumentType)
	case nearDepth.Name:
		return getGenericArgument(nearDepth.New().Arguments()[name], className, argumentType)
	case nearImu.Name:
		return getGenericArgument(nearImu.New().Arguments()[name], className, argumentType)
	case nearThermal.Name:
		return getGenericArgument(nearThermal.New().Arguments()[name], className, argumentType)
	case nearVideo.Name:
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
	case additional.PropertyFeatureProjection:
		fp := additional.NewText2VecProvider().AdditionalProperties()[additional.PropertyFeatureProjection]
		return &fp
	default:
		return nil
	}
}

func GetGenericGenerateProperty(
	className string,
	additionalGenerativeParameters map[string]modulecapabilities.GenerativeProperty,
	defaultProviderName string,
	logger logrus.FieldLogger,
) *modulecapabilities.AdditionalProperty {
	generate := additional.NewGenericGenerativeProvider(className, additionalGenerativeParameters, defaultProviderName, logger).AdditionalProperties()[additional.PropertyGenerate]
	return &generate
}

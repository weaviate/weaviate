//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package nearImage

import (
	"fmt"

	"github.com/graphql-go/graphql"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/descriptions"
)

func getNearImageArgumentFn(classname string) *graphql.ArgumentConfig {
	return nearImageArgument("GetObjects", classname)
}

func exploreNearImageArgumentFn() *graphql.ArgumentConfig {
	return nearImageArgument("Explore", "")
}

func nearImageArgument(prefix, className string) *graphql.ArgumentConfig {
	prefixName := fmt.Sprintf("Img2VecKeras%s%s", prefix, className)
	return &graphql.ArgumentConfig{
		Type: graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name:        fmt.Sprintf("%sNearImageInpObj", prefixName),
				Fields:      nearImageFields(prefixName),
				Description: descriptions.GetWhereInpObj,
			},
		),
	}
}

func nearImageFields(prefix string) graphql.InputObjectConfigFieldMap {
	return graphql.InputObjectConfigFieldMap{
		"image": &graphql.InputObjectFieldConfig{
			Description: "Base64 encoded image",
			Type:        graphql.NewNonNull(graphql.String),
		},
		"certainty": &graphql.InputObjectFieldConfig{
			Description: descriptions.Certainty,
			Type:        graphql.Float,
		},
	}
}

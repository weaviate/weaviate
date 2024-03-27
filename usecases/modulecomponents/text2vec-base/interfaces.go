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

package text2vecBase

import (
	"context"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
)

type TextVectorizer interface {
	Object(ctx context.Context, object *models.Object,
		cfg moduletools.ClassConfig) ([]float32, models.AdditionalProperties, error)
	Texts(ctx context.Context, input []string,
		cfg moduletools.ClassConfig) ([]float32, error)
}

type TextVectorizerBatch interface {
	TextVectorizer
	ObjectBatch(ctx context.Context, objects []*models.Object, skipObject []bool, cfg moduletools.ClassConfig) ([][]float32, map[int]error)
}

type MetaProvider interface {
	MetaInfo() (map[string]interface{}, error)
}

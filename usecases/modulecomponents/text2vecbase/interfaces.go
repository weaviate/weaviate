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

package text2vecbase

import (
	"context"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
	"github.com/weaviate/weaviate/usecases/modulecomponents/batch"
	objectsvectorizer "github.com/weaviate/weaviate/usecases/modulecomponents/vectorizer"
)

type TextVectorizer interface {
	Object(ctx context.Context, object *models.Object,
		cfg moduletools.ClassConfig) ([]float32, models.AdditionalProperties, error)
	Texts(ctx context.Context, input []string,
		cfg moduletools.ClassConfig) ([]float32, error)
}

type TextVectorizerBatch interface {
	Texts(ctx context.Context, input []string,
		cfg moduletools.ClassConfig) ([]float32, error)
	Object(ctx context.Context, object *models.Object,
		cfg moduletools.ClassConfig, cs objectsvectorizer.ClassSettings) ([]float32, models.AdditionalProperties, error)
	ObjectBatch(ctx context.Context, objects []*models.Object, skipObject []bool, cfg moduletools.ClassConfig) ([][]float32, map[int]error)
}

type MetaProvider interface {
	MetaInfo() (map[string]interface{}, error)
}

type BatchVectorizer struct {
	client           BatchClient
	objectVectorizer *objectsvectorizer.ObjectVectorizer
	batchVectorizer  *batch.Batch
	tokenizerFunc    tokenizerFuncType
}

type BatchClient interface {
	batch.BatchClient
	VectorizeQuery(ctx context.Context, input []string,
		cfg moduletools.ClassConfig) (*modulecomponents.VectorizationResult, error)
}

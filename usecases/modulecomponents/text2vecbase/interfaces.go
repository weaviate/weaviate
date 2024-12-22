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

	"github.com/liutizhong/weaviate/entities/models"
	"github.com/liutizhong/weaviate/entities/moduletools"
	"github.com/liutizhong/weaviate/entities/types"
	"github.com/liutizhong/weaviate/usecases/modulecomponents"
	"github.com/liutizhong/weaviate/usecases/modulecomponents/batch"
	objectsvectorizer "github.com/liutizhong/weaviate/usecases/modulecomponents/vectorizer"
)

type TextVectorizer[T types.Embedding] interface {
	Object(ctx context.Context, object *models.Object,
		cfg moduletools.ClassConfig) (T, models.AdditionalProperties, error)
	Texts(ctx context.Context, input []string,
		cfg moduletools.ClassConfig) (T, error)
}

type TextVectorizerBatch[T types.Embedding] interface {
	Texts(ctx context.Context, input []string,
		cfg moduletools.ClassConfig) (T, error)
	Object(ctx context.Context, object *models.Object,
		cfg moduletools.ClassConfig, cs objectsvectorizer.ClassSettings) (T, models.AdditionalProperties, error)
	ObjectBatch(ctx context.Context, objects []*models.Object, skipObject []bool, cfg moduletools.ClassConfig) ([]T, map[int]error)
}

type MetaProvider interface {
	MetaInfo() (map[string]interface{}, error)
}

type BatchVectorizer[T types.Embedding] struct {
	client           BatchClient[T]
	objectVectorizer *objectsvectorizer.ObjectVectorizer
	batchVectorizer  *batch.Batch[T]
	tokenizerFunc    batch.TokenizerFuncType
}

type BatchClient[T types.Embedding] interface {
	batch.BatchClient[T]
	VectorizeQuery(ctx context.Context, input []string,
		cfg moduletools.ClassConfig) (*modulecomponents.VectorizationResult[T], error)
}

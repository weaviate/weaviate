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

package vectorizer

import (
	"context"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/text2vec-transformers/ent"
	objectsvectorizer "github.com/weaviate/weaviate/usecases/modulecomponents/vectorizer"
)

type Vectorizer struct {
	client           Client
	objectVectorizer *objectsvectorizer.ObjectVectorizer
}

func New(client Client) *Vectorizer {
	return &Vectorizer{
		client:           client,
		objectVectorizer: objectsvectorizer.New(),
	}
}

type Client interface {
	VectorizeObject(ctx context.Context, input string,
		cfg ent.VectorizationConfig) (*ent.VectorizationResult, error)
	VectorizeQuery(ctx context.Context, input string,
		cfg ent.VectorizationConfig) (*ent.VectorizationResult, error)
}

// IndexCheck returns whether a property of a class should be indexed
type ClassSettings interface {
	PropertyIndexed(property string) bool
	VectorizeClassName() bool
	VectorizePropertyName(propertyName string) bool
	PoolingStrategy() string
}

func (v *Vectorizer) Object(ctx context.Context, object *models.Object,
	objDiff *moduletools.ObjectDiff, cfg moduletools.ClassConfig,
) error {
	vec, err := v.object(ctx, object.Class, object.Properties, objDiff, cfg)
	if err != nil {
		return err
	}

	object.Vector = vec
	return nil
}

func (v *Vectorizer) object(ctx context.Context, className string,
	schema interface{}, objDiff *moduletools.ObjectDiff, cfg moduletools.ClassConfig,
) ([]float32, error) {
	text, vector, err := v.objectVectorizer.TextsOrVector(ctx, className, schema, objDiff, NewClassSettings(cfg))
	if err != nil {
		return nil, err
	}
	if vector != nil {
		// dont' re-vectorize
		return vector, nil
	}
	// vectorize text
	res, err := v.client.VectorizeObject(ctx, text, ent.VectorizationConfig{
		PoolingStrategy: NewClassSettings(cfg).PoolingStrategy(),
	})
	if err != nil {
		return nil, err
	}

	return res.Vector, nil
}

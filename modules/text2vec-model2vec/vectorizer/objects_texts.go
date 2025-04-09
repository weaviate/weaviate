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

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/usecases/modulecomponents/clients/transformers"
	objectsvectorizer "github.com/weaviate/weaviate/usecases/modulecomponents/vectorizer"
	libvectorizer "github.com/weaviate/weaviate/usecases/vectorizer"
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
		cfg transformers.VectorizationConfig) (*transformers.VectorizationResult, error)
	VectorizeQuery(ctx context.Context, input string,
		cfg transformers.VectorizationConfig) (*transformers.VectorizationResult, error)
}

// IndexCheck returns whether a property of a class should be indexed
type ClassSettings interface {
	PropertyIndexed(property string) bool
	VectorizeClassName() bool
	VectorizePropertyName(propertyName string) bool
	PoolingStrategy() string
}

func (v *Vectorizer) Object(ctx context.Context, object *models.Object, cfg moduletools.ClassConfig,
) ([]float32, models.AdditionalProperties, error) {
	vec, err := v.object(ctx, object, cfg)
	return vec, nil, err
}

func (v *Vectorizer) object(ctx context.Context, object *models.Object, cfg moduletools.ClassConfig,
) ([]float32, error) {
	icheck := NewClassSettings(cfg)
	text := v.objectVectorizer.Texts(ctx, object, icheck)
	res, err := v.client.VectorizeObject(ctx, text, v.getVectorizationConfig(cfg))
	if err != nil {
		return nil, err
	}

	return res.Vector, nil
}

func (v *Vectorizer) Texts(ctx context.Context, inputs []string,
	cfg moduletools.ClassConfig,
) ([]float32, error) {
	vectors := make([][]float32, len(inputs))
	for i := range inputs {
		res, err := v.client.VectorizeQuery(ctx, inputs[i], v.getVectorizationConfig(cfg))
		if err != nil {
			return nil, errors.Wrap(err, "remote client vectorize")
		}
		vectors[i] = res.Vector
	}

	return libvectorizer.CombineVectors(vectors), nil
}

func (v *Vectorizer) getVectorizationConfig(cfg moduletools.ClassConfig) transformers.VectorizationConfig {
	settings := NewClassSettings(cfg)
	return transformers.VectorizationConfig{InferenceURL: settings.InferenceURL()}
}

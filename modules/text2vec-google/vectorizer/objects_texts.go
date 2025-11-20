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
	"fmt"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
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
	VectorizeWithTitleProperty(ctx context.Context,
		input []string, titlePropertyValue string, cfg moduletools.ClassConfig,
	) (*modulecomponents.VectorizationResult[[]float32], error)
	VectorizeQuery(ctx context.Context,
		input []string, cfg moduletools.ClassConfig,
	) (*modulecomponents.VectorizationResult[[]float32], error)
}

func (v *Vectorizer) Object(ctx context.Context, object *models.Object, cfg moduletools.ClassConfig,
) ([]float32, models.AdditionalProperties, error) {
	vec, err := v.object(ctx, object, cfg)
	return vec, nil, err
}

func (v *Vectorizer) object(ctx context.Context, object *models.Object, cfg moduletools.ClassConfig,
) ([]float32, error) {
	icheck := NewClassSettings(cfg)
	corpi, titlePropertyValue := v.objectVectorizer.TextsWithTitleProperty(ctx, object, icheck, icheck.TitleProperty())
	// vectorize text
	res, err := v.client.VectorizeWithTitleProperty(ctx, []string{corpi}, titlePropertyValue, cfg)
	if err != nil {
		return nil, err
	}
	if len(res.Vector) == 0 {
		return nil, fmt.Errorf("no vectors generated")
	}

	if len(res.Vector) > 1 {
		return libvectorizer.CombineVectors(res.Vector), nil
	}
	return res.Vector[0], nil
}

func (v *Vectorizer) Texts(ctx context.Context, inputs []string,
	cfg moduletools.ClassConfig,
) ([]float32, error) {
	res, err := v.client.VectorizeQuery(ctx, inputs, cfg)
	if err != nil {
		return nil, fmt.Errorf("remote client vectorize: %w", err)
	}
	return libvectorizer.CombineVectors(res.Vector), nil
}

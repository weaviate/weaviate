//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package vectorizer

import (
	"context"

	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/multi2multivec-weaviate/ent"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
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
	Vectorize(ctx context.Context, images []string, cfg moduletools.ClassConfig) ([][][]float32, error)
	VectorizeQuery(ctx context.Context, texts []string, cfg moduletools.ClassConfig) (*modulecomponents.VectorizationResult[[][]float32], error)
}

type ClassSettings interface {
	ImageField(property string) bool
}

func (v *Vectorizer) Object(ctx context.Context, object *models.Object,
	cfg moduletools.ClassConfig,
) ([][]float32, models.AdditionalProperties, error) {
	vec, err := v.object(ctx, object, cfg)
	return vec, nil, err
}

func (v *Vectorizer) object(ctx context.Context, object *models.Object,
	cfg moduletools.ClassConfig,
) ([][]float32, error) {
	ichek := ent.NewClassSettings(cfg)

	images := []string{}

	if object.Properties != nil {
		schemamap := object.Properties.(map[string]interface{})
		for _, propName := range moduletools.SortStringKeys(schemamap) {
			if val, ok := schemamap[propName].(string); ok {
				if ichek.ImageField(propName) {
					images = append(images, val)
				}
			}
		}
	}

	if len(images) > 0 {
		vectors, err := v.client.Vectorize(ctx, images, cfg)
		if err != nil {
			return nil, err
		}

		if len(vectors) > 1 {
			return nil, errors.Errorf("got more than 1 embedding back: %v", len(vectors))
		}

		return vectors[0], nil
	}

	return nil, errors.New("configured properties don't exist or are not of blob type")
}

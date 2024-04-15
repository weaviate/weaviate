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
	"github.com/weaviate/weaviate/modules/img2vec-neural/ent"
	libvectorizer "github.com/weaviate/weaviate/usecases/vectorizer"
)

type Vectorizer struct {
	client Client
}

func New(client Client) *Vectorizer {
	return &Vectorizer{
		client: client,
	}
}

type Client interface {
	Vectorize(ctx context.Context,
		id, image string) (*ent.VectorizationResult, error)
}

type ClassSettings interface {
	ImageField(property string) bool
	Properties() ([]string, error)
}

func (v *Vectorizer) Object(ctx context.Context, object *models.Object, cfg moduletools.ClassConfig,
) ([]float32, models.AdditionalProperties, error) {
	vec, err := v.object(ctx, object, cfg)
	return vec, nil, err
}

func (v *Vectorizer) VectorizeImage(ctx context.Context, id, image string, cfg moduletools.ClassConfig) ([]float32, error) {
	res, err := v.client.Vectorize(ctx, id, image)
	if err != nil {
		return nil, err
	}

	return res.Vector, nil
}

func (v *Vectorizer) object(ctx context.Context, object *models.Object, cfg moduletools.ClassConfig,
) ([]float32, error) {
	ichek := NewClassSettings(cfg)

	// vectorize image
	images := []string{}

	if object.Properties != nil {
		schemamap := object.Properties.(map[string]interface{})
		for _, propName := range moduletools.SortStringKeys(schemamap) {
			if !ichek.ImageField(propName) {
				continue
			}

			switch val := schemamap[propName].(type) {
			case string:
				images = append(images, val)

			default:
			}

		}
	}

	vectors := [][]float32{}
	for i, image := range images {
		imgID := fmt.Sprintf("%s_%v", object.ID, i)
		vector, err := v.VectorizeImage(ctx, imgID, image, cfg)
		if err != nil {
			return nil, err
		}
		vectors = append(vectors, vector)
	}

	return libvectorizer.CombineVectors(vectors), nil
}

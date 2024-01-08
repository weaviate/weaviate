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

	"github.com/go-openapi/strfmt"
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
}

func (v *Vectorizer) Object(ctx context.Context, object *models.Object,
	objDiff *moduletools.ObjectDiff, settings ClassSettings,
) error {
	vec, err := v.object(ctx, object.ID, object.Properties, objDiff, settings)
	if err != nil {
		return err
	}

	object.Vector = vec
	return nil
}

func (v *Vectorizer) VectorizeImage(ctx context.Context, id, image string) ([]float32, error) {
	res, err := v.client.Vectorize(ctx, id, image)
	if err != nil {
		return nil, err
	}

	return res.Vector, nil
}

func (v *Vectorizer) object(ctx context.Context, id strfmt.UUID,
	schema interface{}, objDiff *moduletools.ObjectDiff, ichek ClassSettings,
) ([]float32, error) {
	vectorize := objDiff == nil || objDiff.GetVec() == nil

	// vectorize image
	images := []string{}
	if schema != nil {
		for prop, value := range schema.(map[string]interface{}) {
			if !ichek.ImageField(prop) {
				continue
			}
			valueString, ok := value.(string)
			if ok {
				images = append(images, valueString)
				vectorize = vectorize || (objDiff != nil && objDiff.IsChangedProp(prop))
			}
		}
	}

	// no property was changed, old vector can be used
	if !vectorize {
		return objDiff.GetVec(), nil
	}

	vectors := [][]float32{}
	for i, image := range images {
		imgID := fmt.Sprintf("%s_%v", id, i)
		vector, err := v.VectorizeImage(ctx, imgID, image)
		if err != nil {
			return nil, err
		}
		vectors = append(vectors, vector)
	}

	return libvectorizer.CombineVectors(vectors), nil
}

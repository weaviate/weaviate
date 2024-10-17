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
	"github.com/weaviate/weaviate/modules/multi2vec-google/ent"
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
	Vectorize(ctx context.Context,
		texts, images, videos []string, config ent.VectorizationConfig) (*ent.VectorizationResult, error)
}

type ClassSettings interface {
	ImageField(property string) bool
	ImageFieldsWeights() ([]float32, error)
	TextField(property string) bool
	TextFieldsWeights() ([]float32, error)
	VideoField(property string) bool
	VideoFieldsWeights() ([]float32, error)
}

func (v *Vectorizer) Object(ctx context.Context, object *models.Object,
	cfg moduletools.ClassConfig,
) ([]float32, models.AdditionalProperties, error) {
	vec, err := v.object(ctx, object, cfg)
	return vec, nil, err
}

func (v *Vectorizer) VectorizeImage(ctx context.Context, id, image string, cfg moduletools.ClassConfig) ([]float32, error) {
	res, err := v.client.Vectorize(ctx, nil, []string{image}, nil, v.getVectorizationConfig(cfg))
	if err != nil {
		return nil, err
	}
	if len(res.ImageVectors) != 1 {
		return nil, errors.New("empty vector")
	}

	return res.ImageVectors[0], nil
}

func (v *Vectorizer) VectorizeVideo(ctx context.Context,
	video string, cfg moduletools.ClassConfig,
) ([]float32, error) {
	res, err := v.client.Vectorize(ctx, nil, nil, []string{video}, v.getVectorizationConfig(cfg))
	if err != nil {
		return nil, err
	}
	if len(res.VideoVectors) != 1 {
		return nil, errors.New("empty vector")
	}

	return res.VideoVectors[0], nil
}

func (v *Vectorizer) object(ctx context.Context, object *models.Object,
	cfg moduletools.ClassConfig,
) ([]float32, error) {
	ichek := NewClassSettings(cfg)

	// vectorize image and text
	texts := []string{}
	images := []string{}
	videos := []string{}

	if object.Properties != nil {
		schemamap := object.Properties.(map[string]interface{})
		for _, propName := range moduletools.SortStringKeys(schemamap) {
			switch val := schemamap[propName].(type) {
			case string:
				if ichek.ImageField(propName) {
					images = append(images, val)
				}
				if ichek.TextField(propName) {
					texts = append(texts, val)
				}
				if ichek.VideoField(propName) {
					videos = append(videos, val)
				}

			case []string:
				if ichek.TextField(propName) {
					texts = append(texts, val...)
				}
			default: // properties that are not part of the object

			}
		}

	}

	vectors := [][]float32{}
	if len(texts) > 0 || len(images) > 0 || len(videos) > 0 {
		res, err := v.client.Vectorize(ctx, texts, images, videos, v.getVectorizationConfig(cfg))
		if err != nil {
			return nil, err
		}
		vectors = append(vectors, res.TextVectors...)
		vectors = append(vectors, res.ImageVectors...)
		vectors = append(vectors, res.VideoVectors...)
	}
	weights, err := v.getWeights(ichek)
	if err != nil {
		return nil, err
	}

	return libvectorizer.CombineVectorsWithWeights(vectors, weights), nil
}

func (v *Vectorizer) getWeights(ichek ClassSettings) ([]float32, error) {
	weights := []float32{}
	textFieldsWeights, err := ichek.TextFieldsWeights()
	if err != nil {
		return nil, err
	}
	imageFieldsWeights, err := ichek.ImageFieldsWeights()
	if err != nil {
		return nil, err
	}
	videoFieldsWeights, err := ichek.VideoFieldsWeights()
	if err != nil {
		return nil, err
	}

	weights = append(weights, textFieldsWeights...)
	weights = append(weights, imageFieldsWeights...)
	weights = append(weights, videoFieldsWeights...)

	normalizedWeights := v.normalizeWeights(weights)

	return normalizedWeights, nil
}

func (v *Vectorizer) normalizeWeights(weights []float32) []float32 {
	if len(weights) > 0 {
		var denominator float32
		for i := range weights {
			denominator += weights[i]
		}
		normalizer := 1 / denominator
		normalized := make([]float32, len(weights))
		for i := range weights {
			normalized[i] = weights[i] * normalizer
		}
		return normalized
	}
	return nil
}

func (v *Vectorizer) getVectorizationConfig(cfg moduletools.ClassConfig) ent.VectorizationConfig {
	settings := NewClassSettings(cfg)
	return ent.VectorizationConfig{
		Location:             settings.Location(),
		ProjectID:            settings.ProjectID(),
		Model:                settings.ModelID(),
		Dimensions:           settings.Dimensions(),
		VideoIntervalSeconds: settings.VideoIntervalSeconds(),
	}
}

//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
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
		texts, images, videos, audios []string, config ent.VectorizationConfig) (*ent.VectorizationResult, error)
}

type ClassSettings interface {
	ImageField(property string) bool
	ImageFieldsWeights() ([]float32, error)
	TextField(property string) bool
	TextFieldsWeights() ([]float32, error)
	VideoField(property string) bool
	VideoFieldsWeights() ([]float32, error)
	AudioField(property string) bool
	AudioFieldsWeights() ([]float32, error)
	Properties() ([]string, error)
}

func (v *Vectorizer) Object(ctx context.Context, object *models.Object,
	cfg moduletools.ClassConfig,
) ([]float32, models.AdditionalProperties, error) {
	vec, err := v.object(ctx, object, cfg)
	return vec, nil, err
}

func (v *Vectorizer) VectorizeImage(ctx context.Context, id, image string, cfg moduletools.ClassConfig) ([]float32, error) {
	res, err := v.client.Vectorize(ctx, nil, []string{image}, nil, nil, v.getVectorizationConfig(cfg))
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
	res, err := v.client.Vectorize(ctx, nil, nil, []string{video}, nil, v.getVectorizationConfig(cfg))
	if err != nil {
		return nil, err
	}
	if len(res.VideoVectors) != 1 {
		return nil, errors.New("empty vector")
	}

	return res.VideoVectors[0], nil
}

func (v *Vectorizer) VectorizeAudio(ctx context.Context,
	audio string, cfg moduletools.ClassConfig,
) ([]float32, error) {
	res, err := v.client.Vectorize(ctx, nil, nil, nil, []string{audio}, v.getVectorizationConfig(cfg))
	if err != nil {
		return nil, err
	}
	if len(res.AudioVectors) != 1 {
		return nil, errors.New("empty vector")
	}

	return res.AudioVectors[0], nil
}

func (v *Vectorizer) object(ctx context.Context, object *models.Object,
	cfg moduletools.ClassConfig,
) ([]float32, error) {
	ichek := NewClassSettings(cfg)

	fieldWeights, err := v.fieldWeights(ichek)
	if err != nil {
		return nil, err
	}

	// vectorize image and text
	texts := []string{}
	images := []string{}
	videos := []string{}
	audios := []string{}
	textWeights := []float32{}
	imageWeights := []float32{}
	videoWeights := []float32{}
	audioWeights := []float32{}

	if object.Properties != nil {
		schemamap := object.Properties.(map[string]any)
		for _, propName := range moduletools.SortStringKeys(schemamap) {
			switch val := schemamap[propName].(type) {
			case string:
				if ichek.ImageField(propName) {
					images = append(images, val)
					if fieldWeights != nil {
						imageWeights = append(imageWeights, fieldWeights[propName])
					}
				}
				if ichek.TextField(propName) {
					texts = append(texts, val)
					if fieldWeights != nil {
						textWeights = append(textWeights, fieldWeights[propName])
					}
				}
				if ichek.VideoField(propName) {
					videos = append(videos, val)
					if fieldWeights != nil {
						videoWeights = append(videoWeights, fieldWeights[propName])
					}
				}
				if ichek.AudioField(propName) {
					audios = append(audios, val)
					if fieldWeights != nil {
						audioWeights = append(audioWeights, fieldWeights[propName])
					}
				}

			case []string:
				if ichek.TextField(propName) {
					texts = append(texts, val...)
					if fieldWeights != nil {
						for range val {
							textWeights = append(textWeights, fieldWeights[propName])
						}
					}
				}
			default: // properties that are not part of the object

			}
		}

	}

	vectors := [][]float32{}
	weights := []float32{}
	if len(texts) > 0 || len(images) > 0 || len(videos) > 0 || len(audios) > 0 {
		res, err := v.client.Vectorize(ctx, texts, images, videos, audios, v.getVectorizationConfig(cfg))
		if err != nil {
			return nil, err
		}
		vectors = append(vectors, res.TextVectors...)
		vectors = append(vectors, res.ImageVectors...)
		vectors = append(vectors, res.VideoVectors...)
		vectors = append(vectors, res.AudioVectors...)
		weights = append(weights, textWeights...)
		weights = append(weights, imageWeights...)
		weights = append(weights, videoWeights...)
		weights = append(weights, audioWeights...)
	}

	return libvectorizer.CombineVectorsWithWeights(vectors, v.normalizeWeights(weights)), nil
}

// fieldWeights maps each configured field name to its weight, keyed by field
// name rather than by position. object() looks weights up by the field that
// produced each vector, instead of assuming the weights and the vectors they
// are meant to scale end up in the same order.
func (v *Vectorizer) fieldWeights(ichek ClassSettings) (map[string]float32, error) {
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
	audioFieldsWeights, err := ichek.AudioFieldsWeights()
	if err != nil {
		return nil, err
	}

	weights := append(append(append(append([]float32{}, textFieldsWeights...), imageFieldsWeights...), videoFieldsWeights...), audioFieldsWeights...)
	if len(weights) == 0 {
		return nil, nil
	}

	properties, err := ichek.Properties()
	if err != nil {
		return nil, err
	}
	if len(properties) != len(weights) {
		// A field category is configured without a matching weights entry.
		// Fall back to an unweighted combination rather than misassigning weights.
		return nil, nil
	}

	fieldWeights := make(map[string]float32, len(properties))
	for i, name := range properties {
		fieldWeights[name] = weights[i]
	}
	return fieldWeights, nil
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
		ApiEndpoint:          settings.ApiEndpoint(),
		Location:             settings.Location(),
		ProjectID:            settings.ProjectID(),
		Model:                settings.Model(),
		Dimensions:           settings.Dimensions(),
		VideoIntervalSeconds: settings.VideoIntervalSeconds(),
	}
}

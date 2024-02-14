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

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/multi2vec-bind/ent"
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
		texts, images, audio, video, imu, thermal, depth []string,
	) (*ent.VectorizationResult, error)
}

type ClassSettings interface {
	ImageField(property string) bool
	ImageFieldsWeights() ([]float32, error)
	TextField(property string) bool
	TextFieldsWeights() ([]float32, error)
	AudioField(property string) bool
	AudioFieldsWeights() ([]float32, error)
	VideoField(property string) bool
	VideoFieldsWeights() ([]float32, error)
	IMUField(property string) bool
	IMUFieldsWeights() ([]float32, error)
	ThermalField(property string) bool
	ThermalFieldsWeights() ([]float32, error)
	DepthField(property string) bool
	DepthFieldsWeights() ([]float32, error)
}

func (v *Vectorizer) Object(ctx context.Context, object *models.Object,
	comp moduletools.VectorizablePropsComparator, cfg moduletools.ClassConfig,
) ([]float32, models.AdditionalProperties, error) {
	vec, err := v.object(ctx, object.ID, comp, cfg)
	return vec, nil, err
}

func (v *Vectorizer) VectorizeImage(ctx context.Context, id, image string, cfg moduletools.ClassConfig) ([]float32, error) {
	res, err := v.client.Vectorize(ctx, nil, []string{image}, nil, nil, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	return v.getVector(res.ImageVectors)
}

func (v *Vectorizer) VectorizeAudio(ctx context.Context, audio string, cfg moduletools.ClassConfig) ([]float32, error) {
	res, err := v.client.Vectorize(ctx, nil, nil, []string{audio}, nil, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	return v.getVector(res.AudioVectors)
}

func (v *Vectorizer) VectorizeVideo(ctx context.Context, video string, cfg moduletools.ClassConfig) ([]float32, error) {
	res, err := v.client.Vectorize(ctx, nil, nil, nil, []string{video}, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	return v.getVector(res.VideoVectors)
}

func (v *Vectorizer) VectorizeIMU(ctx context.Context, imu string, cfg moduletools.ClassConfig) ([]float32, error) {
	res, err := v.client.Vectorize(ctx, nil, nil, nil, nil, []string{imu}, nil, nil)
	if err != nil {
		return nil, err
	}
	return v.getVector(res.IMUVectors)
}

func (v *Vectorizer) VectorizeThermal(ctx context.Context, thermal string, cfg moduletools.ClassConfig) ([]float32, error) {
	res, err := v.client.Vectorize(ctx, nil, nil, nil, nil, nil, []string{thermal}, nil)
	if err != nil {
		return nil, err
	}
	return v.getVector(res.ThermalVectors)
}

func (v *Vectorizer) VectorizeDepth(ctx context.Context, depth string, cfg moduletools.ClassConfig) ([]float32, error) {
	res, err := v.client.Vectorize(ctx, nil, nil, nil, nil, nil, nil, []string{depth})
	if err != nil {
		return nil, err
	}
	return v.getVector(res.DepthVectors)
}

func (v *Vectorizer) getVector(vectors [][]float32) ([]float32, error) {
	if len(vectors) != 1 {
		return nil, errors.New("empty vector")
	}
	return vectors[0], nil
}

func (v *Vectorizer) object(ctx context.Context, id strfmt.UUID,
	comp moduletools.VectorizablePropsComparator, cfg moduletools.ClassConfig,
) ([]float32, error) {
	icheck := NewClassSettings(cfg)
	prevVector := comp.PrevVector()
	if cfg.TargetVector() != "" {
		prevVector = comp.PrevVectorForName(cfg.TargetVector())
	}

	vectorize := prevVector == nil

	// vectorize image and text
	var texts, images, audio, video, imu, thermal, depth []string

	it := comp.PropsIterator()
	for propName, propValue, ok := it.Next(); ok; propName, propValue, ok = it.Next() {
		switch typed := propValue.(type) {
		case string:
			if icheck.ImageField(propName) {
				vectorize = vectorize || comp.IsChanged(propName)
				images = append(images, typed)
			}
			if icheck.TextField(propName) {
				vectorize = vectorize || comp.IsChanged(propName)
				texts = append(texts, typed)
			}
			if icheck.AudioField(propName) {
				vectorize = vectorize || comp.IsChanged(propName)
				audio = append(audio, typed)
			}
			if icheck.VideoField(propName) {
				vectorize = vectorize || comp.IsChanged(propName)
				video = append(video, typed)
			}
			if icheck.IMUField(propName) {
				vectorize = vectorize || comp.IsChanged(propName)
				imu = append(imu, typed)
			}
			if icheck.ThermalField(propName) {
				vectorize = vectorize || comp.IsChanged(propName)
				thermal = append(thermal, typed)
			}
			if icheck.DepthField(propName) {
				vectorize = vectorize || comp.IsChanged(propName)
				depth = append(depth, typed)
			}

		case []string:
			if icheck.TextField(propName) {
				vectorize = vectorize || comp.IsChanged(propName)
				texts = append(texts, typed...)
			}

		case nil:
			if icheck.ImageField(propName) || icheck.TextField(propName) ||
				icheck.AudioField(propName) || icheck.VideoField(propName) ||
				icheck.IMUField(propName) || icheck.ThermalField(propName) ||
				icheck.DepthField(propName) {
				vectorize = vectorize || comp.IsChanged(propName)
			}
		}
	}

	// no property was changed, old vector can be used
	if !vectorize {
		return prevVector, nil
	}

	vectors := [][]float32{}
	if len(texts) > 0 || len(images) > 0 || len(audio) > 0 || len(video) > 0 ||
		len(imu) > 0 || len(thermal) > 0 || len(depth) > 0 {
		res, err := v.client.Vectorize(ctx, texts, images, audio, video, imu, thermal, depth)
		if err != nil {
			return nil, err
		}
		vectors = append(vectors, res.TextVectors...)
		vectors = append(vectors, res.ImageVectors...)
		vectors = append(vectors, res.AudioVectors...)
		vectors = append(vectors, res.VideoVectors...)
		vectors = append(vectors, res.IMUVectors...)
		vectors = append(vectors, res.ThermalVectors...)
		vectors = append(vectors, res.DepthVectors...)
	}
	weights, err := v.getWeights(icheck)
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
	audioFieldsWeights, err := ichek.AudioFieldsWeights()
	if err != nil {
		return nil, err
	}
	videoFieldsWeights, err := ichek.VideoFieldsWeights()
	if err != nil {
		return nil, err
	}
	imuFieldsWeights, err := ichek.IMUFieldsWeights()
	if err != nil {
		return nil, err
	}
	thermalFieldsWeights, err := ichek.ThermalFieldsWeights()
	if err != nil {
		return nil, err
	}
	depthFieldsWeights, err := ichek.DepthFieldsWeights()
	if err != nil {
		return nil, err
	}

	weights = append(weights, textFieldsWeights...)
	weights = append(weights, imageFieldsWeights...)
	weights = append(weights, audioFieldsWeights...)
	weights = append(weights, videoFieldsWeights...)
	weights = append(weights, imuFieldsWeights...)
	weights = append(weights, thermalFieldsWeights...)
	weights = append(weights, depthFieldsWeights...)

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

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

package batchclip

import (
	"context"
	"errors"
	"fmt"

	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
	"github.com/weaviate/weaviate/usecases/modulecomponents/settings"
	libvectorizer "github.com/weaviate/weaviate/usecases/vectorizer"
)

type Vectorizer[T dto.Embedding] interface {
	VectorizeImage(ctx context.Context,
		id, image string, cfg moduletools.ClassConfig,
	) (T, error)
	Texts(ctx context.Context,
		input []string, cfg moduletools.ClassConfig,
	) (T, error)
	Object(ctx context.Context,
		obj *models.Object, cfg moduletools.ClassConfig,
	) (T, models.AdditionalProperties, error)
	Objects(ctx context.Context,
		objs []*models.Object, cfg moduletools.ClassConfig,
	) ([]T, models.AdditionalProperties, error)
}

type Client[T dto.Embedding] interface {
	VectorizeImages(ctx context.Context,
		images []string, cfg moduletools.ClassConfig,
	) (*modulecomponents.VectorizationCLIPResult[T], error)
	VectorizeQuery(ctx context.Context,
		texts []string, cfg moduletools.ClassConfig,
	) (*modulecomponents.VectorizationCLIPResult[T], error)
	Vectorize(ctx context.Context,
		texts, images []string, cfg moduletools.ClassConfig,
	) (*modulecomponents.VectorizationCLIPResult[T], error)
}

type classSettings interface {
	ImageField(property string) bool
	ImageFieldsWeights() ([]float32, error)
	TextField(property string) bool
	TextFieldsWeights() ([]float32, error)
}

type batchObject struct {
	objectIndex  int
	textIndexes  []int
	imageIndexes []int
}

type BatchCLIPVectorizer[T dto.Embedding] struct {
	moduleName     string
	altNames       []string
	lowerCaseInput bool
	client         Client[T]
}

func New[T dto.Embedding](moduleName string, client Client[T]) Vectorizer[T] {
	return NewWithAltNames(moduleName, nil, client)
}

func NewWithAltNames[T dto.Embedding](moduleName string, altNames []string, client Client[T]) Vectorizer[T] {
	return &BatchCLIPVectorizer[T]{
		moduleName:     moduleName,
		altNames:       altNames,
		lowerCaseInput: false,
		client:         client,
	}
}

func (v *BatchCLIPVectorizer[T]) Objects(ctx context.Context,
	objects []*models.Object, cfg moduletools.ClassConfig,
) ([]T, models.AdditionalProperties, error) {
	vecs, err := v.objects(ctx, objects, cfg)
	return vecs, nil, err
}

func (v *BatchCLIPVectorizer[T]) Object(ctx context.Context,
	obj *models.Object, cfg moduletools.ClassConfig,
) (T, models.AdditionalProperties, error) {
	vecs, err := v.objects(ctx, []*models.Object{obj}, cfg)
	if err != nil {
		return nil, nil, err
	}
	if len(vecs) != 1 {
		return nil, nil, fmt.Errorf("more than one embedding found for object: %s", obj.ID)
	}
	return vecs[0], nil, err
}

func (v *BatchCLIPVectorizer[T]) Texts(ctx context.Context,
	inputs []string, cfg moduletools.ClassConfig,
) (T, error) {
	res, err := v.client.Vectorize(ctx, inputs, nil, cfg)
	if err != nil {
		return nil, fmt.Errorf("remote client vectorize: %w", err)
	}
	if len(inputs) != len(res.TextVectors) {
		return nil, errors.New("inputs are not equal to vectors returned")
	}
	vector, err := v.combineVectors(res.TextVectors, nil)
	if err != nil {
		return nil, err
	}
	return vector, nil
}

func (v *BatchCLIPVectorizer[T]) VectorizeImage(ctx context.Context,
	id, image string, cfg moduletools.ClassConfig,
) (T, error) {
	res, err := v.client.VectorizeImages(ctx, []string{image}, cfg)
	if err != nil {
		return nil, err
	}
	if len(res.ImageVectors) != 1 {
		return nil, errors.New("more than one embedding found for image")
	}
	return res.ImageVectors[0], nil
}

func (v *BatchCLIPVectorizer[T]) objects(ctx context.Context, objects []*models.Object,
	cfg moduletools.ClassConfig,
) ([]T, error) {
	icheck := settings.NewBaseClassMultiModalSettingsWithAltNames(
		cfg, v.lowerCaseInput, v.moduleName, v.altNames, nil,
	)

	// vectorize image and text
	textsIndex := 0
	texts := make([]string, 0)
	imagesIndex := 0
	images := make([]string, 0)

	batchObjects := make([]batchObject, 0)
	for i, object := range objects {
		batchObject := batchObject{
			objectIndex:  i,
			textIndexes:  []int{},
			imageIndexes: []int{},
		}
		if object.Properties != nil {
			schemamap := object.Properties.(map[string]any)
			for _, propName := range moduletools.SortStringKeys(schemamap) {
				switch val := schemamap[propName].(type) {
				case string:
					if icheck.ImageField(propName) {
						images = append(images, val)
						batchObject.imageIndexes = append(batchObject.imageIndexes, imagesIndex)
						imagesIndex++
					}
					if icheck.TextField(propName) {
						texts = append(texts, val)
						batchObject.textIndexes = append(batchObject.textIndexes, textsIndex)
						textsIndex++
					}
				case []string:
					if icheck.TextField(propName) {
						for _, textVal := range val {
							texts = append(texts, textVal)
							batchObject.textIndexes = append(batchObject.textIndexes, textsIndex)
							textsIndex++
						}
					}
				default: // properties that are not part of the object

				}
			}
		}
		batchObjects = append(batchObjects, batchObject)
	}

	var result []T
	if len(texts) > 0 || len(images) > 0 {
		res, err := v.client.Vectorize(ctx, texts, images, cfg)
		if err != nil {
			return nil, err
		}
		result = make([]T, len(batchObjects))
		for objIndex, batchObj := range batchObjects {
			vectors := []T{}
			for _, textIndex := range batchObj.textIndexes {
				vec, ok := v.getVector(res.TextVectors, textIndex)
				if !ok {
					return nil, fmt.Errorf("text vector not found for object: %s at index: %v", objects[batchObj.objectIndex].ID, textIndex)
				}
				vectors = append(vectors, vec)
			}
			for _, imageIndex := range batchObj.imageIndexes {
				vec, ok := v.getVector(res.ImageVectors, imageIndex)
				if !ok {
					return nil, fmt.Errorf("image vector not found for object: %s at index: %v", objects[batchObj.objectIndex].ID, imageIndex)
				}
				vectors = append(vectors, vec)
			}
			weights, err := v.getWeights(icheck)
			if err != nil {
				return nil, err
			}
			resultVector, err := v.combineVectors(vectors, weights)
			if err != nil {
				return nil, err
			}
			result[objIndex] = resultVector
		}
	}

	return result, nil
}

func (v *BatchCLIPVectorizer[T]) combineVectors(vectors []T, weights []float32) (T, error) {
	switch any(vectors).(type) {
	case [][]float32:
		return any(libvectorizer.CombineVectorsWithWeights(any(vectors).([][]float32), weights)).(T), nil
	default:
		if len(vectors) > 1 {
			return nil, fmt.Errorf("cannot combine multi vectors, more than one multi vector found: %v", len(vectors))
		}
		return vectors[0], nil
	}
}

func (v *BatchCLIPVectorizer[T]) getVector(vectors []T, index int) (T, bool) {
	if index >= 0 && index < len(vectors) {
		return vectors[index], true
	}
	return nil, false
}

func (v *BatchCLIPVectorizer[T]) getWeights(icheck classSettings) ([]float32, error) {
	weights := []float32{}
	textFieldsWeights, err := icheck.TextFieldsWeights()
	if err != nil {
		return nil, err
	}
	imageFieldsWeights, err := icheck.ImageFieldsWeights()
	if err != nil {
		return nil, err
	}

	weights = append(weights, textFieldsWeights...)
	weights = append(weights, imageFieldsWeights...)

	normalizedWeights := v.normalizeWeights(weights)

	return normalizedWeights, nil
}

func (v *BatchCLIPVectorizer[T]) normalizeWeights(weights []float32) []float32 {
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

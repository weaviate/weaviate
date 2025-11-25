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

package batchtext

import (
	"context"
	"fmt"

	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
	"github.com/weaviate/weaviate/usecases/modulecomponents/settings"
	objectsvectorizer "github.com/weaviate/weaviate/usecases/modulecomponents/vectorizer"
)

type Vectorizer[T dto.Embedding] interface {
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
	Vectorize(ctx context.Context,
		input []string, cfg moduletools.ClassConfig,
	) (*modulecomponents.VectorizationResult[T], *modulecomponents.RateLimits, int, error)
	VectorizeQuery(ctx context.Context,
		input []string, cfg moduletools.ClassConfig,
	) (*modulecomponents.VectorizationResult[T], error)
}

type batchObject struct {
	index   int
	isEmpty bool
}

type BatchTextVectorizer[T dto.Embedding] struct {
	moduleName       string
	altNames         []string
	lowerCaseInput   bool
	client           Client[T]
	objectVectorizer *objectsvectorizer.ObjectVectorizer
}

func New[T dto.Embedding](moduleName string, lowerCaseInput bool, client Client[T]) Vectorizer[T] {
	return NewWithAltNames(moduleName, nil, lowerCaseInput, client)
}

func NewWithAltNames[T dto.Embedding](moduleName string, altNames []string, lowerCaseInput bool, client Client[T]) Vectorizer[T] {
	return &BatchTextVectorizer[T]{
		moduleName:       moduleName,
		altNames:         altNames,
		lowerCaseInput:   lowerCaseInput,
		client:           client,
		objectVectorizer: objectsvectorizer.New(),
	}
}

func (v *BatchTextVectorizer[T]) Texts(ctx context.Context,
	inputs []string, cfg moduletools.ClassConfig,
) (T, error) {
	return v.texts(ctx, inputs, cfg)
}

func (v *BatchTextVectorizer[T]) Object(ctx context.Context,
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

func (v *BatchTextVectorizer[T]) Objects(ctx context.Context,
	objs []*models.Object, cfg moduletools.ClassConfig,
) ([]T, models.AdditionalProperties, error) {
	vecs, err := v.objects(ctx, objs, cfg)
	return vecs, nil, err
}

func (v *BatchTextVectorizer[T]) objects(ctx context.Context, objects []*models.Object, cfg moduletools.ClassConfig,
) ([]T, error) {
	icheck := settings.NewBaseClassSettingsWithAltNames(cfg, v.lowerCaseInput, v.moduleName, v.altNames, nil)
	titleProperty := icheck.GetPropertyAsString("titleProperty", "")
	batchObjects := make([]*batchObject, len(objects))
	inputs := make([]string, 0)
	inputIndex := 0
	for i := range objects {
		corpi, _, isEmpty := v.objectVectorizer.TextsWithTitleProperty(ctx, objects[i], icheck, titleProperty)
		if isEmpty {
			batchObjects[i] = &batchObject{isEmpty: true}
		} else {
			inputs = append(inputs, corpi)
			batchObjects[i] = &batchObject{index: inputIndex}
			inputIndex++
		}
	}

	var res *modulecomponents.VectorizationResult[T]
	var err error
	if len(inputs) > 0 {
		res, _, _, err = v.client.Vectorize(ctx, inputs, cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to vectorize batch of objects: %w", err)
		}
		if len(res.Vector) != len(inputs) {
			return nil, fmt.Errorf("failed to vectorize all %v texts got only %v embeddings", len(inputs), len(res.Vector))
		}
	}

	results := make([]T, len(batchObjects))
	for i := range batchObjects {
		if batchObjects[i].isEmpty {
			results[i] = nil
		} else {
			results[i] = res.Vector[batchObjects[i].index]
		}
	}

	return results, nil
}

func (v *BatchTextVectorizer[T]) texts(ctx context.Context, input []string, cfg moduletools.ClassConfig,
) (T, error) {
	res, err := v.client.VectorizeQuery(ctx, input, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to vectorize batch of objects: %w", err)
	}
	if len(res.Vector) != 1 {
		return nil, fmt.Errorf("more than one embedding found for input: %v", input)
	}
	return res.Vector[0], nil
}

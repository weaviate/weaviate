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

package generictypes

import (
	"context"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/types"
)

type findVectorFn = func(ctx context.Context,
	className string, id strfmt.UUID, tenant, targetVector string) ([]float32, string, error)

// Helper method for creating modulecapabilities.FindVectorFn[[]float32]
func FindVectorFn(findVectorFn findVectorFn) modulecapabilities.FindVectorFn[[]float32] {
	return &findVector[[]float32]{findVectorFn}
}

type multiFindVectorFn = func(ctx context.Context,
	className string, id strfmt.UUID, tenant, targetVector string) ([][]float32, string, error)

// Helper method for creating modulecapabilities.FindVectorFn[[][]float32]
func MultiFindVectorFn(multiFindVectorFn multiFindVectorFn) modulecapabilities.FindVectorFn[[][]float32] {
	return &findVector[[][]float32]{multiFindVectorFn}
}

func (f *findVector[T]) FindVector(ctx context.Context,
	className string, id strfmt.UUID, tenant, targetVector string,
) (T, string, error) {
	return f.findVectorFn(ctx, className, id, tenant, targetVector)
}

type findVector[T types.Embedding] struct {
	findVectorFn func(ctx context.Context,
		className string, id strfmt.UUID, tenant, targetVector string) (T, string, error)
}

type vectorForParamsFn = func(ctx context.Context, params interface{},
	className string, findVectorFn modulecapabilities.FindVectorFn[[]float32], cfg moduletools.ClassConfig,
) ([]float32, error)

// Helper method for creating modulecapabilities.VectorForParams[[]float32]
func VectorForParams(vectorForParamsFn vectorForParamsFn) modulecapabilities.VectorForParams[[]float32] {
	return &vectorForParams[[]float32]{vectorForParamsFn}
}

type multiVectorForParamsFn = func(ctx context.Context, params interface{},
	className string, findVectorFn modulecapabilities.FindVectorFn[[][]float32], cfg moduletools.ClassConfig,
) ([][]float32, error)

// Helper method for creating modulecapabilities.VectorForParams[[][]float32]
func MultiVectorForParams(multiVectorForParamsFn multiVectorForParamsFn) modulecapabilities.VectorForParams[[][]float32] {
	return &vectorForParams[[][]float32]{multiVectorForParamsFn}
}

type vectorForParams[T types.Embedding] struct {
	vectorForParams func(ctx context.Context, params interface{},
		className string, findVectorFn modulecapabilities.FindVectorFn[T], cfg moduletools.ClassConfig,
	) (T, error)
}

func (v *vectorForParams[T]) VectorForParams(ctx context.Context, params interface{},
	className string, findVectorFn modulecapabilities.FindVectorFn[T], cfg moduletools.ClassConfig,
) (T, error) {
	return v.vectorForParams(ctx, params, className, findVectorFn, cfg)
}

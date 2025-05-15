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

package modules

import (
	"context"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
)

func vectorFromSearchParam[T dto.Embedding](
	ctx context.Context,
	class *models.Class,
	mod modulecapabilities.Module,
	targetModule, targetVector, tenant, param string,
	params interface{},
	findVectorFn modulecapabilities.FindVectorFn[T],
	isModuleNameEqualFn func(module modulecapabilities.Module, targetModule string) bool,
) (bool, T, error) {
	var moduleName string
	var vectorSearches map[string]modulecapabilities.VectorForParams[T]

	if searcher, ok := mod.(modulecapabilities.Searcher[T]); ok {
		if isModuleNameEqualFn(mod, targetModule) {
			moduleName = mod.Name()
			vectorSearches = searcher.VectorSearches()
		}
	} else if searchers, ok := mod.(modulecapabilities.DependencySearcher[T]); ok {
		if dependencySearchers := searchers.VectorSearches(); dependencySearchers != nil {
			moduleName = targetModule
			vectorSearches = dependencySearchers[targetModule]
		}
	}
	if vectorSearches != nil {
		if searchVectorFn := vectorSearches[param]; searchVectorFn != nil {
			cfg := NewClassBasedModuleConfig(class, moduleName, tenant, targetVector)
			vector, err := searchVectorFn.VectorForParams(ctx, params, class.Class, findVectorFn, cfg)
			if err != nil {
				return true, nil, errors.Errorf("vectorize params: %v", err)
			}
			return true, vector, nil
		}
	}

	return false, nil, nil
}

func crossClassVectorFromSearchParam[T dto.Embedding](
	ctx context.Context,
	mod modulecapabilities.Module,
	param string,
	params interface{},
	findVectorFn modulecapabilities.FindVectorFn[T],
	getTargetVectorFn func(class *models.Class, params interface{}) ([]string, error),
) (bool, T, string, error) {
	if searcher, ok := mod.(modulecapabilities.Searcher[T]); ok {
		if vectorSearches := searcher.VectorSearches(); vectorSearches != nil {
			if searchVectorFn := vectorSearches[param]; searchVectorFn != nil {
				cfg := NewCrossClassModuleConfig()
				vector, err := searchVectorFn.VectorForParams(ctx, params, "", findVectorFn, cfg)
				if err != nil {
					return true, nil, "", errors.Errorf("vectorize params: %v", err)
				}
				targetVector, err := getTargetVectorFn(nil, params)
				if err != nil {
					return true, nil, "", errors.Errorf("get target vector: %v", err)
				}
				if len(targetVector) > 0 {
					return true, vector, targetVector[0], nil
				}
				return true, vector, "", nil
			}
		}
	}

	return false, nil, "", nil
}

func vectorFromInput[T dto.Embedding](
	ctx context.Context,
	mod modulecapabilities.Module,
	class *models.Class,
	input, targetVector string,
) (bool, T, error) {
	if vectorizer, ok := mod.(modulecapabilities.InputVectorizer[T]); ok {
		// does not access any objects, therefore tenant is irrelevant
		cfg := NewClassBasedModuleConfig(class, mod.Name(), "", targetVector)
		vector, err := vectorizer.VectorizeInput(ctx, input, cfg)
		return true, vector, err
	}
	return false, nil, nil
}

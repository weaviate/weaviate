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

package traverser

import (
	"fmt"

	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modelsext"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
)

type TargetVectorParamHelper struct{}

func NewTargetParamHelper() *TargetVectorParamHelper {
	return &TargetVectorParamHelper{}
}

// GetTargetVectorOrDefault returns the caller's target vectors unchanged when any
// were supplied. Only when none were supplied does it read the single class via
// getClass to derive the default target vector, so callers that already have target
// vectors read no class at all.
func (t *TargetVectorParamHelper) GetTargetVectorOrDefault(getClass func(string) *models.Class, className string, targetVectors []string) ([]string, error) {
	if len(targetVectors) == 0 {
		class := getClass(className)
		if class == nil {
			return nil, fmt.Errorf("class %q not found", className)
		}

		// If no target vectors provided, check whether legacy vector is configured.
		// For backwards compatibility, we have to return legacy vector in case no named vectors configured.
		if modelsext.ClassHasLegacyVectorIndex(class) || len(class.VectorConfig) == 0 {
			return []string{""}, nil
		}

		if len(class.VectorConfig) > 1 {
			return []string{}, fmt.Errorf("multiple vectorizers configuration found, please specify target vector name")
		}

		if len(class.VectorConfig) == 1 {
			for name := range class.VectorConfig {
				return []string{name}, nil
			}
		}
	}

	return targetVectors, nil
}

func (t *TargetVectorParamHelper) GetTargetVectorsFromParams(params dto.GetParams) []string {
	if params.NearObject != nil && len(params.NearObject.TargetVectors) >= 1 {
		return params.NearObject.TargetVectors
	}
	if params.NearVector != nil && len(params.NearVector.TargetVectors) >= 1 {
		return params.NearVector.TargetVectors
	}
	if params.HybridSearch != nil && len(params.HybridSearch.TargetVectors) >= 1 {
		return params.HybridSearch.TargetVectors
	}
	if len(params.ModuleParams) > 0 {
		for _, moduleParam := range params.ModuleParams {
			if nearParam, ok := moduleParam.(modulecapabilities.NearParam); ok && len(nearParam.GetTargetVectors()) >= 1 {
				return nearParam.GetTargetVectors()
			}
		}
	}
	return []string{}
}

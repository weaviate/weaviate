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

package traverser

import (
	"fmt"

	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/schema"
)

type TargetVectorParamHelper struct{}

func NewTargetParamHelper() *TargetVectorParamHelper {
	return &TargetVectorParamHelper{}
}

func (t *TargetVectorParamHelper) GetTargetVectorOrDefault(sch schema.Schema, className, targetVector string) (string, error) {
	if targetVector == "" {
		class := sch.FindClassByName(schema.ClassName(className))

		if len(class.VectorConfig) > 1 {
			return "", fmt.Errorf("multiple vectorizers configuration found, please specify target vector name")
		}

		if len(class.VectorConfig) == 1 {
			for name := range class.VectorConfig {
				return name, nil
			}
		}
	}
	return targetVector, nil
}

func (t *TargetVectorParamHelper) GetTargetVectorFromParams(params dto.GetParams) string {
	if params.NearObject != nil && len(params.NearObject.TargetVectors) == 1 {
		return params.NearObject.TargetVectors[0]
	}
	if params.NearVector != nil && len(params.NearVector.TargetVectors) == 1 {
		return params.NearVector.TargetVectors[0]
	}
	if params.HybridSearch != nil && len(params.HybridSearch.TargetVectors) == 1 {
		return params.HybridSearch.TargetVectors[0]
	}
	if len(params.ModuleParams) > 0 {
		for _, moduleParam := range params.ModuleParams {
			if nearParam, ok := moduleParam.(modulecapabilities.NearParam); ok && len(nearParam.GetTargetVectors()) == 1 {
				return nearParam.GetTargetVectors()[0]
			}
		}
	}
	return ""
}

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

	"github.com/weaviate/weaviate/entities/schema"
)

type targetVectorParamHelper struct{}

func newTargetParamHelper() *targetVectorParamHelper {
	return &targetVectorParamHelper{}
}

func (t *targetVectorParamHelper) getTargetVectorOrDefault(sch schema.Schema, className, targetVector string) (string, error) {
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

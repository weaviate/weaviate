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
	"strings"

	"github.com/weaviate/weaviate/entities/schema/configvalidation"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/dto"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/vectorindex/common"
)

func (t *Traverser) validateExploreDistance(params ExploreParams) error {
	targetVectors := t.extractTargetVectors(params)
	distType, err := t.validateCrossClassDistanceCompatibility(targetVectors)
	if err != nil {
		return err
	}

	return t.validateExploreDistanceParams(params, distType)
}

// ensures that all classes are configured with the same distance type.
// if all classes are configured with the same type, said type is returned.
// otherwise an error indicating which classes are configured differently.
func (t *Traverser) validateCrossClassDistanceCompatibility(targetVectors []string) (distType string, err error) {
	s := t.schemaGetter.GetSchemaSkipAuth()
	if s.Objects == nil {
		return common.DefaultDistanceMetric, nil
	}

	var (
		// a set used to determine the discrete number
		// of vector index distance types used across
		// all classes. if more than one type exists,
		// a cross-class vector search is not possible
		distancerTypes = make(map[string]struct{})

		// a mapping of class name to vector index distance
		// type. used to emit an error if more than one
		// distance type is found
		classDistanceConfigs = make(map[string]string)
	)

	for _, class := range s.Objects.Classes {
		if class == nil {
			continue
		}

		vectorConfig, assertErr := schemaConfig.TypeAssertVectorIndex(class, targetVectors)
		if assertErr != nil {
			err = assertErr
			return
		}

		if len(vectorConfig) == 0 {
			err = fmt.Errorf("empty vectorConfig fot %v, %v", class, targetVectors)
		}

		distancerTypes[vectorConfig[0].DistanceName()] = struct{}{}
		classDistanceConfigs[class.Class] = vectorConfig[0].DistanceName()
	}

	if len(distancerTypes) != 1 {
		err = crossClassDistCompatError(classDistanceConfigs)
		return
	}

	// the above check ensures that the
	// map only contains one entry
	for dt := range distancerTypes {
		distType = dt
	}

	return
}

func (t *Traverser) validateExploreDistanceParams(params ExploreParams, distType string) error {
	certainty := extractCertaintyFromExploreParams(params)

	if certainty == 0 && !params.WithCertaintyProp {
		return nil
	}

	if distType != common.DistanceCosine {
		return certaintyUnsupportedError(distType)
	}

	return nil
}

func (t *Traverser) validateGetDistanceParams(params dto.GetParams) error {
	class := t.schemaGetter.ReadOnlyClass(params.ClassName)
	if class == nil {
		return fmt.Errorf("failed to find class '%s' in schema", params.ClassName)
	}

	targetVectors := t.targetVectorParamHelper.GetTargetVectorsFromParams(params)
	if err := configvalidation.CheckCertaintyCompatibility(class, targetVectors); err != nil {
		return err
	}

	return nil
}

func (t *Traverser) extractTargetVectors(params ExploreParams) []string {
	if params.NearVector != nil {
		return params.NearVector.TargetVectors
	}
	if params.NearObject != nil {
		return params.NearObject.TargetVectors
	}
	return []string{}
}

func crossClassDistCompatError(classDistanceConfigs map[string]string) error {
	errorMsg := "vector search across classes not possible: found different distance metrics:"
	for class, dist := range classDistanceConfigs {
		errorMsg = fmt.Sprintf("%s class '%s' uses distance metric '%s',", errorMsg, class, dist)
	}
	errorMsg = strings.TrimSuffix(errorMsg, ",")

	return errors.New(errorMsg)
}

func certaintyUnsupportedError(distType string) error {
	return errors.Errorf(
		"can't compute and return certainty when vector index is configured with %s distance",
		distType)
}

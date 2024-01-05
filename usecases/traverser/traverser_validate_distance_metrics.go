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

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/vectorindex/common"
)

func (t *Traverser) validateExploreDistance(params ExploreParams) error {
	distType, err := t.validateCrossClassDistanceCompatibility()
	if err != nil {
		return err
	}

	return t.validateExploreDistanceParams(params, distType)
}

// ensures that all classes are configured with the same distance type.
// if all classes are configured with the same type, said type is returned.
// otherwise an error indicating which classes are configured differently.
func (t *Traverser) validateCrossClassDistanceCompatibility() (distType string, err error) {
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

		vectorConfig, assertErr := schema.TypeAssertVectorIndex(class)
		if assertErr != nil {
			err = assertErr
			return
		}

		distancerTypes[vectorConfig.DistanceName()] = struct{}{}
		classDistanceConfigs[class.Class] = vectorConfig.DistanceName()
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
	sch := t.schemaGetter.GetSchemaSkipAuth()
	class := sch.GetClass(schema.ClassName(params.ClassName))
	if class == nil {
		return fmt.Errorf("failed to find class '%s' in schema", params.ClassName)
	}

	vectorConfig, err := schema.TypeAssertVectorIndex(class)
	if err != nil {
		return err
	}

	if dn := vectorConfig.DistanceName(); dn != common.DistanceCosine {
		return certaintyUnsupportedError(dn)
	}

	return nil
}

func crossClassDistCompatError(classDistanceConfigs map[string]string) error {
	errorMsg := "vector search across classes not possible: found different distance metrics:"
	for class, dist := range classDistanceConfigs {
		errorMsg = fmt.Sprintf("%s class '%s' uses distance metric '%s',", errorMsg, class, dist)
	}
	errorMsg = strings.TrimSuffix(errorMsg, ",")

	return fmt.Errorf(errorMsg)
}

func certaintyUnsupportedError(distType string) error {
	return errors.Errorf(
		"can't compute and return certainty when vector index is configured with %s distance",
		distType)
}

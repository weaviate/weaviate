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

package configvalidation

import (
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/vectorindex/common"
)

func CheckCertaintyCompatibility(class *models.Class, targetVectors []string) error {
	if class == nil {
		return errors.Errorf("no class provided to check certainty compatibility")
	}
	if len(targetVectors) > 1 {
		return errors.Errorf("multiple target vectors are not supported with certainty")
	}

	vectorConfigs, err := config.TypeAssertVectorIndex(class, targetVectors)
	if err != nil {
		return err
	}
	if dn := vectorConfigs[0].DistanceName(); dn != common.DistanceCosine {
		return certaintyUnsupportedError(dn)
	}

	return nil
}

func certaintyUnsupportedError(distType string) error {
	return errors.Errorf(
		"can't compute and return certainty when vector index is configured with %s distance",
		distType)
}

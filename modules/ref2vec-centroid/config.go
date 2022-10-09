//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package modcentroid

import (
	"context"
	"fmt"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/moduletools"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/modules/ref2vec-centroid/config"
)

func (m *CentroidModule) ClassConfigDefaults() map[string]interface{} {
	return config.Default()
}

func (m *CentroidModule) PropertyConfigDefaults(dataType *schema.DataType) map[string]interface{} {
	// no property-specific config for this module
	return nil
}

func (m *CentroidModule) ValidateClass(ctx context.Context,
	class *models.Class, classConfig moduletools.ClassConfig,
) error {
	err := config.Validate(config.New(classConfig))
	if err != nil {
		return fmt.Errorf("validate %q: %w", class.Class, err)
	}
	return nil
}

var _ = modulecapabilities.ClassConfigurator(New())

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

package modgenerativedummy

import (
	"context"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
)

func (m *GenerativeDummyModule) ClassConfigDefaults() map[string]interface{} {
	return map[string]interface{}{}
}

func (m *GenerativeDummyModule) PropertyConfigDefaults(
	dt *schema.DataType,
) map[string]interface{} {
	return map[string]interface{}{}
}

func (m *GenerativeDummyModule) ValidateClass(ctx context.Context,
	class *models.Class, cfg moduletools.ClassConfig,
) error {
	return nil
}

var _ = modulecapabilities.ClassConfigurator(New())

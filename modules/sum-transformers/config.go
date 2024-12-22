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

package modsum

import (
	"context"

	"github.com/liutizhong/weaviate/entities/models"
	"github.com/liutizhong/weaviate/entities/modulecapabilities"
	"github.com/liutizhong/weaviate/entities/moduletools"
	"github.com/liutizhong/weaviate/entities/schema"
)

func (m *SUMModule) ClassConfigDefaults() map[string]interface{} {
	return map[string]interface{}{}
}

func (m *SUMModule) PropertyConfigDefaults(dt *schema.DataType,
) map[string]interface{} {
	return map[string]interface{}{}
}

func (m *SUMModule) ValidateClass(ctx context.Context,
	class *models.Class, cfg moduletools.ClassConfig,
) error {
	return nil
}

var _ = modulecapabilities.ClassConfigurator(New())

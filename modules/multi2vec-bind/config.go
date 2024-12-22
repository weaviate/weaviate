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

package modbind

import (
	"context"

	"github.com/liutizhong/weaviate/entities/models"
	"github.com/liutizhong/weaviate/entities/modulecapabilities"
	"github.com/liutizhong/weaviate/entities/moduletools"
	"github.com/liutizhong/weaviate/entities/schema"
	"github.com/liutizhong/weaviate/modules/multi2vec-clip/vectorizer"
)

func (m *BindModule) ClassConfigDefaults() map[string]interface{} {
	return map[string]interface{}{}
}

func (m *BindModule) PropertyConfigDefaults(
	dt *schema.DataType,
) map[string]interface{} {
	return map[string]interface{}{}
}

func (m *BindModule) ValidateClass(ctx context.Context,
	class *models.Class, cfg moduletools.ClassConfig,
) error {
	icheck := vectorizer.NewClassSettings(cfg)
	return icheck.Validate()
}

var _ = modulecapabilities.ClassConfigurator(New())

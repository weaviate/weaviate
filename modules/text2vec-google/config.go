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

package modgoogle

import (
	"context"

	"github.com/liutizhong/weaviate/modules/qna-openai/config"
	"github.com/liutizhong/weaviate/modules/text2vec-google/vectorizer"

	"github.com/liutizhong/weaviate/entities/models"
	"github.com/liutizhong/weaviate/entities/modulecapabilities"
	"github.com/liutizhong/weaviate/entities/moduletools"
	"github.com/liutizhong/weaviate/entities/schema"
)

func (m *GoogleModule) ClassConfigDefaults() map[string]interface{} {
	return map[string]interface{}{
		"vectorizeClassName": vectorizer.DefaultVectorizeClassName,
	}
}

func (m *GoogleModule) PropertyConfigDefaults(
	dt *schema.DataType,
) map[string]interface{} {
	return map[string]interface{}{
		"skip":                  !vectorizer.DefaultPropertyIndexed,
		"vectorizePropertyName": vectorizer.DefaultVectorizePropertyName,
	}
}

func (m *GoogleModule) ValidateClass(ctx context.Context,
	class *models.Class, cfg moduletools.ClassConfig,
) error {
	settings := config.NewClassSettings(cfg)
	return settings.Validate(class)
}

var _ = modulecapabilities.ClassConfigurator(New())

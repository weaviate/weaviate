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

package modcontextionary

import (
	"context"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/modules/text2vec-contextionary/vectorizer"
	basesettings "github.com/weaviate/weaviate/usecases/modulecomponents/settings"
)

func (m *ContextionaryModule) ClassConfigDefaults() map[string]interface{} {
	return map[string]interface{}{
		"vectorizeClassName": basesettings.DefaultVectorizeClassName,
	}
}

func (m *ContextionaryModule) PropertyConfigDefaults(
	dt *schema.DataType,
) map[string]interface{} {
	return map[string]interface{}{
		"skip":                  !basesettings.DefaultPropertyIndexed,
		"vectorizePropertyName": basesettings.DefaultVectorizePropertyName,
	}
}

func (m *ContextionaryModule) ValidateClass(ctx context.Context,
	class *models.Class, cfg moduletools.ClassConfig,
) error {
	icheck := vectorizer.NewIndexChecker(cfg)
	if err := icheck.Validate(class); err != nil {
		return err
	}
	return m.configValidator.Do(ctx, class, cfg, icheck)
}

var _ = modulecapabilities.ClassConfigurator(New())

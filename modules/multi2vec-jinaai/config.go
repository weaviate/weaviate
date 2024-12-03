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

package modclip

import (
	"context"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/modules/multi2vec-jinaai/ent"
)

func (m *Module) ClassConfigDefaults() map[string]interface{} {
	return map[string]interface{}{
		ent.BaseURLProperty:  ent.DefaultBaseURL,
		ent.ModelProperty:    ent.DefaultJinaAIModel,
		"vectorizeClassName": ent.DefaultVectorizeClassName,
	}
}

func (m *Module) PropertyConfigDefaults(
	dt *schema.DataType,
) map[string]interface{} {
	return map[string]interface{}{}
}

func (m *Module) ValidateClass(ctx context.Context,
	class *models.Class, cfg moduletools.ClassConfig,
) error {
	icheck := ent.NewClassSettings(cfg)
	return icheck.Validate()
}

var _ = modulecapabilities.ClassConfigurator(New())

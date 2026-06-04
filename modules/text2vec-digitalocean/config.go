//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package moddigitalocean

import (
	"context"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/modules/text2vec-digitalocean/ent"
)

func (m *DigitalOceanModule) ClassConfigDefaults() map[string]any {
	return map[string]any{
		"vectorizeClassName": ent.DefaultVectorizeClassName,
		"baseURL":            ent.DefaultBaseURL,
	}
}

func (m *DigitalOceanModule) PropertyConfigDefaults(
	dt *schema.DataType,
) map[string]any {
	return map[string]any{
		"skip":                  !ent.DefaultPropertyIndexed,
		"vectorizePropertyName": ent.DefaultVectorizePropertyName,
	}
}

func (m *DigitalOceanModule) ValidateClass(ctx context.Context,
	class *models.Class, cfg moduletools.ClassConfig,
) error {
	settings := ent.NewClassSettings(cfg)
	return settings.Validate(ctx, class)
}

var _ = modulecapabilities.ClassConfigurator(New())

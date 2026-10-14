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

package modules

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
)

type migratingText2VecModule struct {
	dummyText2VecModuleNoCapabilities
}

func (migratingText2VecModule) MigrateProperties() []modulecapabilities.MigrateProperty {
	return []modulecapabilities.MigrateProperty{{Name: "baseURL"}, {Name: "dimensions"}}
}

func (migratingText2VecModule) ClassConfigDefaults() map[string]any {
	return map[string]any{"baseURL": "https://a", "dimensions": 512, "vectorizeClassName": true}
}

func (migratingText2VecModule) PropertyConfigDefaults(*schema.DataType) map[string]any { return nil }

func (migratingText2VecModule) ValidateClass(context.Context, *models.Class, moduletools.ClassConfig) error {
	return nil
}

// defaultsText2VecModule has class config defaults but no migrate properties.
type defaultsText2VecModule struct {
	dummyText2VecModuleNoCapabilities
}

func (defaultsText2VecModule) ClassConfigDefaults() map[string]any {
	return map[string]any{"baseURL": "https://a"}
}

func (defaultsText2VecModule) PropertyConfigDefaults(*schema.DataType) map[string]any { return nil }

func (defaultsText2VecModule) ValidateClass(context.Context, *models.Class, moduletools.ClassConfig) error {
	return nil
}

func TestMigrateVectorizerSettings(t *testing.T) {
	const (
		migrating = "text2vec-migrating"
		defaults  = "text2vec-defaults"
	)
	logger, _ := test.NewNullLogger()
	p := NewProvider(logger, config.Config{})
	p.Register(migratingText2VecModule{newDummyText2VecModule(migrating, nil)})
	p.Register(defaultsText2VecModule{newDummyText2VecModule(defaults, nil)})

	tests := []struct {
		name         string
		module       string
		stored       map[string]any
		update       map[string]any
		wantMigrated bool
		wantEqual    bool
	}{
		{
			name:         "added setting missing from the stored settings, set to a value other than the default",
			module:       migrating,
			stored:       map[string]any{"model": "small"},
			update:       map[string]any{"model": "small", "baseURL": "https://custom"},
			wantMigrated: true,
		},
		{
			name:         "added setting decoded as a float, default declared as an int",
			module:       migrating,
			stored:       map[string]any{"model": "small"},
			update:       map[string]any{"model": "small", "dimensions": float64(512)},
			wantMigrated: true,
			wantEqual:    true,
		},
		{
			name:         "added setting stored as an empty string",
			module:       migrating,
			stored:       map[string]any{"model": "small", "baseURL": ""},
			update:       map[string]any{"model": "small", "baseURL": "https://a"},
			wantMigrated: true,
			wantEqual:    true,
		},
		{
			name:         "defaulted setting outside the migrate properties missing from the stored settings",
			module:       migrating,
			stored:       map[string]any{"model": "small"},
			update:       map[string]any{"model": "small", "vectorizeClassName": true},
			wantMigrated: true,
			wantEqual:    true,
		},
		{
			name:         "stored custom value, update sets the default",
			module:       migrating,
			stored:       map[string]any{"model": "small", "baseURL": "https://custom"},
			update:       map[string]any{"model": "small", "baseURL": "https://a"},
			wantMigrated: true,
		},
		{
			name:   "module without migrate properties, setting missing from the stored settings, update sets the default",
			module: defaults,
			stored: map[string]any{"model": "small"},
			update: map[string]any{"model": "small", "baseURL": "https://a"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stored := map[string]any{tt.module: tt.stored}
			update := map[string]any{tt.module: tt.update}

			assert.Equal(t, tt.wantMigrated, p.MigrateVectorizerSettings(stored, update))
			assert.Equal(t, tt.wantEqual, assert.ObjectsAreEqual(stored, update), "stored: %v, update: %v", stored, update)
		})
	}
}

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
	return []modulecapabilities.MigrateProperty{
		{Name: "baseUrl", NewName: "baseURL"},
		{Name: "baseURL"},
		{Name: "truncate"},
		{Name: "dimensions"},
		{Name: "legacy", IsDeleted: true},
	}
}

func (migratingText2VecModule) ClassConfigDefaults() map[string]any {
	return map[string]any{"baseURL": "https://a", "truncate": "END", "dimensions": 512, "vectorizeClassName": true}
}

func (migratingText2VecModule) PropertyConfigDefaults(*schema.DataType) map[string]any { return nil }

func (migratingText2VecModule) ValidateClass(context.Context, *models.Class, moduletools.ClassConfig) error {
	return nil
}

func TestMigrateVectorizerSettings(t *testing.T) {
	const mod = "text2vec-migrating"
	logger, _ := test.NewNullLogger()
	p := NewProvider(logger, config.Config{})
	p.Register(migratingText2VecModule{newDummyText2VecModule(mod, nil)})

	tests := []struct {
		name         string
		stored       map[string]any
		update       map[string]any
		wantMigrated bool
		wantEqual    bool
	}{
		{
			name:         "changed model with a migrate key on both sides",
			stored:       map[string]any{"model": "small", "baseURL": "https://a"},
			update:       map[string]any{"model": "large", "baseURL": "https://a"},
			wantMigrated: true,
		},
		{
			name:         "changed model with a migrate key only in the update",
			stored:       map[string]any{"model": "small"},
			update:       map[string]any{"model": "large", "baseURL": "https://a"},
			wantMigrated: true,
		},
		{
			name:         "changed value of a migrate key present on both sides",
			stored:       map[string]any{"model": "small", "baseURL": "https://a"},
			update:       map[string]any{"model": "small", "baseURL": "https://b"},
			wantMigrated: true,
		},
		{
			name:         "stored setting dropped by the update",
			stored:       map[string]any{"model": "small", "dimensions": 512, "baseURL": "https://a"},
			update:       map[string]any{"model": "small", "baseURL": "https://a"},
			wantMigrated: true,
		},
		{
			name:         "renamed key with a different value",
			stored:       map[string]any{"model": "small", "baseUrl": "https://a"},
			update:       map[string]any{"model": "small", "baseURL": "https://b"},
			wantMigrated: true,
		},
		{
			name:         "added setting missing from the stored settings",
			stored:       map[string]any{"model": "small"},
			update:       map[string]any{"model": "small", "baseURL": "https://a", "truncate": "END"},
			wantMigrated: true,
			wantEqual:    true,
		},
		{
			name:         "added setting decoded as a float, default declared as an int",
			stored:       map[string]any{"model": "small"},
			update:       map[string]any{"model": "small", "dimensions": float64(512)},
			wantMigrated: true,
			wantEqual:    true,
		},
		{
			name:         "added setting missing from the stored settings, set to a value other than the default",
			stored:       map[string]any{"model": "small"},
			update:       map[string]any{"model": "small", "baseURL": "https://custom"},
			wantMigrated: true,
		},
		{
			name:         "added setting stored as an empty string, set to a value other than the default",
			stored:       map[string]any{"model": "small", "baseURL": ""},
			update:       map[string]any{"model": "small", "baseURL": "https://custom"},
			wantMigrated: true,
		},
		{
			name:         "added setting stored as an empty string",
			stored:       map[string]any{"model": "small", "baseURL": ""},
			update:       map[string]any{"model": "small", "baseURL": "https://a"},
			wantMigrated: true,
			wantEqual:    true,
		},
		{
			name:         "renamed key with the same value",
			stored:       map[string]any{"model": "small", "baseUrl": "https://a"},
			update:       map[string]any{"model": "small", "baseURL": "https://a"},
			wantMigrated: true,
			wantEqual:    true,
		},
		{
			name:         "deleted key in the stored settings",
			stored:       map[string]any{"model": "small", "legacy": "x"},
			update:       map[string]any{"model": "small"},
			wantMigrated: true,
			wantEqual:    true,
		},
		{
			name:   "no migrate key",
			stored: map[string]any{"model": "small"},
			update: map[string]any{"model": "large"},
		},
		{
			name:         "defaulted setting outside the migrate properties missing from the stored settings",
			stored:       map[string]any{"model": "small"},
			update:       map[string]any{"model": "small", "vectorizeClassName": true},
			wantMigrated: true,
			wantEqual:    true,
		},
		{
			name:   "defaulted setting outside the migrate properties set to a value other than the default",
			stored: map[string]any{"model": "small"},
			update: map[string]any{"model": "small", "vectorizeClassName": false},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stored := map[string]any{mod: tt.stored}
			update := map[string]any{mod: tt.update}

			assert.Equal(t, tt.wantMigrated, p.MigrateVectorizerSettings(stored, update))
			assert.Equal(t, tt.wantEqual, assert.ObjectsAreEqual(stored, update), "stored: %v, update: %v", stored, update)
		})
	}
}

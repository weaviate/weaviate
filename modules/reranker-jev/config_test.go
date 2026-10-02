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

package modrerankerjev

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/modules"
)

func TestValidateClass(t *testing.T) {
	tests := []struct {
		name     string
		settings map[string]any
		wantErr  string
	}{
		{name: "defaults", settings: map[string]any{}},
		{name: "valid maxDocuments", settings: map[string]any{"maxDocuments": 10}},
		{
			name:     "maxDocuments above the upper bound",
			settings: map[string]any{"maxDocuments": 100000},
			wantErr:  "maxDocuments must be between 1 and 1000, got 100000",
		},
		{
			name:     "maxDocuments of the wrong type",
			settings: map[string]any{"maxDocuments": "many"},
			wantErr:  "maxDocuments must be between 1 and 1000, got 0",
		},
		{
			name:     "empty model",
			settings: map[string]any{"model": ""},
			wantErr:  "no model provided",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			class := &models.Class{
				Class:        "Ticket",
				ModuleConfig: map[string]any{Name: tt.settings},
			}
			cfg := modules.NewClassBasedModuleConfig(class, Name, "", "", &config.Config{})

			err := New().ValidateClass(context.Background(), class, cfg)

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}

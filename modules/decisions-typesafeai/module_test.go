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

package moddecisionstypesafeai

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/schema"
)

func TestMaxConcurrentRequestsFromEnv(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		want    int
		wantErr string
	}{
		{name: "unset uses the default", value: "", want: 16},
		{name: "lowest value", value: "1", want: 1},
		{name: "highest value", value: "256", want: 256},
		{name: "zero", value: "0", wantErr: `DECISIONS_TYPESAFEAI_MAX_CONCURRENT_REQUESTS must be a whole number between 1 and 256, got "0"`},
		{name: "above the limit", value: "257", wantErr: `DECISIONS_TYPESAFEAI_MAX_CONCURRENT_REQUESTS must be a whole number between 1 and 256, got "257"`},
		{name: "negative", value: "-4", wantErr: `DECISIONS_TYPESAFEAI_MAX_CONCURRENT_REQUESTS must be a whole number between 1 and 256, got "-4"`},
		{name: "not a number", value: "many", wantErr: `DECISIONS_TYPESAFEAI_MAX_CONCURRENT_REQUESTS must be a whole number between 1 and 256, got "many"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(maxConcurrentRequestsEnv, tt.value)

			got, err := maxConcurrentRequestsFromEnv()

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestDecisionLimits(t *testing.T) {
	tests := []struct {
		name             string
		settings         map[string]any
		wantMaxDocuments int
		wantErr          string
	}{
		{name: "defaults", settings: map[string]any{}, wantMaxDocuments: 100},
		{name: "maxDocuments from the class", settings: map[string]any{"maxDocuments": 40}, wantMaxDocuments: 40},
		// A class created before the module was enabled was never validated.
		{name: "maxDocuments above the upper bound", settings: map[string]any{"maxDocuments": 5000}, wantErr: "maxDocuments must be a whole number between 1 and 1000, got 5000"},
		{name: "maxDocuments of another type", settings: map[string]any{"maxDocuments": "many"}, wantErr: "maxDocuments must be a whole number between 1 and 1000, got many"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			limits, err := New().DecisionLimits(classConfig(tt.settings))

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, modulecapabilities.DecisionLimits{
				MaxDocuments: tt.wantMaxDocuments,
				MaxLevels:    10,
				DataTypes:    []schema.DataType{schema.DataTypeText},
			}, limits)
		})
	}
}

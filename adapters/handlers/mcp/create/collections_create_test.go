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

package create

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
)

func TestClassFromArgs(t *testing.T) {
	yes := true
	tests := []struct {
		name    string
		args    CreateCollectionArgs
		want    *models.Class
		wantErr string
	}{
		{
			name: "full surface maps to the REST class",
			args: CreateCollectionArgs{
				CollectionName: "Article",
				Description:    "News articles",
				Properties: []map[string]any{
					{"name": "title", "dataType": []any{"text"}, "tokenization": "word", "indexFilterable": true},
					{"name": "author", "dataType": []any{"Person"}},
					{"name": "meta", "dataType": []any{"object"}, "nestedProperties": []any{
						map[string]any{"name": "year", "dataType": []any{"int"}},
					}},
				},
				VectorConfig: map[string]any{
					"default": map[string]any{"vectorizer": map[string]any{"none": map[string]any{}}, "vectorIndexType": "hnsw"},
				},
				ModuleConfig:        map[string]any{"generative-dummy": map[string]any{}},
				MultiTenancyConfig:  map[string]any{"enabled": true, "autoTenantCreation": true},
				InvertedIndexConfig: map[string]any{"indexTimestamps": true},
				ReplicationConfig:   map[string]any{"factor": 1},
				ObjectTTLConfig:     map[string]any{"enabled": true, "deleteOn": "_creationTimeUnix", "defaultTtl": 3600},
			},
			want: &models.Class{
				Class:       "Article",
				Description: "News articles",
				Properties: []*models.Property{
					{Name: "title", DataType: []string{"text"}, Tokenization: "word", IndexFilterable: &yes},
					{Name: "author", DataType: []string{"Person"}},
					{Name: "meta", DataType: []string{"object"}, NestedProperties: []*models.NestedProperty{
						{Name: "year", DataType: []string{"int"}},
					}},
				},
				VectorConfig: map[string]models.VectorConfig{
					"default": {Vectorizer: map[string]any{"none": map[string]any{}}, VectorIndexType: "hnsw"},
				},
				ModuleConfig:        map[string]any{"generative-dummy": map[string]any{}},
				MultiTenancyConfig:  &models.MultiTenancyConfig{Enabled: true, AutoTenantCreation: true},
				InvertedIndexConfig: &models.InvertedIndexConfig{IndexTimestamps: true},
				ReplicationConfig:   &models.ReplicationConfig{Factor: 1},
				ObjectTTLConfig:     &models.ObjectTTLConfig{Enabled: true, DeleteOn: "_creationTimeUnix", DefaultTTL: 3600},
			},
		},
		{
			name: "unknown key in a typed nested object is rejected",
			args: CreateCollectionArgs{
				CollectionName:      "Article",
				InvertedIndexConfig: map[string]any{"indexTimestamp": true},
			},
			wantErr: `unknown field "indexTimestamp"`,
		},
		{
			name: "unknown key in a property is rejected",
			args: CreateCollectionArgs{
				CollectionName: "Article",
				Properties:     []map[string]any{{"name": "title", "dataTypes": []any{"text"}}},
			},
			wantErr: `unknown field "dataTypes"`,
		},
		{
			name: "invalid enum value is rejected by model validation",
			args: CreateCollectionArgs{
				CollectionName: "Article",
				Properties:     []map[string]any{{"name": "title", "dataType": []any{"text"}, "tokenization": "bogus"}},
			},
			wantErr: "tokenization",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := classFromArgs(tt.args)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

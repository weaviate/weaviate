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

package schema_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	command "github.com/weaviate/weaviate/cluster/proto/api"
	clusterSchema "github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/fakes"
)

func maskedTestClass(k1 float32, ef int) *models.Class {
	vectorizer := map[string]any{module: map[string]any{}}
	noRange := false
	return &models.Class{
		Class:               className,
		InvertedIndexConfig: &models.InvertedIndexConfig{Bm25: &models.BM25Config{K1: k1, B: 0.75}},
		Properties:          []*models.Property{{Name: "p1", DataType: []string{"text"}, IndexRangeFilters: &noRange}},
		VectorConfig: map[string]models.VectorConfig{
			"a": {VectorIndexType: "hnsw", Vectorizer: vectorizer, VectorIndexConfig: map[string]any{"ef": ef}},
			"b": {VectorIndexType: "hnsw", Vectorizer: vectorizer, VectorIndexConfig: map[string]any{}},
		},
	}
}

func efOf(t *testing.T, c *models.Class, vector string) int {
	t.Helper()
	raw, err := json.Marshal(c.VectorConfig[vector].VectorIndexConfig)
	require.NoError(t, err)
	var cfg struct {
		EF int `json:"ef"`
	}
	require.NoError(t, json.Unmarshal(raw, &cfg))
	return cfg.EF
}

// A stale class write, built from a read taken before an operator raised k1 and
// ef, must keep both in the schema and in the class the shards receive.
func TestSchemaManager_UpdateClass_FieldMask(t *testing.T) {
	flag := command.ClassFieldUsingBlockMaxWAND
	tests := []struct {
		name        string
		addProperty bool
		stale       func(c *models.Class)
		fields      []string
		wantErr     string
		wantK1      float32
		wantEF      int
		wantProps   int
		wantFlag    bool
		wantVectorB string
	}{
		{
			name:   "flag mask keeps newer settings",
			stale:  func(c *models.Class) { c.InvertedIndexConfig.UsingBlockMaxWAND = true },
			fields: []string{flag},
			wantK1: 1.9, wantEF: 777, wantProps: 1, wantFlag: true, wantVectorB: "hnsw",
		},
		{
			name:        "flag mask applies after a property was added",
			addProperty: true,
			stale:       func(c *models.Class) { c.InvertedIndexConfig.UsingBlockMaxWAND = true },
			fields:      []string{flag},
			wantK1:      1.9, wantEF: 777, wantProps: 2, wantFlag: true, wantVectorB: "hnsw",
		},
		{
			name: "marker mask keeps newer settings on the other vector",
			stale: func(c *models.Class) {
				b := c.VectorConfig["b"]
				c.VectorConfig["b"] = models.VectorConfig{VectorIndexType: "none", Vectorizer: b.Vectorizer}
			},
			fields: []string{command.ClassFieldVectorConfig("b")},
			wantK1: 1.9, wantEF: 777, wantProps: 1, wantVectorB: "none",
		},
		{
			name:   "nil mask replaces the class",
			stale:  func(c *models.Class) { c.InvertedIndexConfig.UsingBlockMaxWAND = true },
			wantK1: 1.2, wantEF: 100, wantProps: 1, wantFlag: true, wantVectorB: "hnsw",
		},
		{
			name:    "unknown field is refused",
			stale:   func(c *models.Class) { c.Description = "stale" },
			fields:  []string{"description"},
			wantErr: `unknown class field "description"`,
		},
		{
			name:    "masked removal of a live vector is refused",
			stale:   func(c *models.Class) { delete(c.VectorConfig, "b") },
			fields:  []string{command.ClassFieldVectorConfig("b")},
			wantErr: `missing config for vector "b"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var sent *models.Class
			indexer := fakes.NewMockSchemaExecutor()
			indexer.On("UpdateClass", mock.Anything).Run(func(args mock.Arguments) {
				sent = args.Get(0).(command.UpdateClassRequest).Class
			}).Return(nil)
			sm := newSchemaManagerWithModules(t, indexer)
			require.NoError(t, sm.AddClass(addClassRequest(t, maskedTestClass(1.2, 100)), "node1", true, false))
			require.NoError(t, sm.UpdateClass(updateClassRequest(t, maskedTestClass(1.9, 777), 2), "node1", true, false))
			if tt.addProperty {
				require.NoError(t, sm.AddProperty(applyRequest(t, command.ApplyRequest_TYPE_ADD_PROPERTY, className, 3,
					command.AddPropertyRequest{Properties: []*models.Property{{Name: "p2", DataType: []string{"text"}}}}), true, false))
			}

			stale := maskedTestClass(1.2, 100)
			tt.stale(stale)
			req := applyRequest(t, command.ApplyRequest_TYPE_UPDATE_CLASS, className, 4,
				command.UpdateClassRequest{Class: stale, FieldsToUpdate: tt.fields})
			err := sm.UpdateClass(req, "node1", false, false)
			if tt.wantErr != "" {
				require.ErrorIs(t, err, clusterSchema.ErrBadRequest)
				require.ErrorContains(t, err, tt.wantErr)
				require.Nil(t, sent, "a refused write must not reach the shards")
				return
			}
			require.NoError(t, err)

			stored := sm.NewSchemaReader().ReadOnlyClass(className)
			for _, c := range []*models.Class{stored, sent} {
				require.Equal(t, tt.wantK1, c.InvertedIndexConfig.Bm25.K1)
				require.Equal(t, tt.wantFlag, c.InvertedIndexConfig.UsingBlockMaxWAND)
				require.Equal(t, tt.wantEF, efOf(t, c, "a"))
				require.Equal(t, tt.wantVectorB, c.VectorConfig["b"].VectorIndexType)
				require.Len(t, c.Properties, tt.wantProps)
			}
		})
	}
}

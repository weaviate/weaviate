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

package mcp

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/create"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/read"
	"github.com/weaviate/weaviate/test/helper"
)

const toolNameCreateCollection = "weaviate-collections-create"

// setupCreateCollectionTest makes sure the collection does not exist before
// and after the test.
func setupCreateCollectionTest(t *testing.T, name string) (context.Context, func()) {
	helper.SetupClient(testServerAddr)
	helper.DeleteClassAuth(t, name, testAPIKey)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	return ctx, func() {
		cancel()
		helper.DeleteClassAuth(t, name, testAPIKey)
	}
}

func createCollection(ctx context.Context, t *testing.T, args create.CreateCollectionArgs, apiKey string) (*create.CreateCollectionResp, error) {
	t.Helper()
	var resp *create.CreateCollectionResp
	err := helper.CallToolOnce(ctx, t, toolNameCreateCollection, args, &resp, apiKey)
	return resp, err
}

func TestCreateCollectionToolNameOnly(t *testing.T) {
	name := "MCPCreateNameOnly"
	ctx, cleanup := setupCreateCollectionTest(t, name)
	defer cleanup()

	resp, err := createCollection(ctx, t, create.CreateCollectionArgs{CollectionName: name}, testAPIKey)
	require.NoError(t, err)
	require.NotNil(t, resp.Collection)
	assert.Equal(t, name, resp.Collection.Class)
	assert.Empty(t, resp.Collection.VectorConfig)
	assert.Empty(t, resp.Collection.Vectorizer)
	assert.Empty(t, resp.Collection.VectorIndexType)
}

func TestCreateCollectionToolWideConfig(t *testing.T) {
	name := "MCPCreateWide"
	ctx, cleanup := setupCreateCollectionTest(t, name)
	defer cleanup()

	resp, err := createCollection(ctx, t, create.CreateCollectionArgs{
		CollectionName: name,
		Description:    "wide config",
		Properties: []map[string]any{
			{"name": "title", "dataType": []string{"text"}, "tokenization": "field", "indexFilterable": true},
			{"name": "count", "dataType": []string{"int"}, "indexRangeFilters": true},
			{"name": "meta", "dataType": []string{"object"}, "nestedProperties": []map[string]any{
				{"name": "year", "dataType": []string{"int"}},
			}},
		},
		VectorConfig: map[string]any{
			"main": map[string]any{"vectorizer": map[string]any{"none": map[string]any{}}, "vectorIndexType": "flat"},
		},
		ModuleConfig:        map[string]any{"generative-dummy": map[string]any{}},
		InvertedIndexConfig: map[string]any{"indexTimestamps": true, "indexNullState": true},
		ReplicationConfig:   map[string]any{"factor": 1},
	}, testAPIKey)
	require.NoError(t, err)

	stored := resp.Collection
	require.NotNil(t, stored)
	assert.Equal(t, name, stored.Class)
	assert.Equal(t, "wide config", stored.Description)
	require.Len(t, stored.Properties, 3)
	assert.Equal(t, "field", stored.Properties[0].Tokenization)
	require.Len(t, stored.Properties[2].NestedProperties, 1)
	require.Contains(t, stored.VectorConfig, "main")
	assert.Equal(t, "flat", stored.VectorConfig["main"].VectorIndexType)
	assert.Contains(t, stored.ModuleConfig, "generative-dummy")
	assert.True(t, stored.InvertedIndexConfig.IndexTimestamps)
	assert.True(t, stored.InvertedIndexConfig.IndexNullState)
	assert.Equal(t, int64(1), stored.ReplicationConfig.Factor)

	var got *read.GetCollectionConfigResp
	require.NoError(t, helper.CallToolOnce(ctx, t, toolNameGetConfig,
		&read.GetCollectionConfigArgs{CollectionName: name}, &got, testAPIKey))
	require.Len(t, got.Collections, 1)
	assert.Equal(t, stored, got.Collections[0])
}

func TestCreateCollectionToolMultiTenantAutoCreation(t *testing.T) {
	name := "MCPCreateMultiTenant"
	ctx, cleanup := setupCreateCollectionTest(t, name)
	defer cleanup()

	_, err := createCollection(ctx, t, create.CreateCollectionArgs{
		CollectionName:     name,
		Properties:         []map[string]any{{"name": "title", "dataType": []string{"text"}}},
		MultiTenancyConfig: map[string]any{"enabled": true, "autoTenantCreation": true},
	}, testAPIKey)
	require.NoError(t, err)

	upsertObjects(t, ctx, &create.UpsertObjectArgs{
		CollectionName: name,
		TenantName:     "newTenant",
		Objects:        []create.ObjectToUpsert{{Properties: map[string]any{"title": "hello"}}},
	}, testAPIKey)
}

func TestCreateCollectionToolErrors(t *testing.T) {
	existing, unknownModule := "MCPCreateDuplicate", "MCPCreateUnknownModule"
	ctx, cleanup := setupCreateCollectionTest(t, existing)
	defer cleanup()
	_, cleanupUnknown := setupCreateCollectionTest(t, unknownModule)
	defer cleanupUnknown()
	_, err := createCollection(ctx, t, create.CreateCollectionArgs{CollectionName: existing}, testAPIKey)
	require.NoError(t, err)

	tests := []struct {
		name    string
		args    create.CreateCollectionArgs
		wantErr string
	}{
		{name: "duplicate name", args: create.CreateCollectionArgs{CollectionName: existing}, wantErr: "already exists"},
		{name: "invalid name", args: create.CreateCollectionArgs{CollectionName: "my-coll"}, wantErr: "not a valid class name"},
		{
			name: "unknown vectorizer module",
			args: create.CreateCollectionArgs{
				CollectionName: unknownModule,
				VectorConfig:   map[string]any{"default": map[string]any{"vectorizer": map[string]any{"text2vec-unknown": map[string]any{}}}},
			},
			wantErr: "no module with name",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := createCollection(ctx, t, tt.args, testAPIKey)
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

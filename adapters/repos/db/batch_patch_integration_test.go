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

//go:build integrationTest

package db

import (
	"context"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/objects"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestBatchPatchObjects_Integration(t *testing.T) {
	ctx := context.Background()
	logger := logrus.New()

	t.Run("partial update of properties", func(t *testing.T) {
		// Setup: Create temporary DB
		dirName := t.TempDir()
		shardState := singleShardState()

		schemaGetter := &fakeSchemaGetter{
			schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
			shardState: shardState,
		}

		// Setup mocks following merge_integration_test.go pattern
		mockSchemaReader := schemaUC.NewMockSchemaReader(t)
		mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
		mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything).RunAndReturn(func(className string, readFunc func(*models.Class, *sharding.State) error) error {
			class := &models.Class{Class: className}
			return readFunc(class, shardState)
		}).Maybe()
		mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
		mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()

		mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
		mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
		mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()

		mockNodeSelector := cluster.NewMockNodeSelector(t)
		mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
		mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()

		db, err := New(logger, "node1", Config{
			MemtablesFlushDirtyAfter:  60,
			RootPath:                  dirName,
			QueryMaximumResults:       10000,
			MaxImportGoroutinesFactor: 1,
		}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil, nil,
			mockNodeSelector, mockSchemaReader, mockReplicationFSMReader)
		require.Nil(t, err)
		db.SetSchemaGetter(schemaGetter)
		require.Nil(t, db.WaitForStartup(testCtx()))
		defer db.Shutdown(ctx)

		// Create schema
		migrator := NewMigrator(db, logger, "node1")
		class := &models.Class{
			Class:               "TestClass",
			VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
			InvertedIndexConfig: invertedConfig(),
			Properties: []*models.Property{
				{
					Name:         "title",
					DataType:     schema.DataTypeText.PropString(),
					Tokenization: models.PropertyTokenizationWhitespace,
				},
				{
					Name:         "content",
					DataType:     schema.DataTypeText.PropString(),
					Tokenization: models.PropertyTokenizationWhitespace,
				},
				{
					Name:     "author",
					DataType: schema.DataTypeText.PropString(),
				},
			},
		}

		require.Nil(t, migrator.AddClass(ctx, class))

		// Update schema getter with the new class
		schemaGetter.schema = schema.Schema{
			Objects: &models.Schema{
				Classes: []*models.Class{class},
			},
		}

		// Insert initial objects using BatchPutObjects
		id1 := strfmt.UUID("00000000-0000-0000-0000-000000000001")
		id2 := strfmt.UUID("00000000-0000-0000-0000-000000000002")

		initialObjects := objects.BatchObjects{
			{
				OriginalIndex: 0,
				Object: &models.Object{
					Class: "TestClass",
					ID:    id1,
					Properties: map[string]interface{}{
						"title":   "Original Title 1",
						"content": "Original Content 1",
						"author":  "John Doe",
					},
					Vector: []float32{1.0, 2.0, 3.0},
				},
				UUID: id1,
			},
			{
				OriginalIndex: 1,
				Object: &models.Object{
					Class: "TestClass",
					ID:    id2,
					Properties: map[string]interface{}{
						"title":   "Original Title 2",
						"content": "Original Content 2",
						"author":  "Jane Smith",
					},
					Vector: []float32{4.0, 5.0, 6.0},
				},
				UUID: id2,
			},
		}

		result, err := db.BatchPutObjects(ctx, initialObjects, nil, 1)
		require.Nil(t, err)
		require.Len(t, result, 2)
		assert.Nil(t, result[0].Err)
		assert.Nil(t, result[1].Err)

		// Perform batch patch to update properties
		patchObjects := objects.BatchObjects{
			{
				OriginalIndex: 0,
				Object: &models.Object{
					Class: "TestClass",
					ID:    id1,
					Properties: map[string]interface{}{
						"title":   "Updated Title 1",
						"content": "Updated Content 1",
						// author not included - should be preserved
					},
					Vector: []float32{7.0, 8.0, 9.0}, // Update vector
				},
				UUID: id1,
				MergeDoc: &objects.MergeDocument{
					Class:      "TestClass",
					ID:         id1,
					UpdateTime: 123456789,
					PrimitiveSchema: map[string]interface{}{
						"title":   "Updated Title 1",
						"content": "Updated Content 1",
					},
					PropertiesToDelete: []string{},
					Vector:             []float32{7.0, 8.0, 9.0},
				},
			},
			{
				OriginalIndex: 1,
				Object: &models.Object{
					Class: "TestClass",
					ID:    id2,
					Properties: map[string]interface{}{
						"title": "Updated Title 2",
						// content and author not included - should be preserved
					},
					// No vector update - should preserve existing vector
				},
				UUID: id2,
				MergeDoc: &objects.MergeDocument{
					Class:      "TestClass",
					ID:         id2,
					UpdateTime: 123456789,
					PrimitiveSchema: map[string]interface{}{
						"title": "Updated Title 2",
					},
					PropertiesToDelete: []string{},
				},
			},
		}

		patchResult, err := db.BatchPatchObjects(ctx, patchObjects, nil, 2)
		require.Nil(t, err)
		require.Len(t, patchResult, 2)
		assert.Nil(t, patchResult[0].Err)
		assert.Nil(t, patchResult[1].Err)

		// Verify: Read back objects and check merged properties
		obj1, err := db.ObjectByID(ctx, id1, nil, additional.Properties{}, "")
		require.Nil(t, err)
		require.NotNil(t, obj1)
		props1 := obj1.Schema.(map[string]interface{})
		assert.Equal(t, "Updated Title 1", props1["title"])
		assert.Equal(t, "Updated Content 1", props1["content"])
		assert.Equal(t, "John Doe", props1["author"]) // Preserved
		assert.Equal(t, []float32{7.0, 8.0, 9.0}, obj1.Vector)

		obj2, err := db.ObjectByID(ctx, id2, nil, additional.Properties{}, "")
		require.Nil(t, err)
		require.NotNil(t, obj2)
		props2 := obj2.Schema.(map[string]interface{})
		assert.Equal(t, "Updated Title 2", props2["title"])
		assert.Equal(t, "Original Content 2", props2["content"]) // Preserved
		assert.Equal(t, "Jane Smith", props2["author"])          // Preserved
		assert.Equal(t, []float32{4.0, 5.0, 6.0}, obj2.Vector)   // Preserved
	})

	t.Run("delete properties with nil values", func(t *testing.T) {
		// Setup: Create temporary DB
		dirName := t.TempDir()
		shardState := singleShardState()

		schemaGetter := &fakeSchemaGetter{
			schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
			shardState: shardState,
		}

		mockSchemaReader := schemaUC.NewMockSchemaReader(t)
		mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
		mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything).RunAndReturn(func(className string, readFunc func(*models.Class, *sharding.State) error) error {
			class := &models.Class{Class: className}
			return readFunc(class, shardState)
		}).Maybe()
		mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
		mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()

		mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
		mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
		mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()

		mockNodeSelector := cluster.NewMockNodeSelector(t)
		mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
		mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()

		db, err := New(logger, "node1", Config{
			MemtablesFlushDirtyAfter:  60,
			RootPath:                  dirName,
			QueryMaximumResults:       10000,
			MaxImportGoroutinesFactor: 1,
		}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil, nil,
			mockNodeSelector, mockSchemaReader, mockReplicationFSMReader)
		require.Nil(t, err)
		db.SetSchemaGetter(schemaGetter)
		require.Nil(t, db.WaitForStartup(testCtx()))
		defer db.Shutdown(ctx)

		migrator := NewMigrator(db, logger, "node1")
		class := &models.Class{
			Class:               "TestClass",
			VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
			InvertedIndexConfig: invertedConfig(),
			Properties: []*models.Property{
				{
					Name:     "title",
					DataType: schema.DataTypeText.PropString(),
				},
				{
					Name:     "content",
					DataType: schema.DataTypeText.PropString(),
				},
				{
					Name:     "author",
					DataType: schema.DataTypeText.PropString(),
				},
			},
		}

		require.Nil(t, migrator.AddClass(ctx, class))

		schemaGetter.schema = schema.Schema{
			Objects: &models.Schema{
				Classes: []*models.Class{class},
			},
		}

		// Insert initial object
		id := strfmt.UUID("00000000-0000-0000-0000-000000000001")

		initialObjects := objects.BatchObjects{
			{
				OriginalIndex: 0,
				Object: &models.Object{
					Class: "TestClass",
					ID:    id,
					Properties: map[string]interface{}{
						"title":   "Test Title",
						"content": "Test Content",
						"author":  "Test Author",
					},
					Vector: []float32{1.0, 2.0, 3.0},
				},
				UUID: id,
			},
		}

		result, err := db.BatchPutObjects(ctx, initialObjects, nil, 1)
		require.Nil(t, err)
		require.Len(t, result, 1)
		assert.Nil(t, result[0].Err)

		// Patch to delete content property
		patchObjects := objects.BatchObjects{
			{
				OriginalIndex: 0,
				Object: &models.Object{
					Class: "TestClass",
					ID:    id,
					Properties: map[string]interface{}{
						"title": "Updated Title",
					},
				},
				UUID: id,
				MergeDoc: &objects.MergeDocument{
					Class:      "TestClass",
					ID:         id,
					UpdateTime: 123456789,
					PrimitiveSchema: map[string]interface{}{
						"title": "Updated Title",
					},
					PropertiesToDelete: []string{"content"}, // Delete content property
				},
			},
		}

		patchResult, err := db.BatchPatchObjects(ctx, patchObjects, nil, 2)
		require.Nil(t, err)
		require.Len(t, patchResult, 1)
		assert.Nil(t, patchResult[0].Err)

		// Verify: content should be deleted
		obj, err := db.ObjectByID(ctx, id, nil, additional.Properties{}, "")
		require.Nil(t, err)
		require.NotNil(t, obj)
		props := obj.Schema.(map[string]interface{})
		assert.Equal(t, "Updated Title", props["title"])
		assert.NotContains(t, props, "content")         // Deleted
		assert.Equal(t, "Test Author", props["author"]) // Preserved
	})

	t.Run("handle errors for non-existent objects", func(t *testing.T) {
		// Setup: Create temporary DB
		dirName := t.TempDir()
		shardState := singleShardState()

		schemaGetter := &fakeSchemaGetter{
			schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
			shardState: shardState,
		}

		mockSchemaReader := schemaUC.NewMockSchemaReader(t)
		mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
		mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything).RunAndReturn(func(className string, readFunc func(*models.Class, *sharding.State) error) error {
			class := &models.Class{Class: className}
			return readFunc(class, shardState)
		}).Maybe()
		mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
		mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()

		mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
		mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
		mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()

		mockNodeSelector := cluster.NewMockNodeSelector(t)
		mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
		mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()

		db, err := New(logger, "node1", Config{
			MemtablesFlushDirtyAfter:  60,
			RootPath:                  dirName,
			QueryMaximumResults:       10000,
			MaxImportGoroutinesFactor: 1,
		}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil, nil,
			mockNodeSelector, mockSchemaReader, mockReplicationFSMReader)
		require.Nil(t, err)
		db.SetSchemaGetter(schemaGetter)
		require.Nil(t, db.WaitForStartup(testCtx()))
		defer db.Shutdown(ctx)

		migrator := NewMigrator(db, logger, "node1")
		class := &models.Class{
			Class:               "TestClass",
			VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
			InvertedIndexConfig: invertedConfig(),
			Properties: []*models.Property{
				{
					Name:     "title",
					DataType: schema.DataTypeText.PropString(),
				},
			},
		}

		require.Nil(t, migrator.AddClass(ctx, class))

		schemaGetter.schema = schema.Schema{
			Objects: &models.Schema{
				Classes: []*models.Class{class},
			},
		}

		// Try to patch non-existent object
		nonExistentID := strfmt.UUID("00000000-0000-0000-0000-999999999999")

		patchObjects := objects.BatchObjects{
			{
				OriginalIndex: 0,
				Object: &models.Object{
					Class: "TestClass",
					ID:    nonExistentID,
					Properties: map[string]interface{}{
						"title": "Updated Title",
					},
				},
				UUID: nonExistentID,
				MergeDoc: &objects.MergeDocument{
					Class:      "TestClass",
					ID:         nonExistentID,
					UpdateTime: 123456789,
					PrimitiveSchema: map[string]interface{}{
						"title": "Updated Title",
					},
					PropertiesToDelete: []string{},
				},
			},
		}

		patchResult, err := db.BatchPatchObjects(ctx, patchObjects, nil, 1)
		require.Nil(t, err)
		require.Len(t, patchResult, 1)

		// Should have an error for the non-existent object
		assert.NotNil(t, patchResult[0].Err)
		assert.Contains(t, patchResult[0].Err.Error(), "not found")
	})
}

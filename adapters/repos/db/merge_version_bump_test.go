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

//go:build integrationTest

// Package db B3 regression test: MergeObject (PATCH) must bump Version so that
// an outstanding If-Match ETag is invalidated after a merge.
//
// Without the fix, the merge path left obj.Version unchanged. A subsequent
// If-Match:N PUT would pass even though a PATCH had already modified the object
// after version N - a silent lost update through the CAS contract.
package db

import (
	"context"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/objects"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// TestB3_MergeObjectBumpsVersion verifies that calling MergeObject (PATCH) on
// an existing object increments obj.Version from prevObj.Version+1. Without
// this bump, a stale If-Match ETag would not be invalidated after a PATCH,
// allowing a subsequent PUT with the old ETag to silently overwrite the merge.
//
// Test flow (matches the reviewer's lost-update scenario):
//  1. PUT object → shard mints Version = 1.
//  2. PATCH (MergeObject) → shard must mint Version = 2.
//  3. Read the object back → Version must be 2 on disk.
//
// Causal link: before the fix, mergeObjectInStorage never touched obj.Version.
// After PATCH the object still carried Version = 1 on disk. A subsequent
// If-Match:1 PUT would succeed against the merged object - the precise lost-update
// the CAS contract must prevent. This test fails on the unfixed tree where
// afterMerge.Version == 1 (not 2).
func TestB3_MergeObjectBumpsVersion(t *testing.T) {
	orig := storobj.GetWriteMarshallerVersion()
	storobj.SetWriteMarshallerVersion(2)
	defer storobj.SetWriteMarshallerVersion(orig)

	ctx := context.Background()
	const className = "MergeVersionBumpTest"
	dirName := t.TempDir()

	logger, _ := test.NewNullLogger()
	shardState := singleShardState()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: shardState,
	}

	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(cn string, _ bool, fn func(*models.Class, *sharding.State) error) error {
			return fn(&models.Class{Class: cn}, shardState)
		}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()

	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()

	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()

	repo, err := New(logger, "node1", Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  dirName,
		MaxImportGoroutinesFactor: 1,
		TrackVectorDimensions:     true,
	}, &FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{}, &FakeReplicationClient{}, nil, nil,
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader)
	require.NoError(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.NoError(t, repo.WaitForStartup(testCtx()))
	defer repo.Shutdown(ctx)

	migrator := NewMigrator(repo, logger, "node1")
	cls := &models.Class{
		Class:               className,
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
	}
	require.NoError(t, migrator.AddClass(ctx, cls))
	schemaGetter.schema = schema.Schema{Objects: &models.Schema{Classes: []*models.Class{cls}}}

	objectID := strfmt.UUID("b3b3b3b3-b3b3-4b3b-8b3b-b3b3b3b3b3b3")

	shard := getDurabilityShard(t, repo, className)

	// Step 1: PUT the object → Version should become 1.
	putObj := &storobj.Object{
		MarshallerVersion: 2,
		Object: models.Object{
			ID:                 objectID,
			Class:              className,
			LastUpdateTimeUnix: time.Now().UnixMilli(),
			Properties:         map[string]interface{}{"field": "initial"},
		},
	}
	require.NoError(t, shard.PutObject(ctx, putObj), "initial PUT must succeed")

	afterPut, err := shard.ObjectByID(ctx, objectID, search.SelectProperties{}, additional.Properties{})
	require.NoError(t, err)
	require.NotNil(t, afterPut)
	assert.Equal(t, uint64(1), afterPut.Version,
		"after PUT: Version must be 1 (first write produces Version=1)")

	// Step 2: PATCH (MergeObject) → Version must become 2.
	mergeDoc := objects.MergeDocument{
		Class:           className,
		ID:              objectID,
		UpdateTime:      time.Now().UnixMilli(),
		PrimitiveSchema: map[string]interface{}{"field": "merged"},
	}
	require.NoError(t, shard.MergeObject(ctx, mergeDoc), "PATCH must succeed")

	// Step 3: Read back → Version must be 2.
	afterMerge, err := shard.ObjectByID(ctx, objectID, search.SelectProperties{}, additional.Properties{})
	require.NoError(t, err)
	require.NotNil(t, afterMerge)
	assert.Equal(t, uint64(2), afterMerge.Version,
		"after PATCH: Version must be 2 (merge bumps from prevObj.Version+1); "+
			"without this bump a stale If-Match:1 PUT would silently overwrite the merge")
}

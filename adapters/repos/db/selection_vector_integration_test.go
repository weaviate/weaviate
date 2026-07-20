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

package db

import (
	"context"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/memwatch"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestVectorSelectionPagination(t *testing.T) {
	className := "VectorSelectionPaging"
	const total = 20
	const windowSize = 10
	const pageSize = 5

	dirName := t.TempDir()
	logger, _ := test.NewNullLogger()

	shardState := singleShardState()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: shardState,
	}
	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
		class := &models.Class{Class: className}
		return readFunc(class, shardState)
	}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().HasActiveReplicationForShard(mock.Anything, mock.Anything).Return(false).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()
	repo, err := New(logger, "node1", Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{}, &FakeReplicationClient{}, nil, memwatch.NewDummyMonitor(),
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))
	defer repo.Shutdown(context.Background())
	migrator := NewMigrator(repo, logger, "node1")

	class := &models.Class{
		Class:               className,
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
	}
	require.Nil(t, migrator.AddClass(context.Background(), class))
	schemaGetter.schema = schema.Schema{Objects: &models.Schema{Classes: []*models.Class{class}}}

	// Spread vectors along a line so the nearest-neighbour order is well-defined.
	for i := 0; i < total; i++ {
		vec := []float32{float32(i) / float32(total), 1 - float32(i)/float32(total), 0.25}
		obj := &models.Object{
			ID:    strfmt.UUID(uuid.Must(uuid.NewRandom()).String()),
			Class: className,
		}
		require.Nil(t, repo.PutObject(context.Background(), obj, vec, nil, nil, nil, 0))
	}

	queryVec := []float32{0.1, 0.9, 0.25}

	// Compose the same steps the traverser makes for a per-window MMR page: fetch deep
	// enough to reach offset+windowSize, diversify only the [offset:offset+windowSize]
	// window, then keep its top MMR.Limit.
	sel := &searchparams.Selection{MMR: &searchparams.SelectionMMR{Limit: uint32(pageSize), Balance: 0.5}}
	window := func(res []search.Result, offset, size int) []search.Result {
		if offset < 0 {
			offset = 0
		}
		if offset >= len(res) {
			return []search.Result{}
		}
		end := offset + size
		if end > len(res) {
			end = len(res)
		}
		return res[offset:end]
	}
	runSearch := func(offset int) []search.Result {
		candidates, err := repo.VectorSearch(context.Background(), dto.GetParams{
			ClassName: className,
			Pagination: &filters.Pagination{
				Offset: 0,
				Limit:  offset + windowSize,
			},
			AdditionalProperties: additional.Properties{Vector: true},
		}, []string{""}, []models.Vector{queryVec})
		require.Nil(t, err)

		diversified, err := repo.DiversifyResults(context.Background(), sel, className, "",
			window(candidates, offset, windowSize), true)
		require.Nil(t, err)

		return window(diversified, 0, pageSize)
	}

	ids := func(res []search.Result) []string {
		out := make([]string, len(res))
		for i := range res {
			out[i] = res[i].ID.String()
		}
		return out
	}

	page1 := runSearch(0)
	page2 := runSearch(windowSize)

	require.Len(t, page1, pageSize, "page1 should return MMR.Limit results")
	require.Len(t, page2, pageSize, "deep page should return real results, not empty")

	ids1, ids2 := ids(page1), ids(page2)

	// Disjoint windows ⇒ no overlap between pages.
	seen := map[string]bool{}
	for _, id := range ids1 {
		seen[id] = true
	}
	for _, id := range ids2 {
		require.Falsef(t, seen[id], "id %s appears on both page1 and page2", id)
	}

	// Same offset is deterministic.
	require.Equal(t, ids1, ids(runSearch(0)), "same offset must return the same page")

	// Paging past the dataset (no data, not an artificial cap) returns empty.
	require.Empty(t, runSearch(total), "a window past the dataset returns empty")
}

func TestVectorSelectionMultiShardGlobalDiversity(t *testing.T) {
	className := "VectorSelectionMultiShard"
	const total = 30
	const window = 20
	const pageSize = 5

	dirName := t.TempDir()
	logger, _ := test.NewNullLogger()

	shardState := multiShardState()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: shardState,
	}
	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
		class := &models.Class{Class: className}
		return readFunc(class, shardState)
	}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().HasActiveReplicationForShard(mock.Anything, mock.Anything).Return(false).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()
	repo, err := New(logger, "node1", Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{}, &FakeReplicationClient{}, nil, memwatch.NewDummyMonitor(),
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))
	defer repo.Shutdown(context.Background())
	migrator := NewMigrator(repo, logger, "node1")

	class := &models.Class{
		Class:               className,
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
	}
	require.Nil(t, migrator.AddClass(context.Background(), class))
	schemaGetter.schema = schema.Schema{Objects: &models.Schema{Classes: []*models.Class{class}}}

	require.Greater(t, len(shardState.AllPhysicalShards()), 1, "test requires more than one shard")

	perShard := map[string]int{}
	for i := 0; i < total; i++ {
		vec := []float32{float32(i) / float32(total), 1 - float32(i)/float32(total), 0.25}
		id := strfmt.UUID(uuid.Must(uuid.NewRandom()).String())
		obj := &models.Object{ID: id, Class: className}
		require.Nil(t, repo.PutObject(context.Background(), obj, vec, nil, nil, nil, 0))
		perShard[shardState.PhysicalShard([]byte(id))]++
	}
	require.Greater(t, len(perShard), 1, "objects must be spread across multiple shards")

	queryVec := []float32{0.1, 0.9, 0.25}

	candidates, err := repo.VectorSearch(context.Background(), dto.GetParams{
		ClassName:            className,
		Pagination:           &filters.Pagination{Offset: 0, Limit: window},
		AdditionalProperties: additional.Properties{Vector: true},
	}, []string{""}, []models.Vector{queryVec})
	require.Nil(t, err)
	require.Len(t, candidates, window)

	for i := 1; i < len(candidates); i++ {
		require.LessOrEqualf(t, candidates[i-1].Dist, candidates[i].Dist,
			"merged candidates must be globally distance-sorted across shards")
	}

	ids := func(res []search.Result) []string {
		out := make([]string, len(res))
		for i := range res {
			out[i] = res[i].ID.String()
		}
		return out
	}

	pureRel := &searchparams.Selection{MMR: &searchparams.SelectionMMR{Limit: pageSize, Balance: 1}}
	diversified, err := repo.DiversifyResults(context.Background(), pureRel, className, "", candidates, true)
	require.Nil(t, err)
	require.Equal(t, ids(candidates[:pageSize]), ids(diversified[:pageSize]))

	diverse := &searchparams.Selection{MMR: &searchparams.SelectionMMR{Limit: pageSize, Balance: 0.3}}
	page, err := repo.DiversifyResults(context.Background(), diverse, className, "", candidates, true)
	require.Nil(t, err)
	require.GreaterOrEqual(t, len(page), pageSize)
	require.Equal(t, candidates[0].ID, page[0].ID, "MMR seed must be the globally most relevant result")

	seen := map[string]bool{}
	for _, id := range ids(page[:pageSize]) {
		require.Falsef(t, seen[id], "id %s selected twice", id)
		seen[id] = true
	}
}

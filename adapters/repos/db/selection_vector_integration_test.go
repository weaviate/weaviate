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

// TestVectorSelectionPagination: MMR pages must tile a single stable diversified
// ordering. Query Limit = candidate pool; MMR.Limit = page size; Offset pages the pool.
func TestVectorSelectionPagination(t *testing.T) {
	className := "VectorSelectionPaging"
	const total = 20
	const pool = 20
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

	// Compose the same two repo calls the traverser makes: fetch an offset-independent
	// pool, diversify, then paginate.
	sel := &searchparams.Selection{MMR: &searchparams.SelectionMMR{Limit: 0, Balance: 0.5}}
	runSearch := func(offset int, mmrLimit uint32) []search.Result {
		candidates, err := repo.VectorSearch(context.Background(), dto.GetParams{
			ClassName: className,
			Pagination: &filters.Pagination{
				Offset: 0,
				Limit:  pool,
			},
			AdditionalProperties: additional.Properties{Vector: true},
		}, []string{""}, []models.Vector{queryVec})
		require.Nil(t, err)

		diversified, err := repo.DiversifyResults(context.Background(), sel, className, "", candidates, true)
		require.Nil(t, err)

		if offset >= len(diversified) {
			return []search.Result{}
		}
		end := offset + int(mmrLimit)
		if end > len(diversified) {
			end = len(diversified)
		}
		return diversified[offset:end]
	}

	ids := func(res []search.Result) []string {
		out := make([]string, len(res))
		for i := range res {
			out[i] = res[i].ID.String()
		}
		return out
	}

	page1 := runSearch(0, pageSize)
	page2 := runSearch(pageSize, pageSize)
	full := runSearch(0, 2*pageSize)

	require.Len(t, page1, pageSize, "page1 should return MMR.Limit results")
	require.Len(t, page2, pageSize, "page2 should return MMR.Limit results")
	require.Len(t, full, 2*pageSize)

	ids1, ids2, idsFull := ids(page1), ids(page2), ids(full)

	// No overlap between consecutive pages.
	seen := map[string]bool{}
	for _, id := range ids1 {
		seen[id] = true
	}
	for _, id := range ids2 {
		require.Falsef(t, seen[id], "id %s appears on both page1 and page2", id)
	}

	// Pages tile a single stable diversified ordering.
	require.Equal(t, idsFull[:pageSize], ids1, "page1 must equal full[:pageSize]")
	require.Equal(t, idsFull[pageSize:2*pageSize], ids2, "page2 must equal full[pageSize:2*pageSize]")

	// Regression: paging past the pool must return empty, never duplicates.
	big := runSearch(0, total*2)
	beyond := runSearch(total*2, total*2)
	require.Len(t, big, total, "page size > pool returns the whole pool once")
	require.Empty(t, beyond, "paging beyond the pool returns empty, not duplicates")
}

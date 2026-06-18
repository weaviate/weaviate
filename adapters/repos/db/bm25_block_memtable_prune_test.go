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
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/memwatch"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// TestBM25FBlockMemtableImpactNotPruned exercises the full
// shard.ObjectSearch -> bm25_searcher_block.go path (term creation, SetIdf,
// per-segment DoBlockMaxWand, combineResults) for a memtable-resident term.
//
// A memtable term that leaves its synthesized BlockEntry max-impact pair at
// zero gets a zero BlockMax-WAND upper bound, so its documents are silently
// pruned once the per-segment result heap fills. The existing block harness
// (TestBM25FCompareBlock) never triggered this because it searches with
// limit=1000 over ~7 docs: the heap never fills, so the zero bound is harmless.
//
// To force pruning the filler term must occupy more than the per-segment
// internal limit, which bm25_searcher_block.go bumps to max(limit*1.1,
// limit+10) — so a low caller limit alone is not enough; the segment needs
// >internalLimit matching docs. The rare doc is inserted last (highest docID,
// scored last by WAND, after the heap is full) and matches only the rare term,
// so its survival depends entirely on that term's WAND upper bound.
func TestBM25FBlockMemtableImpactNotPruned(t *testing.T) {
	config.DefaultUsingBlockMaxWAND = true
	ctx := context.Background()
	logger := logrus.New()
	repo, schemaGetter := newBM25BlockTestRepo(t, logger)
	defer repo.Shutdown(ctx)

	const (
		fillerTerm = "filler"
		rareTerm   = "rare"
		rareText   = "rare rare rare rare rare rare rare rare"
		// internalLimit for callerLimit=1 is max(1.1, 11)=11, so the filler term
		// needs more than that in a single segment to fill the WAND heap.
		fillerCount = 20
		callerLimit = 1
		// no pruning happens here: internalLimit >> doc count, so this is the
		// ground-truth ranking the limited search must reproduce.
		oracleLimit = fillerCount + 10
	)

	cases := []struct {
		name string
		// flushAfterFiller flushes the first filler batch to disk, then leaves a
		// fresh filler batch + the rare doc memtable-resident on top of it.
		flushAfterFiller bool
		// flushAll flushes everything so the term comes from a disk segment
		// (which populates max-impact correctly) — a control that must stay green.
		flushAll bool
	}{
		{name: "memtable_resident"},
		{name: "memtable_over_flushed_segment", flushAfterFiller: true},
		{name: "disk_resident", flushAll: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			className := "BM25Block_" + tc.name
			shard := setupSingleTextClass(t, repo, schemaGetter, logger, className)

			docID := 0
			put := func(text string) uint64 {
				putTextDoc(t, repo, className, docID, text)
				id := uint64(docID)
				docID++
				return id
			}

			if tc.flushAfterFiller {
				// first half on disk, second half + rare memtable-resident on top
				for i := 0; i < fillerCount; i++ {
					put(fillerTerm)
				}
				require.NoError(t, shard.Store().FlushMemtables(ctx))
				for i := 0; i < fillerCount; i++ {
					put(fillerTerm)
				}
			} else {
				for i := 0; i < fillerCount; i++ {
					put(fillerTerm)
				}
			}
			rareID := put(rareText)

			if tc.flushAll {
				require.NoError(t, shard.Store().FlushMemtables(ctx))
			}

			search := func(limit int) ([]uint64, []float32) {
				kwr := &searchparams.KeywordRanking{
					Type:       "bm25",
					Properties: []string{"text"},
					Query:      fillerTerm + " " + rareTerm,
				}
				objs, scores, err := shard.ObjectSearch(ctx, limit, nil, kwr, nil, nil,
					additional.Properties{}, []string{"text"})
				require.NoError(t, err)
				ids := make([]uint64, len(objs))
				for i, o := range objs {
					ids[i] = o.DocID
				}
				return ids, scores
			}

			oracleIDs, _ := search(oracleLimit)
			require.NotEmpty(t, oracleIDs)
			require.Equal(t, rareID, oracleIDs[0],
				"sanity: the rare high-tf doc must top the ranking when nothing is pruned")

			limitedIDs, _ := search(callerLimit)
			require.NotEmpty(t, limitedIDs)
			require.Equal(t, rareID, limitedIDs[0],
				"rare memtable doc (id %d) was pruned by BlockMax-WAND once the heap filled; got %v",
				rareID, limitedIDs)
		})
	}
}

func putTextDoc(t *testing.T, repo *DB, className string, idx int, text string) {
	t.Helper()
	id := strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", idx)).String())
	obj := &models.Object{
		Class:              className,
		ID:                 id,
		Properties:         map[string]interface{}{"text": text},
		CreationTimeUnix:   1565612833955,
		LastUpdateTimeUnix: 10000020,
	}
	require.NoError(t, repo.PutObject(context.Background(), obj, []float32{1, 3, 5, 0.4}, nil, nil, nil, 0))
}

func setupSingleTextClass(t *testing.T, repo *DB, schemaGetter *fakeSchemaGetter, logger logrus.FieldLogger, className string) ShardLike {
	t.Helper()
	vFalse, vTrue := false, true
	class := &models.Class{
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: BM25FinvertedConfig(1.2, 0.75, "none"),
		Class:               className,
		Properties: []*models.Property{
			{
				Name:            "text",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationWord,
				IndexFilterable: &vFalse,
				IndexSearchable: &vTrue,
			},
		},
	}
	schemaGetter.schema = schema.Schema{Objects: &models.Schema{Classes: []*models.Class{class}}}
	migrator := NewMigrator(repo, logger, "node1")
	migrator.AddClass(context.Background(), class)

	idx := repo.GetIndex(schema.ClassName(className))
	require.NotNil(t, idx)
	var shard ShardLike
	require.NoError(t, idx.ForEachShard(func(_ string, s ShardLike) error {
		shard = s
		return nil
	}))
	require.NotNil(t, shard)
	return shard
}

func newBM25BlockTestRepo(t *testing.T, logger logrus.FieldLogger) (*DB, *fakeSchemaGetter) {
	t.Helper()
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
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()
	repo, err := New(logger, "node1", Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  t.TempDir(),
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{}, nil, nil, memwatch.NewDummyMonitor(),
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(context.TODO()))
	return repo, schemaGetter
}

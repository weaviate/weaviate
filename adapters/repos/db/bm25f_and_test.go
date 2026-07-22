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
	"strings"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/graphql/local/common_filters"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/memwatch"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestBM25FJourneyBlockAnd(t *testing.T) {
	config.DefaultUsingBlockMaxWAND = true
	dirName := t.TempDir()

	logger := logrus.New()
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
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()
	repo, err := New(logger, "node1", Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{}, nil, nil, memwatch.NewDummyMonitor(),
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(context.Background()))
	defer repo.Shutdown(context.Background())

	props, _ := SetupClass(t, repo, schemaGetter, logger, 1.2, 0.75, "none")

	idx := repo.GetIndex("MyClass")

	require.NotNil(t, idx)

	// Check basic search
	addit := additional.Properties{}
	for _, location := range []string{"memory", "disk"} {
		t.Run("bm25f text with AND "+location, func(t *testing.T) {
			kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title", "description"}, Query: "This is how we get to BM25F", SearchOperator: common_filters.SearchOperatorAnd}
			res, scores, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)
			// Print results
			t.Log("--- Start results for search with AND ---")
			for i, r := range res {
				t.Logf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.DocID, scores[i], r.Object.Properties.(map[string]interface{})["title"], r.Object.Properties.(map[string]interface{})["description"], r.Object.Additional)
			}
			require.Nil(t, err)

			// Check results in correct order
			require.Equal(t, uint64(0), res[0].DocID)
		})

		t.Run("bm25f text with AND == minimum should match with len(queryTerms) "+location, func(t *testing.T) {
			q := "This is how we get to BM25F right?"
			kwr1 := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title", "description"}, Query: q, SearchOperator: common_filters.SearchOperatorAnd}
			res1, scores1, err := idx.objectSearch(context.TODO(), 1000, nil, kwr1, nil, nil, addit, nil, "", 0, props)

			require.Nil(t, err)

			kwr2 := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title", "description"}, Query: q, SearchOperator: common_filters.SearchOperatorOr, MinimumOrTokensMatch: len(strings.Split(q, " "))}
			res2, scores2, err := idx.objectSearch(context.TODO(), 1000, nil, kwr2, nil, nil, addit, nil, "", 0, props)

			require.Nil(t, err)
			// Print results
			t.Log("--- Start results for search with AND ---")
			for i, r := range res1 {
				t.Logf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.DocID, scores1[i], r.Object.Properties.(map[string]interface{})["title"], r.Object.Properties.(map[string]interface{})["description"], r.Object.Additional)
			}
			for i, r := range res2 {
				t.Logf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.DocID, scores2[i], r.Object.Properties.(map[string]interface{})["title"], r.Object.Properties.(map[string]interface{})["description"], r.Object.Additional)
			}

			require.Equal(t, len(res1), len(res2))

			for i := 0; i < len(res1); i++ {
				require.Equal(t, res1[i].DocID, res2[i].DocID)
				require.Equal(t, scores1[i], scores2[i])
			}
		})

		// depending on the minimum should match, we will have a different number of results showing up
		expectedSizes := []int{3, 3, 2, 2, 2, 2, 1, 1}
		for minimumOrTokensMatch, expectedSize := range expectedSizes {
			t.Run("bm25f text with minimum should match with 0...len(queryTerms) "+location, func(t *testing.T) {
				kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title", "description"}, Query: "This is how we get to BM25F", MinimumOrTokensMatch: minimumOrTokensMatch}
				res, scores, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)
				// Print results
				t.Log("--- Start results for search with AND ---")
				for i, r := range res {
					t.Logf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.DocID, scores[i], r.Object.Properties.(map[string]interface{})["title"], r.Object.Properties.(map[string]interface{})["description"], r.Object.Additional)
				}
				require.Nil(t, err)
				require.Equal(t, expectedSize, len(res))
				require.Equal(t, uint64(0), res[0].DocID)

				// if minimumOrTokensMatch < 3, title and description will both match, and thus the score will be higher
				if minimumOrTokensMatch < 3 {
					EqualFloats(t, scores[0], 5.470736, 3)
				} else {
					EqualFloats(t, scores[0], 4.0164075, 3)
				}
			})
		}

	}
}

func TestBM25FJourneyAnd(t *testing.T) {
	config.DefaultUsingBlockMaxWAND = false
	dirName := t.TempDir()

	logger := logrus.New()
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
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()
	repo, err := New(logger, "node1", Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{}, nil, nil, memwatch.NewDummyMonitor(),
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(context.Background()))
	defer repo.Shutdown(context.Background())

	props, _ := SetupClass(t, repo, schemaGetter, logger, 1.2, 0.75, "none")

	idx := repo.GetIndex("MyClass")

	require.NotNil(t, idx)

	// Check basic search
	addit := additional.Properties{}
	for _, location := range []string{"memory", "disk"} {
		t.Run("bm25f text with AND "+location, func(t *testing.T) {
			kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title", "description"}, Query: "This is how we get to BM25F", SearchOperator: common_filters.SearchOperatorAnd}
			res, scores, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)
			// Print results
			t.Log("--- Start results for search with AND ---")
			for i, r := range res {
				t.Logf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.DocID, scores[i], r.Object.Properties.(map[string]interface{})["title"], r.Object.Properties.(map[string]interface{})["description"], r.Object.Additional)
			}
			require.Nil(t, err)

			// Check results in correct order
			require.Equal(t, uint64(0), res[0].DocID)
		})

		t.Run("bm25f text with AND == minimum should match with len(queryTerms) "+location, func(t *testing.T) {
			q := "This is how we get to BM25F right?"
			kwr1 := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title", "description"}, Query: q, SearchOperator: common_filters.SearchOperatorAnd}
			res1, scores1, err := idx.objectSearch(context.TODO(), 1000, nil, kwr1, nil, nil, addit, nil, "", 0, props)

			require.Nil(t, err)

			kwr2 := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title", "description"}, Query: q, SearchOperator: common_filters.SearchOperatorOr, MinimumOrTokensMatch: len(strings.Split(q, " "))}
			res2, scores2, err := idx.objectSearch(context.TODO(), 1000, nil, kwr2, nil, nil, addit, nil, "", 0, props)

			require.Nil(t, err)
			// Print results
			t.Log("--- Start results for search with AND ---")
			for i, r := range res1 {
				t.Logf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.DocID, scores1[i], r.Object.Properties.(map[string]interface{})["title"], r.Object.Properties.(map[string]interface{})["description"], r.Object.Additional)
			}
			for i, r := range res2 {
				t.Logf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.DocID, scores2[i], r.Object.Properties.(map[string]interface{})["title"], r.Object.Properties.(map[string]interface{})["description"], r.Object.Additional)
			}

			require.Equal(t, len(res1), len(res2))

			for i := 0; i < len(res1); i++ {
				require.Equal(t, res1[i].DocID, res2[i].DocID)
				require.Equal(t, scores1[i], scores2[i])
			}
		})

		// depending on the minimum should match, we will have a different number of results showing up
		expectedSizes := []int{3, 3, 2, 2, 2, 2, 1, 1}
		for minimumOrTokensMatch, expectedSize := range expectedSizes {
			t.Run("bm25f text with minimum should match with 0...len(queryTerms) "+location, func(t *testing.T) {
				kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title", "description"}, Query: "This is how we get to BM25F", MinimumOrTokensMatch: minimumOrTokensMatch}
				res, scores, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)
				// Print results
				t.Log("--- Start results for search with AND ---")
				for i, r := range res {
					t.Logf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.DocID, scores[i], r.Object.Properties.(map[string]interface{})["title"], r.Object.Properties.(map[string]interface{})["description"], r.Object.Additional)
				}
				require.Nil(t, err)
				require.Equal(t, expectedSize, len(res))
				require.Equal(t, uint64(0), res[0].DocID)

				EqualFloats(t, scores[0], 3.4539468, 3)
			})
		}

	}
}

func TestBM25FCrossPropertyAnd(t *testing.T) {
	config.DefaultUsingBlockMaxWAND = true

	logger := logrus.New()
	repo, schemaGetter := newBM25BlockTestRepo(t, logger)
	defer repo.Shutdown(context.Background())

	props, _ := SetupClass(t, repo, schemaGetter, logger, 1.2, 0.75, "none")
	idx := repo.GetIndex("MyClass")
	require.NotNil(t, idx)

	addit := additional.Properties{}
	searchProps := []string{"title", "description"}
	// doc 3 is title "An unrelated title", description "Actually all about journey".
	// "unrelated" appears only in titles (docs 3 and 7); "journey" appears in many
	// descriptions/titles. Only doc 3 has BOTH tokens — and they sit in different
	// properties, so it matches cross-property AND but not per-property AND.
	const query = "unrelated journey"

	idSet := func(res []*storobj.Object) map[uint64]struct{} {
		out := make(map[uint64]struct{}, len(res))
		for _, r := range res {
			out[r.DocID] = struct{}{}
		}
		return out
	}
	scoreFor := func(res []*storobj.Object, scores []float32, id uint64) (float32, bool) {
		for i, r := range res {
			if r.DocID == id {
				return scores[i], true
			}
		}
		return 0, false
	}

	kwrCross := &searchparams.KeywordRanking{Type: "bm25", Properties: searchProps, Query: query, SearchOperator: common_filters.SearchOperatorAndCross}
	resCross, scoresCross, err := idx.objectSearch(context.TODO(), 1000, nil, kwrCross, nil, nil, addit, nil, "", 0, props)
	require.Nil(t, err)

	kwrAnd := &searchparams.KeywordRanking{Type: "bm25", Properties: searchProps, Query: query, SearchOperator: common_filters.SearchOperatorAnd}
	resAnd, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwrAnd, nil, nil, addit, nil, "", 0, props)
	require.Nil(t, err)

	kwrOr := &searchparams.KeywordRanking{Type: "bm25", Properties: searchProps, Query: query, SearchOperator: common_filters.SearchOperatorOr}
	resOr, scoresOr, err := idx.objectSearch(context.TODO(), 1000, nil, kwrOr, nil, nil, addit, nil, "", 0, props)
	require.Nil(t, err)

	crossIDs := idSet(resCross)
	orIDs := idSet(resOr)

	// cross-property AND matches exactly doc 3
	require.Len(t, resCross, 1, "cross-property AND should match exactly one doc")
	require.Contains(t, crossIDs, uint64(3))

	// per-property AND matches nothing: no single property holds both tokens
	require.Empty(t, resAnd, "per-property AND should not match when tokens are split across properties")

	// cross-property AND is a strict subset of OR, which also includes doc 3
	require.Contains(t, orIDs, uint64(3))
	for id := range crossIDs {
		require.Contains(t, orIDs, id, "cross-property AND result must be a subset of OR")
	}
	require.GreaterOrEqual(t, len(resOr), len(resCross))

	// scoring is unchanged: doc 3's cross-property AND score equals its OR score,
	// since cross-property AND only filters and sums the same per-property scores.
	sCross, okCross := scoreFor(resCross, scoresCross, 3)
	sOr, okOr := scoreFor(resOr, scoresOr, 3)
	require.True(t, okCross)
	require.True(t, okOr)
	EqualFloats(t, sCross, sOr, 4)
}

func TestBM25FCrossPropertyAndNonInverted(t *testing.T) {
	config.DefaultUsingBlockMaxWAND = false

	logger := logrus.New()
	repo, schemaGetter := newBM25BlockTestRepo(t, logger)
	defer repo.Shutdown(context.Background())

	props, _ := SetupClass(t, repo, schemaGetter, logger, 1.2, 0.75, "none")
	idx := repo.GetIndex("MyClass")
	require.NotNil(t, idx)

	kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title", "description"}, Query: "unrelated journey", SearchOperator: common_filters.SearchOperatorAndCross}
	_, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, additional.Properties{}, nil, "", 0, props)
	require.ErrorContains(t, err, "BlockMax WAND")
}

// setupMixedTokenizationClass builds a class with two searched properties that use
// different tokenizations (word vs. whitespace), which fragments cross-property AND
// into two tokenization groups instead of the single group it requires.
func setupMixedTokenizationClass(t require.TestingT, repo *DB, schemaGetter *fakeSchemaGetter, logger logrus.FieldLogger) []string {
	vFalse := false
	vTrue := true

	class := &models.Class{
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: BM25FinvertedConfig(1.2, 0.75, "none"),
		Class:               "MixedTokClass",
		Properties: []*models.Property{
			{
				Name:            "titleWord",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationWord,
				IndexFilterable: &vFalse,
				IndexSearchable: &vTrue,
			},
			{
				Name:            "descWhitespace",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationWhitespace,
				IndexFilterable: &vFalse,
				IndexSearchable: &vTrue,
			},
		},
	}

	props := make([]string, len(class.Properties))
	for i, prop := range class.Properties {
		props[i] = prop.Name
	}
	schemaGetter.schema = schema.Schema{Objects: &models.Schema{Classes: []*models.Class{class}}}

	migrator := NewMigrator(repo, logger, "node1")
	migrator.AddClass(context.Background(), class)

	// "unrelated" only sits in titleWord, "journey" only in descWhitespace: neither
	// property alone holds both query tokens, only the pair across properties does.
	data := map[string]interface{}{"titleWord": "An unrelated title", "descWhitespace": "the journey continues"}
	id := strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", 0)).String())
	obj := &models.Object{Class: "MixedTokClass", ID: id, Properties: data, CreationTimeUnix: 1565612833955, LastUpdateTimeUnix: 10000020}
	require.Nil(t, repo.PutObject(context.Background(), obj, []float32{1, 3, 5, 0.4}, nil, nil, nil, 0))

	return props
}

func TestBM25FCrossPropertyAndMixedTokenizationFails(t *testing.T) {
	config.DefaultUsingBlockMaxWAND = true

	logger := logrus.New()
	repo, schemaGetter := newBM25BlockTestRepo(t, logger)
	defer repo.Shutdown(context.Background())

	searchProps := setupMixedTokenizationClass(t, repo, schemaGetter, logger)
	idx := repo.GetIndex("MixedTokClass")
	require.NotNil(t, idx)

	addit := additional.Properties{}
	const query = "unrelated journey"

	kwrCross := &searchparams.KeywordRanking{Type: "bm25", Properties: searchProps, Query: query, SearchOperator: common_filters.SearchOperatorAndCross}
	_, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwrCross, nil, nil, addit, nil, "", 0, searchProps)
	require.ErrorContains(t, err, "tokenization")

	// the same properties stay searchable under the other operators
	kwrAnd := &searchparams.KeywordRanking{Type: "bm25", Properties: searchProps, Query: query, SearchOperator: common_filters.SearchOperatorAnd}
	resAnd, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwrAnd, nil, nil, addit, nil, "", 0, searchProps)
	require.Nil(t, err)
	require.Empty(t, resAnd, "per-property AND should not match when tokens are split across properties")

	kwrOr := &searchparams.KeywordRanking{Type: "bm25", Properties: searchProps, Query: query, SearchOperator: common_filters.SearchOperatorOr}
	resOr, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwrOr, nil, nil, addit, nil, "", 0, searchProps)
	require.Nil(t, err)
	require.Len(t, resOr, 1, "OR should match since each property holds at least one query token")
}

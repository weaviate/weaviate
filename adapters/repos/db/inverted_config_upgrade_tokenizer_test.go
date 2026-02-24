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
	"github.com/weaviate/weaviate/entities/tokenizer"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/memwatch"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func BM25FinvertedUserDictConfig(k1, b float32) *models.InvertedIndexConfig {
	ptr := func(s string) *string { return &s }
	return &models.InvertedIndexConfig{
		Bm25: &models.BM25Config{
			K1: k1,
			B:  b,
		},
		CleanupIntervalSeconds: 60,
		Stopwords: &models.StopwordConfig{
			Preset: "en",
		},
		IndexNullState:      true,
		IndexPropertyLength: true,
		UsingBlockMaxWAND:   config.DefaultUsingBlockMaxWAND,
		TokenizerUserDict: []*models.TokenizerUserDictConfig{
			{
				Tokenizer: models.PropertyTokenizationKagomeKr,
				Replacements: []*models.TokenizerUserDictConfigReplacementsItems0{
					{
						Source: ptr("Weaviate"),
						Target: ptr("We Aviate"),
					},
					{
						Source: ptr("Semi Technologies"),
						Target: ptr("SemiTechnologies"),
					},
					{
						Source: ptr("Aviate"),
						Target: ptr("Aviate"),
					},
				},
			},
		},
	}
}

func SetupUserDictClass(t require.TestingT, repo *DB, schemaGetter *fakeSchemaGetter, logger logrus.FieldLogger, k1, b float32,
) ([]string, *Migrator) {
	vFalse := false
	vTrue := true

	class := &models.Class{
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: BM25FinvertedUserDictConfig(k1, b),
		Class:               "MyClass",

		Properties: []*models.Property{
			{
				Name:            "title",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationKagomeKr,
				IndexFilterable: &vFalse,
				IndexSearchable: &vTrue,
			},
			{
				Name:            "text",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationWord,
				IndexFilterable: &vTrue,
				IndexSearchable: &vTrue,
			},
		},
	}
	props := make([]string, len(class.Properties))
	for i, prop := range class.Properties {
		props[i] = prop.Name
	}
	schema := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{class},
		},
	}

	schemaGetter.schema = schema

	migrator := NewMigrator(repo, logger, "node1")
	migrator.AddClass(context.Background(), class)

	testData := []map[string]interface{}{}
	testData = append(testData, map[string]interface{}{"title": "Weaviate is a product by Semi Technologies", "text": "Weaviate is a product by Semi Technologies"})
	testData = append(testData, map[string]interface{}{"title": "We Aviate is a product by SemiTechnologies", "text": "We Aviate is a product by SemiTechnologies"})
	testData = append(testData, map[string]interface{}{"title": "Aviate Technologies", "text": "Aviate Technologies"})
	testData = append(testData, map[string]interface{}{"title": "W e Aviate Technologies will match as tokenizer splits", "text": "W e Aviate Technologies will match as tokenizer splits"})
	testData = append(testData, map[string]interface{}{"title": "An unrelated title", "text": "An unrelated title"})

	for i, data := range testData {
		id := strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", i)).String())

		obj := &models.Object{Class: "MyClass", ID: id, Properties: data, CreationTimeUnix: 1565612833955, LastUpdateTimeUnix: 10000020}
		vector := []float32{1, 3, 5, 0.4}
		err := repo.PutObject(context.Background(), obj, vector, nil, nil, nil, 0)
		require.Nil(t, err)
	}
	return props, migrator
}

func TestBM25FUserDictTest(t *testing.T) {
	t.Setenv("ENABLE_TOKENIZER_KAGOME_KR", "true")
	config.DefaultUsingBlockMaxWAND = true
	tokenizer.InitOptionalTokenizers()

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

	props, migrator := SetupUserDictClass(t, repo, schemaGetter, logger, config.DefaultBM25k1, config.DefaultBM25b)
	className := schema.ClassName("MyClass")
	idx := repo.GetIndex(className)
	require.NotNil(t, idx)

	for _, location := range []string{"memory", "disk"} {
		t.Run("bm25f Aviate "+location, func(t *testing.T) {
			kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title"}, Query: "Aviate"}
			addit := additional.Properties{}
			res, scores, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props, nil)
			t.Log("--- Start results for singleprop search ---")
			for i, r := range res {
				t.Logf("Result id: %v, score: %v, title: %v, additional %+v\n", r.DocID, scores[i], r.Object.Properties.(map[string]interface{})["title"], r.Object.Additional)
			}
			require.Nil(t, err)
			// Check results in correct order
			require.Equal(t, uint64(2), res[0].DocID)
			require.Equal(t, uint64(0), res[1].DocID)
			require.Equal(t, uint64(1), res[2].DocID)

			// Check scores
			EqualFloats(t, float32(0.18053718), scores[0], 5)
			EqualFloats(t, float32(0.12627266), scores[1], 5)
			EqualFloats(t, float32(0.11628625), scores[2], 5)
		})

		t.Run("bm25f Weaviate "+location, func(t *testing.T) {
			kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title"}, Query: "Weaviate"}
			addit := additional.Properties{}
			res, scores, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props, nil)
			t.Log("--- Start results for singleprop search ---")
			for i, r := range res {
				t.Logf("Result id: %v, score: %v, title: %v, additional %+v\n", r.DocID, scores[i], r.Object.Properties.(map[string]interface{})["title"], r.Object.Additional)
			}
			require.Nil(t, err)
			// Check results in correct order
			require.Equal(t, uint64(0), res[0].DocID)
			require.Equal(t, uint64(2), res[1].DocID)
			require.Equal(t, uint64(1), res[2].DocID)

			// Check scores
			EqualFloats(t, float32(0.73476064), scores[0], 5)
			EqualFloats(t, float32(0.18053718), scores[1], 5)
			EqualFloats(t, float32(0.11628625), scores[2], 5)
		})

		t.Run("bm25f We Aviate "+location, func(t *testing.T) {
			kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title"}, Query: "We Aviate"}
			addit := additional.Properties{}
			res, scores, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props, nil)
			t.Log("--- Start results for singleprop search ---")
			for i, r := range res {
				t.Logf("Result id: %v, score: %v, title: %v, additional %+v\n", r.DocID, scores[i], r.Object.Properties.(map[string]interface{})["title"], r.Object.Additional)
			}
			require.Nil(t, err)
			// Check results in correct order
			require.Equal(t, uint64(1), res[0].DocID)
			require.Equal(t, uint64(3), res[1].DocID)
			require.Equal(t, uint64(4), res[2].DocID)

			// Check scores
			EqualFloats(t, float32(0.68803847), scores[0], 5)
			EqualFloats(t, float32(0.61507285), scores[1], 5)
			EqualFloats(t, float32(0.2856433), scores[2], 5)
		})

		for _, index := range repo.indices {
			index.ForEachShard(func(name string, shard ShardLike) error {
				err := shard.Store().FlushMemtables(context.Background())
				require.Nil(t, err)
				return nil
			})
		}
	}

	t.Run("update class description", func(t *testing.T) {
		class := repo.schemaGetter.ReadOnlyClass(className.String())
		class.InvertedIndexConfig.TokenizerUserDict = []*models.TokenizerUserDictConfig{}
		ctx := context.Background()
		err := migrator.UpdateInvertedIndexConfig(ctx, string(className), class.InvertedIndexConfig)
		require.Nil(t, err)
	})

	t.Run("Updated tokenizer", func(t *testing.T) {
		t.Run("bm25f Aviate", func(t *testing.T) {
			// Check boosted
			kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title"}, Query: "Aviate"}
			addit := additional.Properties{}
			res, scores, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props, nil)
			t.Log("--- Start results for singleprop search ---")
			for i, r := range res {
				t.Logf("Result id: %v, score: %v, title: %v, additional %+v\n", r.DocID, scores[i], r.Object.Properties.(map[string]interface{})["title"], r.Object.Additional)
			}
			require.Nil(t, err)
			// Check results in correct order
			require.Equal(t, uint64(4), res[0].DocID)
			require.Equal(t, uint64(1), res[1].DocID)
			require.Equal(t, uint64(3), res[2].DocID)

			// Check scores
			EqualFloats(t, float32(0.2856433), scores[0], 5)
			EqualFloats(t, float32(0.21787204), scores[1], 5)
			EqualFloats(t, float32(0.194767), scores[2], 5)
		})

		t.Run("bm25f Weaviate", func(t *testing.T) {
			// Check boosted
			kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title"}, Query: "Weaviate"}
			addit := additional.Properties{}
			res, scores, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props, nil)
			t.Log("--- Start results for singleprop search ---")
			for i, r := range res {
				t.Logf("Result id: %v, score: %v, title: %v, additional %+v\n", r.DocID, scores[i], r.Object.Properties.(map[string]interface{})["title"], r.Object.Additional)
			}
			require.Nil(t, err)
			// Check results in correct order
			require.Equal(t, uint64(4), res[0].DocID)
			require.Equal(t, uint64(1), res[1].DocID)
			require.Equal(t, uint64(3), res[2].DocID)

			// Check scores
			EqualFloats(t, float32(0.2856433), scores[0], 5)
			EqualFloats(t, float32(0.21787204), scores[1], 5)
			EqualFloats(t, float32(0.194767), scores[2], 5)
		})

		t.Run("bm25f We Aviate", func(t *testing.T) {
			// Check boosted
			kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title"}, Query: "We Aviate"}
			addit := additional.Properties{}
			res, scores, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props, nil)
			t.Log("--- Start results for singleprop search ---")
			for i, r := range res {
				t.Logf("Result id: %v, score: %v, title: %v, additional %+v\n", r.DocID, scores[i], r.Object.Properties.(map[string]interface{})["title"], r.Object.Additional)
			}
			require.Nil(t, err)
			// Check results in correct order
			require.Equal(t, uint64(1), res[0].DocID)
			require.Equal(t, uint64(3), res[1].DocID)
			require.Equal(t, uint64(4), res[2].DocID)

			// Check scores
			EqualFloats(t, float32(0.7896242), scores[0], 5)
			EqualFloats(t, float32(0.7058856), scores[1], 5)
			EqualFloats(t, float32(0.5712866), scores[2], 5)
		})
	})
}

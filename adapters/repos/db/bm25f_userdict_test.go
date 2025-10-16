//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
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
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers/tokenizer"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"

	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

func BM25FinvertedUserDictConfig(k1, b float32, stopWordPreset string) *models.InvertedIndexConfig {
	ptr := func(s string) *string { return &s }
	return &models.InvertedIndexConfig{
		Bm25: &models.BM25Config{
			K1: k1,
			B:  b,
		},
		CleanupIntervalSeconds: 60,
		Stopwords: &models.StopwordConfig{
			Preset: stopWordPreset,
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
		InvertedIndexConfig: BM25FinvertedUserDictConfig(k1, b, "none"),
		Class:               "MyClass",

		Properties: []*models.Property{
			{
				Name:            "title",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationKagomeKr,
				IndexFilterable: &vFalse,
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

	migrator := NewMigrator(repo, logger)
	migrator.AddClass(context.Background(), class, schemaGetter.shardState)

	testData := []map[string]interface{}{}
	testData = append(testData, map[string]interface{}{"title": "Weaviate is a product by Semi Technologies"})
	testData = append(testData, map[string]interface{}{"title": "We Aviate is a product by SemiTechnologies"})
	testData = append(testData, map[string]interface{}{"title": "Aviate Technologies"})
	testData = append(testData, map[string]interface{}{"title": "W e Aviate Technologies will match as tokenizer splits"})
	testData = append(testData, map[string]interface{}{"title": "An unrelated title"})

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
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	repo, err := New(logger, Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, nil, nil, memwatch.NewDummyMonitor())
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(context.TODO()))
	defer repo.Shutdown(context.Background())

	props, migrator := SetupUserDictClass(t, repo, schemaGetter, logger, 0.5, 1)
	className := schema.ClassName("MyClass")
	idx := repo.GetIndex(className)
	require.NotNil(t, idx)

	for _, location := range []string{"memory", "disk"} {
		t.Run("bm25f Aviate "+location, func(t *testing.T) {
			// Check boosted
			kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title"}, Query: "Aviate"}
			addit := additional.Properties{}
			res, scores, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)
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
			EqualFloats(t, float32(0.24735281), scores[0], 5)
			EqualFloats(t, float32(0.18638557), scores[1], 5)
			EqualFloats(t, float32(0.17412336), scores[2], 5)
		})

		t.Run("bm25f Weaviate "+location, func(t *testing.T) {
			// Check boosted
			kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title"}, Query: "Weaviate"}
			addit := additional.Properties{}
			res, scores, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)
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
			EqualFloats(t, float32(1.0845481), scores[0], 5)
			EqualFloats(t, float32(0.24735281), scores[1], 5)
			EqualFloats(t, float32(0.17412336), scores[2], 5)
		})

		t.Run("bm25f We Aviate "+location, func(t *testing.T) {
			// Check boosted
			kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title"}, Query: "We Aviate"}
			addit := additional.Properties{}
			res, scores, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)
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
			EqualFloats(t, float32(1.0302471), scores[0], 5)
			EqualFloats(t, float32(0.93770987), scores[1], 5)
			EqualFloats(t, float32(0.40645638), scores[2], 5)
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
		migrator.UpdateInvertedIndexConfig(ctx, string(className), class.InvertedIndexConfig)
	})

	t.Run("Updated tokenizer", func(t *testing.T) {
		t.Run("bm25f Aviate", func(t *testing.T) {
			// Check boosted
			kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title"}, Query: "Aviate"}
			addit := additional.Properties{}
			res, scores, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)
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
			EqualFloats(t, float32(0.40645638), scores[0], 5)
			EqualFloats(t, float32(0.32623473), scores[1], 5)
			EqualFloats(t, float32(0.2969322), scores[2], 5)
		})

		t.Run("bm25f Weaviate", func(t *testing.T) {
			// Check boosted
			kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title"}, Query: "Weaviate"}
			addit := additional.Properties{}
			res, scores, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)
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
			EqualFloats(t, float32(0.40645638), scores[0], 5)
			EqualFloats(t, float32(0.32623473), scores[1], 5)
			EqualFloats(t, float32(0.2969322), scores[2], 5)
		})

		t.Run("bm25f We Aviate", func(t *testing.T) {
			// Check boosted
			kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title"}, Query: "We Aviate"}
			addit := additional.Properties{}
			res, scores, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)
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
			EqualFloats(t, float32(1.1823584), scores[0], 5)
			EqualFloats(t, float32(1.0761585), scores[1], 5)
			EqualFloats(t, float32(0.81291276), scores[2], 5)
		})
	})
}

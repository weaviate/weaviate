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

// SetupStopwordPresetClass creates a class with two text properties:
//   - "title":   stopwordPreset="none" (no stopwords filtered)
//   - "content": no stopwordPreset   (uses collection-level "en" stopwords)
//
// The collection-level invertedIndexConfig uses preset "en" so that by default
// English stopwords are filtered. The "title" property overrides this with "none".
func SetupStopwordPresetClass(t require.TestingT, repo *DB, schemaGetter *fakeSchemaGetter, logger logrus.FieldLogger) []string {
	vFalse := false
	vTrue := true

	iic := BM25FinvertedConfig(1.2, 0.75, "en")
	iic.StopwordPresets = map[string][]string{
		"custom": {"fox", "dog"},
	}

	class := &models.Class{
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: iic,
		Class:               "StopwordPresetClass",
		Properties: []*models.Property{
			{
				Name:            "title",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationWord,
				IndexFilterable: &vFalse,
				IndexSearchable: &vTrue,
				TextAnalyzer: &models.TextAnalyzerConfig{
					StopwordPreset: "none",
				},
			},
			{
				Name:            "content",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationWord,
				IndexFilterable: &vFalse,
				IndexSearchable: &vTrue,
				// No TextAnalyzer — uses collection-level "en" stopwords
			},
			{
				Name:            "notes",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationWord,
				IndexFilterable: &vFalse,
				IndexSearchable: &vTrue,
				TextAnalyzer: &models.TextAnalyzerConfig{
					StopwordPreset: "custom",
				},
			},
		},
	}

	props := make([]string, len(class.Properties))
	for i, prop := range class.Properties {
		props[i] = prop.Name
	}

	s := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{class},
		},
	}
	schemaGetter.schema = s

	migrator := NewMigrator(repo, logger, "node1")
	migrator.AddClass(context.Background(), class)

	// Test data: documents containing English stopwords ("the", "is", "a")
	// and words from the custom preset ("fox", "dog")
	testData := []map[string]interface{}{
		{"title": "the quick brown fox", "content": "the quick brown fox", "notes": "the quick brown fox"},    // doc 0
		{"title": "a lazy dog is here", "content": "a lazy dog is here", "notes": "a lazy dog is here"},       // doc 1
		{"title": "fox and the hound", "content": "fox and the hound", "notes": "fox and the hound"},          // doc 2
		{"title": "no stopwords present", "content": "no stopwords present", "notes": "no stopwords present"}, // doc 3
	}

	for i, data := range testData {
		id := strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", i)).String())
		obj := &models.Object{
			Class:              "StopwordPresetClass",
			ID:                 id,
			Properties:         data,
			CreationTimeUnix:   1565612833955,
			LastUpdateTimeUnix: 10000020,
		}
		vector := []float32{1, 3, 5, 0.4}
		err := repo.PutObject(context.Background(), obj, vector, nil, nil, nil, 0)
		require.Nil(t, err)
	}

	return props
}

func setupStopwordPresetRepo(t *testing.T, useBlockMaxWAND bool) (*DB, *fakeSchemaGetter, []string) {
	if useBlockMaxWAND {
		config.DefaultUsingBlockMaxWAND = true
	} else {
		config.DefaultUsingBlockMaxWAND = false
	}

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
	require.Nil(t, repo.WaitForStartup(context.TODO()))

	props := SetupStopwordPresetClass(t, repo, schemaGetter, logger)
	return repo, schemaGetter, props
}

func runStopwordPresetTests(t *testing.T, repo *DB, props []string) {
	idx := repo.GetIndex("StopwordPresetClass")
	require.NotNil(t, idx)

	addit := additional.Properties{}

	// --- title property: stopwordPreset="none" (stopwords are NOT filtered) ---

	t.Run("title no stopwords: 'the' matches as a regular term", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title"}, Query: "the"}
		res, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)
		require.Nil(t, err)
		require.Len(t, res, 2, "title has no stopwords, so 'the' matches docs 0 and 2")
	})

	t.Run("title no stopwords: 'is' matches as a regular term", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title"}, Query: "is"}
		res, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)
		require.Nil(t, err)
		require.Len(t, res, 1, "title has no stopwords, so 'is' matches doc 1")
		require.Equal(t, uint64(1), res[0].DocID)
	})

	// --- content property: uses collection-level "en" (English stopwords filtered) ---

	t.Run("content en stopwords: 'the' is filtered as stopword", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"content"}, Query: "the"}
		res, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)
		require.Nil(t, err)
		require.Len(t, res, 0, "content uses 'en' preset, 'the' is a stopword and should return no results")
	})

	t.Run("content en stopwords: 'is' is filtered as stopword", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"content"}, Query: "is"}
		res, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)
		require.Nil(t, err)
		require.Len(t, res, 0, "content uses 'en' preset, 'is' is a stopword and should return no results")
	})

	t.Run("content en stopwords: non-stopword 'fox' matches", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"content"}, Query: "fox"}
		res, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)
		require.Nil(t, err)
		require.Len(t, res, 2, "'fox' is not a stopword and should match docs 0 and 2")
	})

	// --- BM25F multi-property: title + content ---

	t.Run("bm25f 'the' in title+content: matches via title only", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title", "content"}, Query: "the"}
		res, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)
		require.Nil(t, err)
		require.Len(t, res, 2, "title has no stopwords (matches 'the'), content filters it")
	})

	t.Run("bm25f 'fox' in title+content: matches via both properties", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title", "content"}, Query: "fox"}
		res, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)
		require.Nil(t, err)
		require.Len(t, res, 2, "'fox' is not a stopword in either property, matches docs 0 and 2")
	})

	// --- notes property: user-defined "custom" preset (filters "fox" and "dog") ---

	t.Run("notes custom preset: 'fox' is a stopword", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"notes"}, Query: "fox"}
		res, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)
		require.Nil(t, err)
		require.Len(t, res, 0, "notes uses 'custom' preset where 'fox' is a stopword")
	})

	t.Run("notes custom preset: 'dog' is a stopword", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"notes"}, Query: "dog"}
		res, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)
		require.Nil(t, err)
		require.Len(t, res, 0, "notes uses 'custom' preset where 'dog' is a stopword")
	})

	t.Run("notes custom preset: 'the' is NOT a stopword", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"notes"}, Query: "the"}
		res, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)
		require.Nil(t, err)
		require.Len(t, res, 2, "notes uses 'custom' preset, 'the' is NOT in it so matches docs 0 and 2")
	})

	t.Run("notes custom preset: 'quick' is NOT a stopword", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"notes"}, Query: "quick"}
		res, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)
		require.Nil(t, err)
		require.Len(t, res, 1, "notes uses 'custom' preset, 'quick' is not a stopword")
		require.Equal(t, uint64(0), res[0].DocID)
	})
}

func TestBM25FStopwordPreset(t *testing.T) {
	repo, _, props := setupStopwordPresetRepo(t, false)
	defer repo.Shutdown(context.Background())
	runStopwordPresetTests(t, repo, props)
}

func TestBM25FStopwordPresetBlock(t *testing.T) {
	repo, _, props := setupStopwordPresetRepo(t, true)
	defer repo.Shutdown(context.Background())
	runStopwordPresetTests(t, repo, props)
}

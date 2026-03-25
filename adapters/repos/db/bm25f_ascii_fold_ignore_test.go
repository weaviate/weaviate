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

// SetupASCIIFoldIgnoreClass creates a class with two text properties:
//   - "title": asciiFold=true, asciiFoldIgnore=["é"] (preserves é)
//   - "body":  asciiFold=true, asciiFoldIgnore=[]   (folds everything)
func SetupASCIIFoldIgnoreClass(t require.TestingT, repo *DB, schemaGetter *fakeSchemaGetter, logger logrus.FieldLogger) []string {
	vFalse := false
	vTrue := true

	class := &models.Class{
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: BM25FinvertedConfig(1.2, 0.75, "none"),
		Class:               "ASCIIFoldIgnoreClass",
		Properties: []*models.Property{
			{
				Name:            "title",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationWord,
				IndexFilterable: &vFalse,
				IndexSearchable: &vTrue,
				TextAnalyser: &models.TextAnalyserConfig{
					ASCIIFold:       true,
					ASCIIFoldIgnore: []string{"é"},
				},
			},
			{
				Name:            "body",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationWord,
				IndexFilterable: &vFalse,
				IndexSearchable: &vTrue,
				TextAnalyser: &models.TextAnalyserConfig{
					ASCIIFold:       true,
					ASCIIFoldIgnore: []string{},
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

	testData := []map[string]interface{}{
		{"title": "L'école est fermée", "body": "L'école est fermée"},       // doc 0
		{"title": "cafe résumé", "body": "cafe résumé"},                     // doc 1
		{"title": "São Paulo café", "body": "São Paulo café"},               // doc 2
		{"title": "plain text no accents", "body": "plain text no accents"}, // doc 3
	}

	for i, data := range testData {
		id := strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", i)).String())
		obj := &models.Object{
			Class:              "ASCIIFoldIgnoreClass",
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

func setupASCIIFoldIgnoreRepo(t *testing.T, useBlockMaxWAND bool) (*DB, *fakeSchemaGetter, []string) {
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

	props := SetupASCIIFoldIgnoreClass(t, repo, schemaGetter, logger)
	return repo, schemaGetter, props
}

func runASCIIFoldIgnoreTests(t *testing.T, repo *DB, props []string) {
	idx := repo.GetIndex("ASCIIFoldIgnoreClass")
	require.NotNil(t, idx)

	addit := additional.Properties{}

	// --- Single property: body (full fold, no ignore) ---

	t.Run("body full fold: ecole matches école", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"body"}, Query: "ecole"}
		res, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)
		require.Nil(t, err)
		require.Len(t, res, 1, "body folds école→ecole, so 'ecole' should match")
		require.Equal(t, uint64(0), res[0].DocID)
	})

	t.Run("body full fold: cafe matches café and cafe", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"body"}, Query: "cafe"}
		res, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)
		require.Nil(t, err)
		require.Len(t, res, 2, "body folds café→cafe, so 'cafe' matches both docs with cafe/café")
	})

	t.Run("body full fold: resume matches résumé", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"body"}, Query: "resume"}
		res, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)
		require.Nil(t, err)
		require.Len(t, res, 1, "body folds résumé→resume")
		require.Equal(t, uint64(1), res[0].DocID)
	})

	// --- Single property: title (ignore é) ---

	t.Run("title ignore é: ecole does NOT match école", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title"}, Query: "ecole"}
		res, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)
		require.Nil(t, err)
		require.Len(t, res, 0, "title preserves é, so 'ecole' should not match 'école'")
	})

	t.Run("title ignore é: école matches école", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title"}, Query: "école"}
		res, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)
		require.Nil(t, err)
		require.Len(t, res, 1, "title preserves é, so 'école' matches")
		require.Equal(t, uint64(0), res[0].DocID)
	})

	t.Run("title ignore é: cafe matches cafe but not café", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title"}, Query: "cafe"}
		res, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)
		require.Nil(t, err)
		require.Len(t, res, 1, "title preserves é: 'cafe' matches literal 'cafe' in doc1, not 'café' in doc2")
		require.Equal(t, uint64(1), res[0].DocID)
	})

	t.Run("title ignore é: café matches café but not cafe", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title"}, Query: "café"}
		res, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)
		require.Nil(t, err)
		require.Len(t, res, 1, "title preserves é: 'café' matches literal 'café' in doc2")
		require.Equal(t, uint64(2), res[0].DocID)
	})

	t.Run("title ignore é: resume does NOT match résumé", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title"}, Query: "resume"}
		res, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)
		require.Nil(t, err)
		require.Len(t, res, 0, "title preserves é: 'resume' does not match 'résumé'")
	})

	t.Run("title ignore é: résumé matches résumé", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title"}, Query: "résumé"}
		res, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)
		require.Nil(t, err)
		require.Len(t, res, 1, "title preserves é: 'résumé' matches")
		require.Equal(t, uint64(1), res[0].DocID)
	})

	t.Run("title ignore é: sao matches São (ã is folded)", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title"}, Query: "sao"}
		res, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)
		require.Nil(t, err)
		require.Len(t, res, 1, "title folds ã→a (not in ignore list), so 'sao' matches 'São'")
		require.Equal(t, uint64(2), res[0].DocID)
	})

	// --- BM25F multi-property: title + body ---

	t.Run("bm25f ecole in title+body: matches via body only", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title", "body"}, Query: "ecole"}
		res, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)
		require.Nil(t, err)
		require.Len(t, res, 1, "body folds école→ecole (match), title preserves é (no match)")
		require.Equal(t, uint64(0), res[0].DocID)
	})

	t.Run("bm25f école in title+body: matches via both properties", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title", "body"}, Query: "école"}
		res, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)
		require.Nil(t, err)
		require.Len(t, res, 1, "body folds école→ecole (match), title keeps école (match)")
		require.Equal(t, uint64(0), res[0].DocID)
	})

	t.Run("bm25f cafe in title+body: matches 2 docs", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title", "body"}, Query: "cafe"}
		res, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)
		require.Nil(t, err)
		require.Len(t, res, 2, "body: cafe matches cafe+café (2 docs), title: cafe matches cafe (1 doc)")
	})

	t.Run("bm25f café in title+body: matches 2 docs", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title", "body"}, Query: "café"}
		res, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)
		require.Nil(t, err)
		require.Len(t, res, 2, "body: café→cafe matches 2 docs, title: café matches café (1 doc)")
	})

	t.Run("bm25f resume in title+body: matches via body only", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title", "body"}, Query: "resume"}
		res, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)
		require.Nil(t, err)
		require.Len(t, res, 1, "body folds résumé→resume (match), title preserves é (no match)")
		require.Equal(t, uint64(1), res[0].DocID)
	})

	t.Run("bm25f résumé in title+body: matches via both", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title", "body"}, Query: "résumé"}
		res, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)
		require.Nil(t, err)
		require.Len(t, res, 1, "body folds résumé→resume (match), title keeps résumé (match)")
		require.Equal(t, uint64(1), res[0].DocID)
	})
}

func TestBM25FASCIIFoldIgnore(t *testing.T) {
	repo, _, props := setupASCIIFoldIgnoreRepo(t, false)
	defer repo.Shutdown(context.Background())
	runASCIIFoldIgnoreTests(t, repo, props)
}

func TestBM25FASCIIFoldIgnoreBlock(t *testing.T) {
	repo, _, props := setupASCIIFoldIgnoreRepo(t, true)
	defer repo.Shutdown(context.Background())
	runASCIIFoldIgnoreTests(t, repo, props)
}

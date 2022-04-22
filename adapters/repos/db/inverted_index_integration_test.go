//go:build integrationTest
// +build integrationTest

package db

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAddClass_IndexByTimestamps(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()

	shardState := singleShardState()
	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{shardState: shardState}
	repo := New(logger, Config{
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		DiskUseWarningPercentage:  config.DefaultDiskUseWarningPercentage,
		DiskUseReadOnlyPercentage: config.DefaultDiskUseReadonlyPercentage,
		IndexByTimestamps:         true,
	}, &fakeRemoteClient{},
		&fakeNodeResolver{})
	repo.SetSchemaGetter(schemaGetter)
	err := repo.WaitForStartup(testCtx())
	require.Nil(t, err)
	migrator := NewMigrator(repo, logger)

	t.Run("add class", func(t *testing.T) {
		class := &models.Class{
			Class:               "TestClass",
			VectorIndexConfig:   hnsw.NewDefaultUserConfig(),
			InvertedIndexConfig: invertedConfig(),
			Properties: []*models.Property{
				{
					Name:         "name",
					DataType:     []string{"string"},
					Tokenization: "word",
				},
			},
		}

		err := migrator.AddClass(context.Background(), class, schemaGetter.shardState)
		require.Nil(t, err)
	})

	t.Run("check for timestamp buckets", func(t *testing.T) {
		for _, idx := range migrator.db.indices {
			for _, shd := range idx.Shards {
				createBucket := shd.store.Bucket("property__creationTimeUnix")
				assert.NotNil(t, createBucket, "property__creationTimeUnix bucket not found")

				createHashBucket := shd.store.Bucket("hash_property__creationTimeUnix")
				assert.NotNil(t, createHashBucket, "hash_property__creationTimeUnix bucket not found")

				updateBucket := shd.store.Bucket("property__lastUpdateTimeUnix")
				assert.NotNil(t, updateBucket)

				updateHashBucket := shd.store.Bucket("hash_property__lastUpdateTimeUnix")
				assert.NotNil(t, updateHashBucket, "hash_property__creationTimeUnix bucket not found")
			}
		}
	})
}

func TestGetClass_IndexByTimestamps(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()

	class := &models.Class{
		Class:               "TestClass",
		VectorIndexConfig:   hnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		Properties: []*models.Property{
			{
				Name:         "name",
				DataType:     []string{"string"},
				Tokenization: "word",
			},
		},
	}

	shardState := singleShardState()
	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{shardState: shardState, schema: schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{class},
		},
	}}
	repo := New(logger, Config{
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		DiskUseWarningPercentage:  config.DefaultDiskUseWarningPercentage,
		DiskUseReadOnlyPercentage: config.DefaultDiskUseReadonlyPercentage,
		IndexByTimestamps:         true,
	}, &fakeRemoteClient{},
		&fakeNodeResolver{})
	repo.SetSchemaGetter(schemaGetter)
	err := repo.WaitForStartup(testCtx())
	require.Nil(t, err)
	migrator := NewMigrator(repo, logger)

	t.Run("add class", func(t *testing.T) {

		err := migrator.AddClass(context.Background(), class, schemaGetter.shardState)
		require.Nil(t, err)
	})

	now := time.Now().UnixNano() / int64(time.Millisecond)
	testID := strfmt.UUID("a0b55b05-bc5b-4cc9-b646-1452d1390a62")

	t.Run("insert test object", func(t *testing.T) {
		obj := &models.Object{
			ID:                 testID,
			Class:              "TestClass",
			CreationTimeUnix:   now,
			LastUpdateTimeUnix: now,
			Properties:         map[string]interface{}{"name": "objectarooni"},
		}
		vec := []float32{1, 2, 3}
		err := repo.PutObject(context.Background(), obj, vec)
		require.Nil(t, err)
	})

	t.Run("get testObject with timestamp filters", func(t *testing.T) {
		createTimeFilter := &filters.LocalFilter{
			Root: &filters.Clause{
				Operator: filters.OperatorEqual,
				On: &filters.Path{
					Class:    "TestClass",
					Property: "_creationTimeUnix",
				},
				Value: &filters.Value{
					Value: fmt.Sprint(now),
					Type:  dtString,
				},
			},
		}

		updateTimeFilter := &filters.LocalFilter{
			Root: &filters.Clause{
				Operator: filters.OperatorEqual,
				On: &filters.Path{
					Class:    "TestClass",
					Property: "_lastUpdateTimeUnix",
				},
				Value: &filters.Value{
					Value: fmt.Sprint(now),
					Type:  dtString,
				},
			},
		}

		res1, err := repo.ClassSearch(context.Background(), traverser.GetParams{
			ClassName:  "TestClass",
			Pagination: &filters.Pagination{Limit: 10},
			Filters:    createTimeFilter,
		})
		require.Nil(t, err)
		assert.Len(t, res1, 1)
		assert.Equal(t, testID, res1[0].ID)

		res2, err := repo.ClassSearch(context.Background(), traverser.GetParams{
			ClassName:  "TestClass",
			Pagination: &filters.Pagination{Limit: 10},
			Filters:    updateTimeFilter,
		})
		require.Nil(t, err)
		assert.Len(t, res2, 1)
		assert.Equal(t, testID, res2[0].ID)
	})
}

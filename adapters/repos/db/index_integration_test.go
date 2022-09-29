//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

//go:build integrationTest
// +build integrationTest

package db

import (
	"os"
	"strings"

	"github.com/semi-technologies/weaviate/entities/models"
)

func getIndexFilenames(dirName string, className string) ([]string, error) {
	filenames := []string{}
	infos, err := os.ReadDir(dirName)
	if err != nil {
		return filenames, err
	}
	for _, i := range infos {
		if strings.Contains(i.Name(), className) {
			filenames = append(filenames, i.Name())
		}
	}
	return filenames, nil
}

// TODO: gh-1599 reenable

// func TestIndex_DropIndex(t *testing.T) {
// 	rand.Seed(time.Now().UnixNano())
// 	dirName := t.TmpDir()
// 	testClassName := "deletetest"
// 	logger, _ := test.NewNullLogger()
// 	shardState := singleShardState()
// 	index, err := NewIndex(testCtx(), IndexConfig{
// 		RootPath: dirName, ClassName: schema.ClassName(testClassName),
// 	}, shardState, invertedConfig(), hnsw.NewDefaultUserConfig(), &fakeSchemaGetter{
// 		shardState: shardState,
// 	}, nil, logger)
// 	require.Nil(t, err)

// 	indexFilesBeforeDelete, err := getIndexFilenames(dirName, testClassName)
// 	require.Nil(t, err)

// 	// drop the index
// 	err = index.drop()
// 	require.Nil(t, err)

// 	indexFilesAfterDelete, err := getIndexFilenames(dirName, testClassName)
// 	require.Nil(t, err)

// 	assert.Equal(t, 3, len(indexFilesBeforeDelete))
// 	assert.Equal(t, 0, len(indexFilesAfterDelete))
// }

// func TestIndex_DropEmptyAndRecreateEmptyIndex(t *testing.T) {
// 	rand.Seed(time.Now().UnixNano())
// 	dirName := t.TmpDir()
// 	testClassName := "deletetest"
// 	logger, _ := test.NewNullLogger()
// 	shardState := singleShardState()
// 	index, err := NewIndex(testCtx(), IndexConfig{
// 		RootPath: dirName, ClassName: schema.ClassName(testClassName),
// 	}, shardState, invertedConfig(), hnsw.NewDefaultUserConfig(), &fakeSchemaGetter{
// 		shardState: shardState,
// 	}, nil, logger)
// 	require.Nil(t, err)

// 	indexFilesBeforeDelete, err := getIndexFilenames(dirName, testClassName)
// 	require.Nil(t, err)

// 	// drop the index
// 	err = index.drop()
// 	require.Nil(t, err)

// 	indexFilesAfterDelete, err := getIndexFilenames(dirName, testClassName)
// 	require.Nil(t, err)

// 	index, err = NewIndex(testCtx(), IndexConfig{
// 		RootPath: dirName, ClassName: schema.ClassName(testClassName),
// 	}, singleShardState(), invertedConfig(), hnsw.NewDefaultUserConfig(), &fakeSchemaGetter{}, nil, logger)
// 	require.Nil(t, err)

// 	indexFilesAfterRecreate, err := getIndexFilenames(dirName, testClassName)
// 	require.Nil(t, err)

// 	assert.Equal(t, 3, len(indexFilesBeforeDelete))
// 	assert.Equal(t, 0, len(indexFilesAfterDelete))
// 	assert.Equal(t, 3, len(indexFilesAfterRecreate))
// 	assert.Equal(t, indexFilesBeforeDelete, indexFilesAfterRecreate)
// }

// func TestIndex_DropWithDataAndRecreateWithDataIndex(t *testing.T) {
// 	rand.Seed(time.Now().UnixNano())
// 	dirName := t.TmpDir()
// 	logger, _ := test.NewNullLogger()
// 	testClassName := "deletetest"
// 	testClass := &models.Class{
// 		Class: testClassName,
// 		Properties: []*models.Property{
// 			&models.Property{
// 				Name:     "name",
// 				DataType: []string{"string"},
// 			},
// 		},
// 	}
// 	fakeSchema := schema.Schema{
// 		Objects: &models.Schema{
// 			Classes: []*models.Class{
// 				testClass,
// 			},
// 		},
// 	}
// 	// create index with data
// 	shardState := singleShardState()
// 	index, err := NewIndex(testCtx(), IndexConfig{
// 		RootPath:  dirName,
// 		ClassName: schema.ClassName(testClassName),
// 	}, shardState, invertedConfig(), hnsw.NewDefaultUserConfig(), &fakeSchemaGetter{
// 		schema: fakeSchema, shardState: shardState,
// 	}, nil, logger)
// 	require.Nil(t, err)

// 	productsIds := []strfmt.UUID{
// 		"1295c052-263d-4aae-99dd-920c5a370d06",
// 		"1295c052-263d-4aae-99dd-920c5a370d07",
// 	}

// 	products := []map[string]interface{}{
// 		{"name": "one"},
// 		{"name": "two"},
// 	}

// 	index.addUUIDProperty(context.TODO())

// 	index.addProperty(context.TODO(), &models.Property{
// 		Name:     "name",
// 		DataType: []string{"string"},
// 	})

// 	for i, p := range products {
// 		thing := models.Object{
// 			Class:      testClass.Class,
// 			ID:         productsIds[i],
// 			Properties: p,
// 		}

// 		err := index.putObject(context.TODO(), storobj.FromObject(&thing, []float32{0.1, 0.2, 0.01, 0.2}))
// 		require.Nil(t, err)
// 	}

// 	indexFilesBeforeDelete, err := getIndexFilenames(dirName, testClassName)
// 	require.Nil(t, err)

// 	beforeDeleteObj1, err := index.objectByID(context.TODO(), productsIds[0], nil, additional.Properties{})
// 	require.Nil(t, err)

// 	beforeDeleteObj2, err := index.objectByID(context.TODO(), productsIds[1], nil, additional.Properties{})
// 	require.Nil(t, err)

// 	// drop the index
// 	err = index.drop()
// 	require.Nil(t, err)

// 	indexFilesAfterDelete, err := getIndexFilenames(dirName, testClassName)
// 	require.Nil(t, err)

// 	// recreate the index
// 	index, err = NewIndex(testCtx(), IndexConfig{
// 		RootPath:  dirName,
// 		ClassName: schema.ClassName(testClassName),
// 	}, shardState, invertedConfig(), hnsw.NewDefaultUserConfig(), &fakeSchemaGetter{
// 		schema:     fakeSchema,
// 		shardState: shardState,
// 	}, nil, logger)
// 	require.Nil(t, err)

// 	index.addUUIDProperty(context.TODO())
// 	index.addProperty(context.TODO(), &models.Property{
// 		Name:     "name",
// 		DataType: []string{"string"},
// 	})

// 	indexFilesAfterRecreate, err := getIndexFilenames(dirName, testClassName)
// 	require.Nil(t, err)

// 	afterRecreateObj1, err := index.objectByID(context.TODO(), productsIds[0], nil, additional.Properties{})
// 	require.Nil(t, err)

// 	afterRecreateObj2, err := index.objectByID(context.TODO(), productsIds[1], nil, additional.Properties{})
// 	require.Nil(t, err)

// 	// insert some data in the recreated index
// 	for i, p := range products {
// 		thing := models.Object{
// 			Class:      testClass.Class,
// 			ID:         productsIds[i],
// 			Properties: p,
// 		}

// 		err := index.putObject(context.TODO(), storobj.FromObject(&thing, []float32{0.1, 0.2, 0.01, 0.2}))
// 		require.Nil(t, err)
// 	}

// 	afterRecreateAndInsertObj1, err := index.objectByID(context.TODO(), productsIds[0], nil, additional.Properties{})
// 	require.Nil(t, err)

// 	afterRecreateAndInsertObj2, err := index.objectByID(context.TODO(), productsIds[1], nil, additional.Properties{})
// 	require.Nil(t, err)

// 	assert.Equal(t, 3, len(indexFilesBeforeDelete))
// 	assert.Equal(t, 0, len(indexFilesAfterDelete))
// 	assert.Equal(t, 3, len(indexFilesAfterRecreate))
// 	assert.Equal(t, indexFilesBeforeDelete, indexFilesAfterRecreate)
// 	assert.NotNil(t, beforeDeleteObj1)
// 	assert.NotNil(t, beforeDeleteObj2)
// 	assert.Empty(t, afterRecreateObj1)
// 	assert.Empty(t, afterRecreateObj2)
// 	assert.NotNil(t, afterRecreateAndInsertObj1)
// 	assert.NotNil(t, afterRecreateAndInsertObj2)
// }

func invertedConfig() *models.InvertedIndexConfig {
	return &models.InvertedIndexConfig{
		CleanupIntervalSeconds: 60,
		Stopwords: &models.StopwordConfig{
			Preset: "none",
		},
		IndexNullState:      true,
		IndexPropertyLength: true,
	}
}

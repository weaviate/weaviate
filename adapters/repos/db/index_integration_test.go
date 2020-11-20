//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

// +build integrationTest

package db

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/adapters/repos/db/storobj"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getIndexFilenames(dirName string, className string) ([]string, error) {
	filenames := []string{}
	infos, err := ioutil.ReadDir(dirName)
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

func TestIndex_DropIndex(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()
	testClassName := "deletetest"
	index, err := NewIndex(IndexConfig{
		RootPath: dirName, Kind: kind.Thing, ClassName: schema.ClassName(testClassName),
	}, &fakeSchemaGetter{}, nil, nil)
	require.Nil(t, err)

	indexFilesBeforeDelete, err := getIndexFilenames(dirName, testClassName)
	require.Nil(t, err)

	// drop the index
	err = index.drop()
	require.Nil(t, err)

	indexFilesAfterDelete, err := getIndexFilenames(dirName, testClassName)
	require.Nil(t, err)

	assert.Equal(t, 3, len(indexFilesBeforeDelete))
	assert.Equal(t, 0, len(indexFilesAfterDelete))
}

func TestIndex_DropEmptyAndRecreateEmptyIndex(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()
	testClassName := "deletetest"
	index, err := NewIndex(IndexConfig{
		RootPath: dirName, Kind: kind.Thing, ClassName: schema.ClassName(testClassName),
	}, &fakeSchemaGetter{}, nil, nil)
	require.Nil(t, err)

	indexFilesBeforeDelete, err := getIndexFilenames(dirName, testClassName)
	require.Nil(t, err)

	// drop the index
	err = index.drop()
	require.Nil(t, err)

	indexFilesAfterDelete, err := getIndexFilenames(dirName, testClassName)
	require.Nil(t, err)

	index, err = NewIndex(IndexConfig{
		RootPath: dirName, Kind: kind.Thing, ClassName: schema.ClassName(testClassName),
	}, &fakeSchemaGetter{}, nil, nil)
	require.Nil(t, err)

	indexFilesAfterRecreate, err := getIndexFilenames(dirName, testClassName)
	require.Nil(t, err)

	assert.Equal(t, 3, len(indexFilesBeforeDelete))
	assert.Equal(t, 0, len(indexFilesAfterDelete))
	assert.Equal(t, 3, len(indexFilesAfterRecreate))
	assert.Equal(t, indexFilesBeforeDelete, indexFilesAfterRecreate)
}

func TestIndex_DropWithDataAndRecreateWithDataIndex(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()
	logger := logrus.New()
	testClassName := "deletetest"
	testClass := &models.Class{
		Class: testClassName,
		Properties: []*models.Property{
			&models.Property{
				Name:     "name",
				DataType: []string{"string"},
			},
		},
	}
	fakeSchema := schema.Schema{
		Things: &models.Schema{
			Classes: []*models.Class{
				testClass,
			},
		},
	}
	// create index with data
	index, err := NewIndex(IndexConfig{
		RootPath:  dirName,
		Kind:      kind.Thing,
		ClassName: schema.ClassName(testClassName),
	}, &fakeSchemaGetter{schema: fakeSchema}, nil, logger)
	require.Nil(t, err)

	var productsIds = []strfmt.UUID{
		"1295c052-263d-4aae-99dd-920c5a370d06",
		"1295c052-263d-4aae-99dd-920c5a370d07",
	}

	products := []map[string]interface{}{
		{"name": "one"},
		{"name": "two"},
	}

	index.addProperty(context.TODO(), &models.Property{
		Name:     "uuid",
		DataType: []string{"string"},
	})

	index.addProperty(context.TODO(), &models.Property{
		Name:     "name",
		DataType: []string{"string"},
	})

	for i, p := range products {
		thing := models.Thing{
			Class:  testClass.Class,
			ID:     productsIds[i],
			Schema: p,
		}

		err := index.putObject(context.TODO(), storobj.FromThing(&thing, []float32{0.1, 0.2, 0.01, 0.2}))
		require.Nil(t, err)
	}

	indexFilesBeforeDelete, err := getIndexFilenames(dirName, testClassName)
	require.Nil(t, err)

	beforeDeleteObj1, err := index.objectByID(context.TODO(), productsIds[0], nil, traverser.UnderscoreProperties{})
	require.Nil(t, err)

	beforeDeleteObj2, err := index.objectByID(context.TODO(), productsIds[1], nil, traverser.UnderscoreProperties{})
	require.Nil(t, err)

	// drop the index
	err = index.drop()
	require.Nil(t, err)

	indexFilesAfterDelete, err := getIndexFilenames(dirName, testClassName)
	require.Nil(t, err)

	// recreate the index
	index, err = NewIndex(IndexConfig{
		RootPath:  dirName,
		Kind:      kind.Thing,
		ClassName: schema.ClassName(testClassName),
	}, &fakeSchemaGetter{schema: fakeSchema}, nil, logger)
	require.Nil(t, err)

	index.addProperty(context.TODO(), &models.Property{
		Name:     "uuid",
		DataType: []string{"string"},
	})

	index.addProperty(context.TODO(), &models.Property{
		Name:     "name",
		DataType: []string{"string"},
	})

	indexFilesAfterRecreate, err := getIndexFilenames(dirName, testClassName)
	require.Nil(t, err)

	afterRecreateObj1, err := index.objectByID(context.TODO(), productsIds[0], nil, traverser.UnderscoreProperties{})
	require.Nil(t, err)

	afterRecreateObj2, err := index.objectByID(context.TODO(), productsIds[1], nil, traverser.UnderscoreProperties{})
	require.Nil(t, err)

	// insert some data in the recreated index
	for i, p := range products {
		thing := models.Thing{
			Class:  testClass.Class,
			ID:     productsIds[i],
			Schema: p,
		}

		err := index.putObject(context.TODO(), storobj.FromThing(&thing, []float32{0.1, 0.2, 0.01, 0.2}))
		require.Nil(t, err)
	}

	afterRecreateAndInsertObj1, err := index.objectByID(context.TODO(), productsIds[0], nil, traverser.UnderscoreProperties{})
	require.Nil(t, err)

	afterRecreateAndInsertObj2, err := index.objectByID(context.TODO(), productsIds[1], nil, traverser.UnderscoreProperties{})
	require.Nil(t, err)

	assert.Equal(t, 3, len(indexFilesBeforeDelete))
	assert.Equal(t, 0, len(indexFilesAfterDelete))
	assert.Equal(t, 3, len(indexFilesAfterRecreate))
	assert.Equal(t, indexFilesBeforeDelete, indexFilesAfterRecreate)
	assert.NotNil(t, beforeDeleteObj1)
	assert.NotNil(t, beforeDeleteObj2)
	assert.Empty(t, afterRecreateObj1)
	assert.Empty(t, afterRecreateObj2)
	assert.NotNil(t, afterRecreateAndInsertObj1)
	assert.NotNil(t, afterRecreateAndInsertObj2)
}

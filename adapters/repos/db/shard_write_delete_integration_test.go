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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/boltdb/bolt"
	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/adapters/repos/db/storobj"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPerformCleanupIndexWithFrequencyProp(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()
	logger := logrus.New()
	testClassName := "deletetest"
	testPropName := "name"
	testClass := &models.Class{
		Class: testClassName,
		Properties: []*models.Property{
			&models.Property{
				Name:     testPropName,
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
	shard, err := NewShard("extend_invert_benchmark", index)
	require.Nil(t, err)

	var productsIds = []strfmt.UUID{
		"1295c052-263d-4aae-99dd-920c5a370d06",
		"1295c052-263d-4aae-99dd-920c5a370d07",
	}

	products := []map[string]interface{}{
		{"name": "one"},
		{"name": "two one"},
	}

	err = shard.addProperty(context.TODO(), &models.Property{
		Name:     "uuid",
		DataType: []string{"string"},
	})
	require.Nil(t, err)

	err = shard.addProperty(context.TODO(), &models.Property{
		Name:     testPropName,
		DataType: []string{"string"},
	})
	require.Nil(t, err)

	for i, p := range products {
		thing := models.Thing{
			Class:  testClass.Class,
			ID:     productsIds[i],
			Schema: p,
		}

		err := shard.putObject(context.TODO(), storobj.FromThing(&thing, []float32{0.1, 0.2, 0.01, 0.2}))
		require.Nil(t, err)
	}

	productToDeleteID := productsIds[1]
	existsBeforeDelete, err := shard.exists(context.TODO(), strfmt.UUID(productToDeleteID))
	require.Nil(t, err)

	idBytes1, err := uuid.MustParse(strfmt.UUID(productsIds[0]).String()).MarshalBinary()
	require.Nil(t, err)

	idBytes2, err := uuid.MustParse(strfmt.UUID(productsIds[1]).String()).MarshalBinary()
	require.Nil(t, err)

	var beforeOne []byte
	var beforeTwo []byte
	var beforeObjID1 []byte
	var beforeObjID2 []byte
	err = shard.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(helpers.BucketFromPropName(testPropName))
		beforeOne = bucket.Get([]byte("one"))
		beforeTwo = bucket.Get([]byte("two"))
		bucket = tx.Bucket(helpers.ObjectsBucket)
		beforeObjID1 = bucket.Get([]byte(idBytes1))
		bucket = tx.Bucket(helpers.ObjectsBucket)
		beforeObjID2 = bucket.Get([]byte(idBytes2))
		return nil
	})
	require.Nil(t, err)

	err = shard.deleteObject(context.TODO(), strfmt.UUID(productToDeleteID))
	require.Nil(t, err)

	existsAfterDelete, err := shard.exists(context.TODO(), strfmt.UUID(productToDeleteID))
	require.Nil(t, err)

	beforeDeletedIDs := shard.deletedDocIDs.GetAll()

	err = shard.periodicCleanup(10, 1*time.Millisecond)
	require.Nil(t, err)

	afterDeletedIDs := shard.deletedDocIDs.GetAll()

	var afterOne []byte
	var afterTwo []byte
	var afterObjID1 []byte
	var afterObjID2 []byte
	err = shard.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(helpers.BucketFromPropName(testPropName))
		afterOne = bucket.Get([]byte("one"))
		afterTwo = bucket.Get([]byte("two"))
		bucket = tx.Bucket(helpers.ObjectsBucket)
		afterObjID1 = bucket.Get([]byte(idBytes1))
		bucket = tx.Bucket(helpers.ObjectsBucket)
		afterObjID2 = bucket.Get([]byte(idBytes2))
		return nil
	})
	require.Nil(t, err)

	var updatedDocOneCount uint32
	r := bytes.NewReader(afterOne[4:])
	err = binary.Read(r, binary.LittleEndian, &updatedDocOneCount)
	require.Nil(t, err)

	var updatedDocTwoCount uint32
	r = bytes.NewReader(afterTwo[4:])
	err = binary.Read(r, binary.LittleEndian, &updatedDocTwoCount)
	require.Nil(t, err)

	assert.Equal(t, true, existsBeforeDelete)
	assert.Equal(t, false, existsAfterDelete)
	assert.Equal(t, 1, len(beforeDeletedIDs))
	assert.Equal(t, 0, len(afterDeletedIDs))
	assert.Equal(t, len(beforeObjID1), len(afterObjID1))
	assert.NotEqual(t, len(beforeObjID2), len(afterObjID2))
	assert.Equal(t, 0, len(afterObjID2))
	assert.Equal(t, 2, len(beforeOne[8:])/8)
	assert.Equal(t, 1, len(afterOne[8:])/8)
	assert.Equal(t, 1, len(beforeTwo[8:])/8)
	assert.Equal(t, 0, len(afterTwo[8:]))
	assert.Equal(t, uint32(1), updatedDocOneCount)
	assert.Equal(t, uint32(0), updatedDocTwoCount)
}

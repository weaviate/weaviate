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
	"context"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"

	"github.com/google/uuid"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	enthnsw "github.com/semi-technologies/weaviate/entities/vectorindex/hnsw"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/semi-technologies/weaviate/usecases/objects"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

func TestNullArrayClass(t *testing.T) {
	arrayClass := createClassWithEverything()

	names := []string{"elements", "batches"}
	for _, name := range names {
		t.Run("add nil object via "+name, func(t *testing.T) {
			migrator, repo, schemaGetter := createRepo(t)
			defer repo.Shutdown(context.Background())
			err := migrator.AddClass(context.Background(), arrayClass, schemaGetter.shardState)
			require.Nil(t, err)
			// update schema getter so it's in sync with class
			schemaGetter.schema = schema.Schema{
				Objects: &models.Schema{
					Classes: []*models.Class{arrayClass},
				},
			}

			ObjectUuid1 := uuid.New()
			arrayObjNil := &models.Object{
				ID:    strfmt.UUID(ObjectUuid1.String()),
				Class: "EverythingClass",
				Properties: map[string]interface{}{
					"strings":        nil,
					"ints":           nil,
					"datesAsStrings": nil,
					"numbers":        nil,
					"booleans":       nil,
					"texts":          nil,
					"number":         nil,
					"boolean":        nil,
					"int":            nil,
					"string":         nil,
					"text":           nil,
					"phoneNumber":    nil,
					"phoneNumbers":   nil,
				},
			}

			ObjectUuid2 := uuid.New()
			arrayObjEmpty := &models.Object{
				ID:         strfmt.UUID(ObjectUuid2.String()),
				Class:      "EverythingClass",
				Properties: map[string]interface{}{},
			}

			if name == names[0] {
				assert.Nil(t, repo.PutObject(context.Background(), arrayObjNil, []float32{1}))
				assert.Nil(t, repo.PutObject(context.Background(), arrayObjEmpty, []float32{1}))

			} else {
				batch := make([]objects.BatchObject, 2)
				batch[0] = objects.BatchObject{Object: arrayObjNil, UUID: arrayObjNil.ID}
				batch[1] = objects.BatchObject{Object: arrayObjEmpty, UUID: arrayObjEmpty.ID}
				_, err := repo.BatchPutObjects(context.Background(), batch)
				assert.Nil(t, err)
			}

			item1, err := repo.ObjectByID(context.Background(), arrayObjNil.ID, nil, additional.Properties{})
			assert.Nil(t, err)
			item2, err := repo.ObjectByID(context.Background(), arrayObjEmpty.ID, nil, additional.Properties{})
			assert.Nil(t, err)

			item1Schema := item1.Schema.(map[string]interface{})
			item2Schema := item2.Schema.(map[string]interface{})
			delete(item1Schema, "id")
			delete(item2Schema, "id")
			assert.Equal(t, item1Schema, item2Schema)
		})
	}
}

func createRepo(t *testing.T) (*Migrator, *DB, *fakeSchemaGetter) {
	schemaGetter := &fakeSchemaGetter{shardState: singleShardState()}
	logger, _ := test.NewNullLogger()
	dirName := t.TempDir()
	repo := New(logger, Config{
		FlushIdleAfter:            60,
		RootPath:                  dirName,
		QueryMaximumResults:       10,
		DiskUseWarningPercentage:  config.DefaultDiskUseWarningPercentage,
		DiskUseReadOnlyPercentage: config.DefaultDiskUseReadonlyPercentage,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, nil)
	repo.SetSchemaGetter(schemaGetter)
	err := repo.WaitForStartup(testCtx())
	require.Nil(t, err)
	return NewMigrator(repo, logger), repo, schemaGetter
}

func createClassWithEverything() *models.Class {
	return &models.Class{
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		Class:               "EverythingClass",
		Properties: []*models.Property{
			{
				Name:         "strings",
				DataType:     []string{"string[]"},
				Tokenization: models.PropertyTokenizationWord,
			},
			{
				Name:         "texts",
				DataType:     []string{"text[]"},
				Tokenization: models.PropertyTokenizationWord,
			},
			{
				Name:     "numbers",
				DataType: []string{"number[]"},
			},
			{
				Name:     "ints",
				DataType: []string{"int[]"},
			},
			{
				Name:     "booleans",
				DataType: []string{"boolean[]"},
			},
			{
				Name:     "datesAsStrings",
				DataType: []string{"date[]"},
			},
			{
				Name:     "number",
				DataType: []string{"number"},
			},
			{
				Name:     "bool",
				DataType: []string{"boolean"},
			},
			{
				Name:     "int",
				DataType: []string{"int"},
			},
			{
				Name:     "string",
				DataType: []string{"string"},
			},
			{
				Name:     "text",
				DataType: []string{"text"},
			},
			{
				Name:     "phoneNumber",
				DataType: []string{"phoneNumber"},
			},
			{
				Name:     "phoneNumbers",
				DataType: []string{"phoneNumber[]"},
			},
		},
	}
}

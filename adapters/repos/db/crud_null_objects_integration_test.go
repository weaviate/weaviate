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
	"testing"

	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/objects"
)

// Cannot filter for null state without enabling in the InvertedIndexConfig
func TestFilterNullStateError(t *testing.T) {
	class := createClassWithEverything(false, false)
	migrator, repo, schemaGetter := createRepo(t)
	defer repo.Shutdown(context.Background())
	err := migrator.AddClass(context.Background(), class, schemaGetter.shardState)
	require.Nil(t, err)
	// update schema getter so it's in sync with class
	schemaGetter.schema = schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{class},
		},
	}

	nilFilter := &filters.LocalFilter{
		Root: &filters.Clause{
			Operator: filters.OperatorIsNull,
			On: &filters.Path{
				Class:    schema.ClassName(carClass.Class),
				Property: schema.PropertyName(class.Properties[0].Name),
			},
			Value: &filters.Value{
				Value: true,
				Type:  schema.DataTypeBoolean,
			},
		},
	}

	params := dto.GetParams{
		SearchVector: []float32{0.1, 0.1, 0.1, 1.1, 0.1},
		ClassName:    class.Class,
		Pagination:   &filters.Pagination{Limit: 5},
		Filters:      nilFilter,
	}
	_, err = repo.Search(context.Background(), params)
	require.NotNil(t, err)
}

func TestNullArrayClass(t *testing.T) {
	arrayClass := createClassWithEverything(true, false)

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
				assert.Nil(t, repo.PutObject(context.Background(), arrayObjNil, []float32{1}, nil))
				assert.Nil(t, repo.PutObject(context.Background(), arrayObjEmpty, []float32{1}, nil))

			} else {
				batch := make([]objects.BatchObject, 2)
				batch[0] = objects.BatchObject{Object: arrayObjNil, UUID: arrayObjNil.ID}
				batch[1] = objects.BatchObject{Object: arrayObjEmpty, UUID: arrayObjEmpty.ID}
				_, err := repo.BatchPutObjects(context.Background(), batch, nil)
				assert.Nil(t, err)
			}

			item1, err := repo.ObjectByID(context.Background(), arrayObjNil.ID, nil, additional.Properties{}, "")
			assert.Nil(t, err)
			item2, err := repo.ObjectByID(context.Background(), arrayObjEmpty.ID, nil, additional.Properties{}, "")
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
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	logger, _ := test.NewNullLogger()
	dirName := t.TempDir()
	repo, err := New(logger, Config{
		MemtablesFlushIdleAfter:   60,
		RootPath:                  dirName,
		QueryMaximumResults:       10,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))
	return NewMigrator(repo, logger), repo, schemaGetter
}

func createClassWithEverything(IndexNullState bool, IndexPropertyLength bool) *models.Class {
	return &models.Class{
		VectorIndexConfig: enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 60,
			Stopwords: &models.StopwordConfig{
				Preset: "none",
			},
			IndexNullState:      IndexNullState,
			IndexPropertyLength: IndexPropertyLength,
		},
		Class: "EverythingClass",
		Properties: []*models.Property{
			{
				Name:         "strings",
				DataType:     schema.DataTypeTextArray.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
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
				Name:         "string",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
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

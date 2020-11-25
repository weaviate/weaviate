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
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	uuid "github.com/satori/go.uuid"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This test aims to prevent a regression on
// https://github.com/semi-technologies/weaviate/issues/1308 where we
// discovered that if the first n doc ids are deleted and a filter would return
// <= n doc ids, it would return no results instead of skipping the deleted ids
// and returning the next ones
func Test_FilterSearchesOnDeletedDocIDsWithLimits(t *testing.T) {
	className := "DeletedDocIDLimitTestClass"
	rand.Seed(time.Now().UnixNano())
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()

	logger, _ := test.NewNullLogger()
	thingclass := &models.Class{
		Class: className,
		Properties: []*models.Property{{
			Name:     "unrelatedProp",
			DataType: []string{string(schema.DataTypeString)},
		}, {
			Name:     "boolProp",
			DataType: []string{string(schema.DataTypeBoolean)},
		}},
	}
	schemaGetter := &fakeSchemaGetter{}
	repo := New(logger, Config{RootPath: dirName})
	repo.SetSchemaGetter(schemaGetter)
	err := repo.WaitForStartup(30 * time.Second)
	require.Nil(t, err)
	migrator := NewMigrator(repo, logger)

	t.Run("creating the thing class", func(t *testing.T) {
		require.Nil(t,
			migrator.AddClass(context.Background(), kind.Thing, thingclass))

		// update schema getter so it's in sync with class
		schemaGetter.schema = schema.Schema{
			Things: &models.Schema{
				Classes: []*models.Class{thingclass},
			},
		}
	})

	var things []*models.Thing
	t.Run("importing 10 initial items", func(t *testing.T) {
		things = make([]*models.Thing, 10)
		for i := 0; i < 10; i++ {
			things[i] = &models.Thing{
				Class: className,
				ID:    mustNewUUID(),
				Schema: map[string]interface{}{
					"boolProp":      i < 5,
					"unrelatedProp": "initialValue",
				},
				Vector: []float32{0.1},
			}

			err := repo.PutThing(context.Background(), things[i], things[i].Vector)
			require.Nil(t, err)
		}
	})

	t.Run("updating the first 5 elements", func(t *testing.T) {
		// The idea is that the first 5 elements can be found with a boolProp==true
		// search, however, the bug occured if those items all had received an
		// update

		for i := 0; i < 5; i++ {
			things[i].Schema.(map[string]interface{})["unrelatedProp"] = "updatedValue"

			err := repo.PutThing(context.Background(), things[i], things[i].Vector)
			require.Nil(t, err)
		}
	})

	t.Run("searching for boolProp == true with a strict limit", func(t *testing.T) {
		res, err := repo.ClassSearch(context.Background(), traverser.GetParams{
			ClassName: className,
			Pagination: &filters.Pagination{
				// important as the first 5 doc ids we encounter now should all be
				// deleted
				Limit: 5,
			},
			Filters: buildFilter("boolProp", true, eq, dtBool),
			Kind:    kind.Thing,
		})
		expectedIDs := []strfmt.UUID{
			things[0].ID, things[1].ID, things[2].ID, things[3].ID, things[4].ID,
		}

		require.Nil(t, err)

		require.Len(t, res, 5)
		actualIDs := extractIDs(res)
		assert.Equal(t, expectedIDs, actualIDs)
	})
}

func mustNewUUID() strfmt.UUID {
	id, err := uuid.NewV4()
	if err != nil {
		panic(err)
	}

	return strfmt.UUID(id.String())
}

func extractIDs(in []search.Result) []strfmt.UUID {
	out := make([]strfmt.UUID, len(in), len(in))
	for i, res := range in {
		out[i] = res.ID
	}

	return out
}

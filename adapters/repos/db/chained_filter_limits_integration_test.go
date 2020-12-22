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

	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This test aims to prevent a regression on
// https://github.com/semi-technologies/weaviate/issues/1352
//
// It reuses the company-schema from the regular filters test, but runs them in
// isolation as to not interfere with the existing tests
func Test_LimitsOnChainedFilters(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()

	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{}
	repo := New(logger, Config{RootPath: dirName})
	repo.SetSchemaGetter(schemaGetter)
	err := repo.WaitForStartup(30 * time.Second)
	require.Nil(t, err)
	migrator := NewMigrator(repo, logger)

	t.Run("creating the class", func(t *testing.T) {
		schema := schema.Schema{
			Things: &models.Schema{
				Classes: []*models.Class{
					productClass,
					companyClass,
				},
			},
		}

		require.Nil(t,
			migrator.AddClass(context.Background(), kind.Thing, productClass))
		require.Nil(t,
			migrator.AddClass(context.Background(), kind.Thing, companyClass))

		schemaGetter.schema = schema
	})

	data := chainedFilterCompanies(100)

	t.Run("import companies", func(t *testing.T) {
		for i, company := range data {
			t.Run(fmt.Sprintf("importing product %d", i), func(t *testing.T) {
				require.Nil(t,
					repo.PutThing(context.Background(), company,
						[]float32{0.1, 0.2, 0.01, 0.2}))
			})
		}
	})

	t.Run("combine two filters with a strict limit", func(t *testing.T) {
		limit := 20

		filter := filterAnd(
			buildFilter("price", 20, gte, dtInt),
			buildFilter("price", 100, lt, dtInt),
		)

		res, err := repo.ClassSearch(context.Background(), traverser.GetParams{
			ClassName: companyClass.Class,
			Filters:   filter,
			Pagination: &filters.Pagination{
				Limit: limit,
			},
			Kind: kind.Thing,
		})

		require.Nil(t, err)
		assert.Len(t, res, limit)

		for _, obj := range res {
			assert.Less(t, obj.Schema.(map[string]interface{})["price"].(float64),
				float64(100))
			assert.GreaterOrEqual(t,
				obj.Schema.(map[string]interface{})["price"].(float64), float64(20))
		}
	})
}

func chainedFilterCompanies(size int) []*models.Thing {
	out := make([]*models.Thing, size)

	for i := range out {
		out[i] = &models.Thing{
			ID:    mustNewUUID(),
			Class: companyClass.Class,
			Schema: map[string]interface{}{
				"price": int64(i),
			},
		}
	}

	return out
}

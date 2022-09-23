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
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/entities/aggregation"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
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
	dirName := t.TempDir()

	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{shardState: singleShardState()}
	repo := New(logger, Config{
		FlushIdleAfter:            60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, nil)
	repo.SetSchemaGetter(schemaGetter)
	err := repo.WaitForStartup(testCtx())
	require.Nil(t, err)
	defer repo.Shutdown(context.Background())
	migrator := NewMigrator(repo, logger)

	t.Run("creating the class", func(t *testing.T) {
		schema := schema.Schema{
			Objects: &models.Schema{
				Classes: []*models.Class{
					productClass,
					companyClass,
				},
			},
		}

		require.Nil(t,
			migrator.AddClass(context.Background(), productClass, schemaGetter.shardState))
		require.Nil(t,
			migrator.AddClass(context.Background(), companyClass, schemaGetter.shardState))

		schemaGetter.schema = schema
	})

	data := chainedFilterCompanies(100)

	t.Run("import companies", func(t *testing.T) {
		for i, company := range data {
			t.Run(fmt.Sprintf("importing product %d", i), func(t *testing.T) {
				require.Nil(t,
					repo.PutObject(context.Background(), company,
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

func chainedFilterCompanies(size int) []*models.Object {
	out := make([]*models.Object, size)

	for i := range out {
		out[i] = &models.Object{
			ID:    mustNewUUID(),
			Class: companyClass.Class,
			Properties: map[string]interface{}{
				"price": int64(i),
			},
		}
	}

	return out
}

// This test aims to prevent a regression on
// https://github.com/semi-technologies/weaviate/issues/1355
//
// It reuses the company-schema from the regular filters test, but runs them in
// isolation as to not interfere with the existing tests
func Test_FilterLimitsAfterUpdates(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := t.TempDir()

	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{shardState: singleShardState()}
	repo := New(logger, Config{
		FlushIdleAfter:            60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, nil)
	repo.SetSchemaGetter(schemaGetter)
	err := repo.WaitForStartup(testCtx())
	require.Nil(t, err)
	migrator := NewMigrator(repo, logger)

	t.Run("creating the class", func(t *testing.T) {
		schema := schema.Schema{
			Objects: &models.Schema{
				Classes: []*models.Class{
					productClass,
					companyClass,
				},
			},
		}

		require.Nil(t,
			migrator.AddClass(context.Background(), productClass, schemaGetter.shardState))
		require.Nil(t,
			migrator.AddClass(context.Background(), companyClass, schemaGetter.shardState))

		schemaGetter.schema = schema
	})

	data := chainedFilterCompanies(100)

	t.Run("import companies", func(t *testing.T) {
		for i, company := range data {
			t.Run(fmt.Sprintf("importing product %d", i), func(t *testing.T) {
				require.Nil(t,
					repo.PutObject(context.Background(), company,
						[]float32{0.1, 0.2, 0.01, 0.2}))
			})
		}
	})

	t.Run("verify all with ref count 0 are found", func(t *testing.T) {
		limit := 100
		filter := buildFilter("makesProduct", 0, eq, dtInt)
		res, err := repo.ClassSearch(context.Background(), traverser.GetParams{
			ClassName: companyClass.Class,
			Filters:   filter,
			Pagination: &filters.Pagination{
				Limit: limit,
			},
		})

		require.Nil(t, err)
		assert.Len(t, res, limit)
	})

	t.Run("verify a non refcount prop", func(t *testing.T) {
		limit := 100
		filter := buildFilter("price", float64(0), gte, dtNumber)
		res, err := repo.ClassSearch(context.Background(), traverser.GetParams{
			ClassName: companyClass.Class,
			Filters:   filter,
			Pagination: &filters.Pagination{
				Limit: limit,
			},
		})

		require.Nil(t, err)
		assert.Len(t, res, limit)
	})

	t.Run("perform updates on each company", func(t *testing.T) {
		// in this case we're altering the vector position, but it doesn't really
		// matter - what we want to provoke is to fill up our index with deleted
		// doc ids
		for i, company := range data {
			t.Run(fmt.Sprintf("importing product %d", i), func(t *testing.T) {
				require.Nil(t,
					repo.PutObject(context.Background(), company,
						[]float32{0.1, 0.21, 0.01, 0.2}))
			})
		}
	})

	t.Run("verify all with ref count 0 are found", func(t *testing.T) {
		limit := 100
		filter := buildFilter("makesProduct", 0, eq, dtInt)
		res, err := repo.ClassSearch(context.Background(), traverser.GetParams{
			ClassName: companyClass.Class,
			Filters:   filter,
			Pagination: &filters.Pagination{
				Limit: limit,
			},
		})

		require.Nil(t, err)
		assert.Len(t, res, limit)
	})

	t.Run("verify a non refcount prop", func(t *testing.T) {
		limit := 100
		filter := buildFilter("price", float64(0), gte, dtNumber)
		res, err := repo.ClassSearch(context.Background(), traverser.GetParams{
			ClassName: companyClass.Class,
			Filters:   filter,
			Pagination: &filters.Pagination{
				Limit: limit,
			},
		})

		require.Nil(t, err)
		assert.Len(t, res, limit)
	})
}

// This test aims to prevent a regression on
// https://github.com/semi-technologies/weaviate/issues/1356
//
// It reuses the company-schema from the regular filters test, but runs them in
// isolation as to not interfere with the existing tests
func Test_AggregationsAfterUpdates(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := t.TempDir()

	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{shardState: singleShardState()}
	repo := New(logger, Config{
		FlushIdleAfter:            60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, nil)
	repo.SetSchemaGetter(schemaGetter)
	err := repo.WaitForStartup(testCtx())
	require.Nil(t, err)
	migrator := NewMigrator(repo, logger)

	t.Run("creating the class", func(t *testing.T) {
		schema := schema.Schema{
			Objects: &models.Schema{
				Classes: []*models.Class{
					productClass,
					companyClass,
				},
			},
		}

		require.Nil(t,
			migrator.AddClass(context.Background(), productClass, schemaGetter.shardState))
		require.Nil(t,
			migrator.AddClass(context.Background(), companyClass, schemaGetter.shardState))

		schemaGetter.schema = schema
	})

	data := chainedFilterCompanies(100)

	t.Run("import companies", func(t *testing.T) {
		for i, company := range data {
			t.Run(fmt.Sprintf("importing product %d", i), func(t *testing.T) {
				require.Nil(t,
					repo.PutObject(context.Background(), company,
						[]float32{0.1, 0.2, 0.01, 0.2}))
			})
		}
	})

	t.Run("verify all with ref count 0 are correctly aggregated",
		func(t *testing.T) {
			filter := buildFilter("makesProduct", 0, eq, dtInt)
			res, err := repo.Aggregate(context.Background(),
				aggregation.Params{
					ClassName:        schema.ClassName(companyClass.Class),
					Filters:          filter,
					IncludeMetaCount: true,
				})

			require.Nil(t, err)
			require.Len(t, res.Groups, 1)
			assert.Equal(t, res.Groups[0].Count, 100)
		})

	t.Run("perform updates on each company", func(t *testing.T) {
		// in this case we're altering the vector position, but it doesn't really
		// matter - what we want to provoke is to fill up our index with deleted
		// doc ids
		for i, company := range data {
			t.Run(fmt.Sprintf("importing product %d", i), func(t *testing.T) {
				require.Nil(t,
					repo.PutObject(context.Background(), company,
						[]float32{0.1, 0.21, 0.01, 0.2}))
			})
		}
	})

	t.Run("verify all with ref count 0 are correctly aggregated",
		func(t *testing.T) {
			filter := buildFilter("makesProduct", 0, eq, dtInt)
			res, err := repo.Aggregate(context.Background(),
				aggregation.Params{
					ClassName:        schema.ClassName(companyClass.Class),
					Filters:          filter,
					IncludeMetaCount: true,
				})

			require.Nil(t, err)
			require.Len(t, res.Groups, 1)
			assert.Equal(t, res.Groups[0].Count, 100)
		})

	t.Run("verify all with ref count 0 are correctly aggregated",
		func(t *testing.T) {
			filter := buildFilter("makesProduct", 0, eq, dtInt)
			res, err := repo.Aggregate(context.Background(),
				aggregation.Params{
					ClassName:        schema.ClassName(companyClass.Class),
					Filters:          filter,
					IncludeMetaCount: true,
				})

			require.Nil(t, err)
			require.Len(t, res.Groups, 1)
			assert.Equal(t, 100, res.Groups[0].Count)
		})
}

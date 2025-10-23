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

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"

	"github.com/weaviate/weaviate/usecases/memwatch"
)

func TestUpdateInvertedConfigStopwords(t *testing.T) {
	dirName := t.TempDir()

	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	repo, err := New(logger, Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, nil, nil, memwatch.NewDummyMonitor())
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(context.TODO()))
	defer repo.Shutdown(context.Background())

	props, migrator := SetupClass(t, repo, schemaGetter, logger, 1.2, 0.75, "en")

	className := schema.ClassName("MyClass")
	idx := repo.GetIndex(className)
	require.NotNil(t, idx)

	for _, location := range []string{"memory", "disk"} {
		t.Run("bm25f journey "+location, func(t *testing.T) {
			filter := &filters.LocalFilter{
				Root: &filters.Clause{
					On: &filters.Path{
						Class:    className,
						Property: schema.PropertyName("description"),
					},
					Value: &filters.Value{
						Value: []string{"journey"},
						Type:  schema.DataTypeText,
					},
					Operator: filters.ContainsAny,
				},
			}
			res, _, err := idx.objectSearch(context.TODO(), 1000, filter, nil, nil, nil, additional.Properties{}, nil, "", 0, props)
			require.Nil(t, err)
			t.Log("--- Start results for singleprop search ---")
			for _, r := range res {
				t.Logf("Result id: %v, title: %v\n", r.DocID, r.Object.Properties.(map[string]interface{})["description"])
			}
			require.Equal(t, len(res), 5)
		})

		t.Run("bm25f a "+location, func(t *testing.T) {
			filter := &filters.LocalFilter{
				Root: &filters.Clause{
					On: &filters.Path{
						Class:    className,
						Property: schema.PropertyName("description"),
					},
					Value: &filters.Value{
						Value: []string{"a"},
						Type:  schema.DataTypeText,
					},
					Operator: filters.ContainsAny,
				},
			}
			_, _, err := idx.objectSearch(context.TODO(), 1000, filter, nil, nil, nil, additional.Properties{}, nil, "", 0, props)
			require.Error(t, err)
		})

		for _, index := range repo.indices {
			index.ForEachShard(func(name string, shard ShardLike) error {
				err := shard.Store().FlushMemtables(context.Background())
				require.Nil(t, err)
				return nil
			})
		}
	}

	t.Run("update stopwords", func(t *testing.T) {
		class := repo.schemaGetter.ReadOnlyClass(className.String())
		class.InvertedIndexConfig.Stopwords = &models.StopwordConfig{
			Preset:    "en",
			Additions: []string{"journey"},
			Removals:  []string{"a"},
		}

		ctx := context.Background()
		err := migrator.UpdateInvertedIndexConfig(ctx, string(className), class.InvertedIndexConfig)
		require.Nil(t, err)
	})

	t.Run("Updated stopwords", func(t *testing.T) {
		t.Run("bm25f journey", func(t *testing.T) {
			filter := &filters.LocalFilter{
				Root: &filters.Clause{
					On: &filters.Path{
						Class:    className,
						Property: schema.PropertyName("description"),
					},
					Value: &filters.Value{
						Value: []string{"journey"},
						Type:  schema.DataTypeText,
					},
					Operator: filters.ContainsAny,
				},
			}
			_, _, err := idx.objectSearch(context.TODO(), 1000, filter, nil, nil, nil, additional.Properties{}, nil, "", 0, props)
			require.Error(t, err)
		})

		t.Run("bm25f a", func(t *testing.T) {
			filter := &filters.LocalFilter{
				Root: &filters.Clause{
					On: &filters.Path{
						Class:    className,
						Property: schema.PropertyName("description"),
					},
					Value: &filters.Value{
						Value: []string{"a"},
						Type:  schema.DataTypeText,
					},
					Operator: filters.ContainsAny,
				},
			}
			res, _, err := idx.objectSearch(context.TODO(), 1000, filter, nil, nil, nil, additional.Properties{}, nil, "", 0, props)
			require.Nil(t, err)
			t.Log("--- Start results for singleprop search ---")
			for _, r := range res {
				t.Logf("Result id: %v, title: %v\n", r.DocID, r.Object.Properties.(map[string]interface{})["description"])
			}
			require.Equal(t, len(res), 2)
		})
	})
}

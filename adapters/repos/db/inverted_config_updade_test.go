//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build integrationTest

package db

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/memwatch"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestUpdateInvertedConfigStopwords(t *testing.T) {
	dirName := t.TempDir()

	logger := logrus.New()
	shardState := singleShardState()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: shardState,
	}
	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(className string, retryIfClassNotFound bool, readerFunc func(*models.Class, *sharding.State) error) error {
		class := &models.Class{Class: className}
		return readerFunc(class, shardState)
	}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{}).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()
	repo, err := New(logger, "node1", Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{}, nil, nil, memwatch.NewDummyMonitor(),
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(context.TODO()))
	defer repo.Shutdown(context.Background())

	props, migrator := SetupClass(t, repo, schemaGetter, logger, 1.2, 0.75, "en")

	className := schema.ClassName("MyClass")
	idx := repo.GetIndex(className)
	require.NotNil(t, idx)

	t.Run("object query with filter journey (not stopword)", func(t *testing.T) {
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
		res, _, err := idx.objectSearch(context.TODO(), 1000, filter, nil, nil, nil, additional.Properties{}, nil, "", 0, props, nil)
		require.Nil(t, err)
		t.Log("--- Start results for singleprop search ---")
		for _, r := range res {
			t.Logf("Result id: %v, title: %v\n", r.DocID, r.Object.Properties.(map[string]interface{})["description"])
		}
		require.Equal(t, len(res), 5)
	})

	t.Run("object query with filter a (stopword)", func(t *testing.T) {
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
		_, _, err := idx.objectSearch(context.TODO(), 1000, filter, nil, nil, nil, additional.Properties{}, nil, "", 0, props, nil)
		require.Error(t, err)
	})

	t.Run("object query with filter a journey (stopword + not stopword)", func(t *testing.T) {
		filter := &filters.LocalFilter{
			Root: &filters.Clause{
				On: &filters.Path{
					Class:    className,
					Property: schema.PropertyName("description"),
				},
				Value: &filters.Value{
					Value: []string{"a", "journey"},
					Type:  schema.DataTypeText,
				},
				Operator: filters.ContainsAny,
			},
		}
		res, _, err := idx.objectSearch(context.TODO(), 1000, filter, nil, nil, nil, additional.Properties{}, nil, "", 0, props, nil)
		require.Nil(t, err)
		// all results contain "journey", "a" is a stopword and ignored
		require.Equal(t, len(res), 5)
	})

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
		t.Run("object query with filter journey (stopword)", func(t *testing.T) {
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
			_, _, err := idx.objectSearch(context.TODO(), 1000, filter, nil, nil, nil, additional.Properties{}, nil, "", 0, props, nil)
			// now an error, as "journey" was added to stopwords, and we are searching only for stopwords
			require.Error(t, err)
		})

		t.Run("object query with filter  a (not a stopword)", func(t *testing.T) {
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
			res, _, err := idx.objectSearch(context.TODO(), 1000, filter, nil, nil, nil, additional.Properties{}, nil, "", 0, props, nil)
			// now no error, as "a" was removed from stopwords
			require.Nil(t, err)
			// all results contain "a"
			require.Equal(t, len(res), 2)
		})

		t.Run("object query with filter a journey (not stopword + stopword)", func(t *testing.T) {
			filter := &filters.LocalFilter{
				Root: &filters.Clause{
					On: &filters.Path{
						Class:    className,
						Property: schema.PropertyName("description"),
					},
					Value: &filters.Value{
						Value: []string{"a", "journey"},
						Type:  schema.DataTypeText,
					},
					Operator: filters.ContainsAny,
				},
			}
			res, _, err := idx.objectSearch(context.TODO(), 1000, filter, nil, nil, nil, additional.Properties{}, nil, "", 0, props, nil)
			require.Nil(t, err)
			// all results contain "a", "journey" is now a stopword and ignored
			require.Equal(t, len(res), 2)
		})
	})
}

func TestUpdateInvertedConfigStopwordsPresetSwitch(t *testing.T) {
	dirName := t.TempDir()

	logger := logrus.New()
	shardState := singleShardState()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: shardState,
	}
	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(className string, retryIfClassNotFound bool, readerFunc func(*models.Class, *sharding.State) error) error {
		class := &models.Class{Class: className}
		return readerFunc(class, shardState)
	}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{}).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()
	repo, err := New(logger, "node1", Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{}, nil, nil, memwatch.NewDummyMonitor(),
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(context.TODO()))
	defer repo.Shutdown(context.Background())

	props, migrator := SetupClass(t, repo, schemaGetter, logger, 1.2, 0.75, "en")

	className := schema.ClassName("MyClass")
	idx := repo.GetIndex(className)
	require.NotNil(t, idx)

	filterPresetWord := &filters.LocalFilter{
		Root: &filters.Clause{
			On: &filters.Path{
				Class:    className,
				Property: schema.PropertyName("description"),
			},
			Value: &filters.Value{
				Value: []string{"the"},
				Type:  schema.DataTypeText,
			},
			Operator: filters.ContainsAny,
		},
	}

	t.Run("initial preset word is stopword", func(t *testing.T) {
		_, _, err := idx.objectSearch(context.TODO(), 1000, filterPresetWord, nil, nil, nil, additional.Properties{}, nil, "", 0, props, nil)
		require.Error(t, err)
	})

	additionWord := "journey"
	filterAddition := &filters.LocalFilter{
		Root: &filters.Clause{
			On: &filters.Path{
				Class:    className,
				Property: schema.PropertyName("description"),
			},
			Value: &filters.Value{
				Value: []string{additionWord},
				Type:  schema.DataTypeText,
			},
			Operator: filters.ContainsAny,
		},
	}

	t.Run("update preset en with addition/removal", func(t *testing.T) {
		class := repo.schemaGetter.ReadOnlyClass(className.String())
		class.InvertedIndexConfig.Stopwords = &models.StopwordConfig{
			Preset:    "en",
			Additions: []string{additionWord},
			Removals:  []string{"boo"},
		}

		// UpdateInvertedIndexConfig rebuilds the detector from config using
		// NewDetectorFromConfig, so the preset is recreated from "en" default.
		// Now we have all "en" stopwords plus "journey", with "boo" removed (if it existed).
		err := migrator.UpdateInvertedIndexConfig(context.Background(), string(className), class.InvertedIndexConfig)
		require.Nil(t, err)

		_, _, err = idx.objectSearch(context.TODO(), 1000, filterAddition, nil, nil, nil, additional.Properties{}, nil, "", 0, props, nil)
		require.Error(t, err)

		// "the" is still a stopword, as the preset was recreated from default config
		_, _, err = idx.objectSearch(context.TODO(), 1000, filterPresetWord, nil, nil, nil, additional.Properties{}, nil, "", 0, props, nil)
		require.Error(t, err)
	})

	t.Run("switch to none preset with new addition", func(t *testing.T) {
		class := repo.schemaGetter.ReadOnlyClass(className.String())
		class.InvertedIndexConfig.Stopwords = &models.StopwordConfig{
			Preset:    "none",
			Additions: []string{"custom"},
			Removals:  []string{},
		}

		// UpdateInvertedIndexConfig rebuilds the detector from scratch using
		// NewDetectorFromConfig. Switching to "none" preset clears all old "en"
		// words and previous additions. Only "custom" should be a stopword now.
		err := migrator.UpdateInvertedIndexConfig(context.Background(), string(className), class.InvertedIndexConfig)
		require.Nil(t, err)

		res, _, err := idx.objectSearch(context.TODO(), 1000, filterAddition, nil, nil, nil, additional.Properties{}, nil, "", 0, props, nil)
		require.Nil(t, err)
		require.Greater(t, len(res), 0)

		res, _, err = idx.objectSearch(context.TODO(), 1000, filterPresetWord, nil, nil, nil, additional.Properties{}, nil, "", 0, props, nil)
		require.Nil(t, err)
		require.Greater(t, len(res), 0)

		filterCustom := &filters.LocalFilter{
			Root: &filters.Clause{
				On: &filters.Path{
					Class:    className,
					Property: schema.PropertyName("description"),
				},
				Value: &filters.Value{
					Value: []string{"custom"},
					Type:  schema.DataTypeText,
				},
				Operator: filters.ContainsAny,
			},
		}

		_, _, err = idx.objectSearch(context.TODO(), 1000, filterCustom, nil, nil, nil, additional.Properties{}, nil, "", 0, props, nil)
		require.Error(t, err)
	})
}

func TestUpdateInvertedConfigStopwordsPersistence(t *testing.T) {
	dirName := t.TempDir()

	logger := logrus.New()
	shardState := singleShardState()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: shardState,
	}
	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(className string, retryIfClassNotFound bool, readerFunc func(*models.Class, *sharding.State) error) error {
		class := &models.Class{Class: className}
		return readerFunc(class, shardState)
	}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{}).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()
	repo, err := New(logger, "node1", Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{}, nil, nil, memwatch.NewDummyMonitor(),
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(context.TODO()))

	props, migrator := SetupClass(t, repo, schemaGetter, logger, 1.2, 0.75, "en")

	className := schema.ClassName("MyClass")
	idx := repo.GetIndex(className)
	require.NotNil(t, idx)

	t.Run("update stopwords", func(t *testing.T) {
		class := repo.schemaGetter.ReadOnlyClass(className.String())
		class.InvertedIndexConfig.Stopwords = &models.StopwordConfig{
			Preset:    "en",
			Additions: []string{},
			Removals:  []string{"a"},
		}

		ctx := context.Background()
		err := migrator.UpdateInvertedIndexConfig(ctx, string(className), class.InvertedIndexConfig)
		require.Nil(t, err)
	})

	t.Run("update stopwords", func(t *testing.T) {
		class := repo.schemaGetter.ReadOnlyClass(className.String())
		class.InvertedIndexConfig.Stopwords = &models.StopwordConfig{
			Preset:    "en",
			Additions: []string{"journey"},
			Removals:  []string{},
		}

		ctx := context.Background()
		err := migrator.UpdateInvertedIndexConfig(ctx, string(className), class.InvertedIndexConfig)
		require.Nil(t, err)
	})

	t.Run("Before shutdown", func(t *testing.T) {
		t.Run("object query with filter journey (stopword)", func(t *testing.T) {
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
			_, _, err := idx.objectSearch(context.TODO(), 1000, filter, nil, nil, nil, additional.Properties{}, nil, "", 0, props, nil)
			// now an error, as "journey" was added to stopwords, and we are searching only for stopwords
			require.Error(t, err)
		})

		t.Run("object query with filter  a (not a stopword)", func(t *testing.T) {
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
			_, _, err := idx.objectSearch(context.TODO(), 1000, filter, nil, nil, nil, additional.Properties{}, nil, "", 0, props, nil)
			// now error, as the "journey" update overwrote the previous removal of "a"
			require.Error(t, err)
		})

		t.Run("object query with filter a journey (not stopword + stopword)", func(t *testing.T) {
			filter := &filters.LocalFilter{
				Root: &filters.Clause{
					On: &filters.Path{
						Class:    className,
						Property: schema.PropertyName("description"),
					},
					Value: &filters.Value{
						Value: []string{"a", "journey"},
						Type:  schema.DataTypeText,
					},
					Operator: filters.ContainsAny,
				},
			}
			_, _, err := idx.objectSearch(context.TODO(), 1000, filter, nil, nil, nil, additional.Properties{}, nil, "", 0, props, nil)
			// now error, as the "journey" update overwrote the previous removal of "a", and they are both stopwords again
			require.Error(t, err)
		})
	})

	err = repo.Shutdown(context.Background())
	require.Nil(t, err)

	// reopen
	repo, err = New(logger, "node1", Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{}, nil, nil, memwatch.NewDummyMonitor(),
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(context.TODO()))
	defer repo.Shutdown(context.Background())

	idx = repo.GetIndex(className)
	require.NotNil(t, idx)

	t.Run("After shutdown", func(t *testing.T) {
		t.Run("object query with filter journey (stopword)", func(t *testing.T) {
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
			_, _, err := idx.objectSearch(context.TODO(), 1000, filter, nil, nil, nil, additional.Properties{}, nil, "", 0, props, nil)
			// now an error, as "journey" was added to stopwords, and we are searching only for stopwords
			require.Error(t, err)
		})

		t.Run("object query with filter  a (not a stopword)", func(t *testing.T) {
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
			_, _, err := idx.objectSearch(context.TODO(), 1000, filter, nil, nil, nil, additional.Properties{}, nil, "", 0, props, nil)
			// now error, as the "journey" update overwrote the previous removal of "a"
			require.Error(t, err)
		})

		t.Run("object query with filter a journey (not stopword + stopword)", func(t *testing.T) {
			filter := &filters.LocalFilter{
				Root: &filters.Clause{
					On: &filters.Path{
						Class:    className,
						Property: schema.PropertyName("description"),
					},
					Value: &filters.Value{
						Value: []string{"a", "journey"},
						Type:  schema.DataTypeText,
					},
					Operator: filters.ContainsAny,
				},
			}
			_, _, err := idx.objectSearch(context.TODO(), 1000, filter, nil, nil, nil, additional.Properties{}, nil, "", 0, props, nil)
			// now error, as the "journey" update overwrote the previous removal of "a", and they are both stopwords again
			require.Error(t, err)
		})
	})
}

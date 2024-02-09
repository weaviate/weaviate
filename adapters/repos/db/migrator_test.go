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

package db

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	vectorIndex "github.com/weaviate/weaviate/entities/vectorindex/common"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestUpdateIndex(t *testing.T) {
	var (
		rootpath = "./testdata/"
		ctx      = context.Background()
		r        = require.New(t)
		state1   = &sharding.State{Physical: map[string]sharding.Physical{"C1": {
			Name:           "node1",
			BelongsToNodes: []string{"node1"},
		}}}
		class1 = &models.Class{
			Class:             "C1",
			VectorIndexConfig: hnsw.UserConfig{Distance: vectorIndex.DefaultDistanceMetric},
			InvertedIndexConfig: &models.InvertedIndexConfig{
				CleanupIntervalSeconds: 60,
			},
			ReplicationConfig: &models.ReplicationConfig{
				Factor: 1,
			},
			VectorIndexType: "flat",
			Vectorizer:      "text2vec-contextionary",
		}
		// mi = newMigrator(t, 3, class1)
		class2 = &models.Class{
			Class:               "C2",
			InvertedIndexConfig: &models.InvertedIndexConfig{},
			VectorIndexConfig:   hnsw.UserConfig{Distance: vectorIndex.DefaultDistanceMetric},
		}
	)

	// init filesystem
	err := os.MkdirAll(rootpath, os.ModePerm)
	require.Nil(t, err)

	// init DB with class 1 and state 1
	db := testDB(rootpath, []*models.Class{class1}, map[string]*sharding.State{class1.Class: state1})
	db.indices = make(map[string]*Index)
	mi := NewMigrator(db, db.logger)

	t.Cleanup(func() {
		r.Nil(os.RemoveAll(rootpath))
	})

	t.Run("update existing class", func(t *testing.T) {
		err = mi.UpdateIndex(ctx, class1, state1)
		r.Nil(err)
		r.Equal(1, len(mi.db.indices))
		classIsInMemoryAndFileSystem(r, mi, class1, rootpath)

		// update class 1 InvertedIndexConfig
		class1.InvertedIndexConfig = &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 90,
		}

		err = mi.UpdateIndex(ctx, class1, state1)
		r.Nil(err)
		r.Equal(1, len(mi.db.indices))
		classIsInMemoryAndFileSystem(r, mi, class1, rootpath)

		// update class 1 Properties
		class1.Properties = []*models.Property{
			{
				Name:         "untouched_string",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
			{
				Name:         "touched_string",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
		}

		err = mi.UpdateIndex(ctx, class1, state1)
		r.Nil(err)
		r.Equal(1, len(mi.db.indices))
		classIsInMemoryAndFileSystem(r, mi, class1, rootpath)

		// update class 1 replication config
		class1.ReplicationConfig = &models.ReplicationConfig{Factor: 10}
		err = mi.UpdateIndex(ctx, class1, state1)
		r.Nil(err)
		r.Equal(1, len(mi.db.indices))
		classIsInMemoryAndFileSystem(r, mi, class1, rootpath)

		// update class 1 VectorIndexType
		class1.VectorIndexType = "hnsw"
		err = mi.UpdateIndex(ctx, class1, state1)
		r.Nil(err)
		r.Equal(1, len(mi.db.indices))
		classIsInMemoryAndFileSystem(r, mi, class1, rootpath)

		// update class 1 Vectorizer
		class1.Vectorizer = "custom-near-text-module"
		err = mi.UpdateIndex(ctx, class1, state1)
		r.Nil(err)
		r.Equal(1, len(mi.db.indices))
		classIsInMemoryAndFileSystem(r, mi, class1, rootpath)

		// update class 1 VectorIndexConfig
		class1.VectorIndexConfig = hnsw.UserConfig{Distance: vectorIndex.DefaultDistanceMetric, FlatSearchCutoff: 10}
		err = mi.UpdateIndex(ctx, class1, state1)
		r.Nil(err)
		r.Equal(1, len(mi.db.indices))
		classIsInMemoryAndFileSystem(r, mi, class1, rootpath)

		// update class 1 ShardingConfig
		class1.ShardingConfig = map[string]interface{}{
			"desiredCount": 1,
		}
		err = mi.UpdateIndex(ctx, class1, state1)
		r.Nil(err)
		r.Equal(1, len(mi.db.indices))
		classIsInMemoryAndFileSystem(r, mi, class1, rootpath)

		// update class 1 MultiTenancyConfig
		class1.MultiTenancyConfig = &models.MultiTenancyConfig{AutoTenantCreation: true, Enabled: true}
		err = mi.UpdateIndex(ctx, class1, state1)
		r.Nil(err)
		r.Equal(1, len(mi.db.indices))
		classIsInMemoryAndFileSystem(r, mi, class1, rootpath)

		// update class 1 ModuleConfig
		class1.ModuleConfig = map[string]interface{}{
			"my-module": map[string]interface{}{
				"vectorizeClassName": true,
			},
		}
		err = mi.UpdateIndex(ctx, class1, state1)
		r.Nil(err)
		r.Equal(1, len(mi.db.indices))
		classIsInMemoryAndFileSystem(r, mi, class1, rootpath)

		// update class 1 Description
		class1.Description = "updated"
		err = mi.UpdateIndex(ctx, class1, state1)
		r.Nil(err)
		r.Equal(1, len(mi.db.indices))
		classIsInMemoryAndFileSystem(r, mi, class1, rootpath)

		// update state
		state1.PartitioningEnabled = true
		err = mi.UpdateIndex(ctx, class1, state1)
		r.Nil(err)
		r.Equal(1, len(mi.db.indices))
		existedIndex := mi.db.GetIndex(schema.ClassName(class1.Class))
		r.Equal(existedIndex.partitioningEnabled, state1.PartitioningEnabled)
	})

	t.Run("create new class on updating it", func(t *testing.T) {
		state2 := &sharding.State{Physical: map[string]sharding.Physical{"C2": {
			Name:           "node1",
			BelongsToNodes: []string{"node1"},
		}}}
		err = mi.UpdateIndex(ctx, class2, state2)
		r.Nil(err)
		r.Equal(2, len(mi.db.indices))
		classIsInMemoryAndFileSystem(r, mi, class1, rootpath)
	})
}

func TestAddClass(t *testing.T) {
	var (
		ctx    = context.Background()
		r      = require.New(t)
		mi     = newMigrator(t, 3)
		class1 = &models.Class{
			Class:               "C1",
			InvertedIndexConfig: &models.InvertedIndexConfig{},
			VectorIndexConfig:   hnsw.UserConfig{Distance: vectorIndex.DefaultDistanceMetric},
		}
		class2 = &models.Class{
			Class:               "C2",
			InvertedIndexConfig: &models.InvertedIndexConfig{},
			VectorIndexConfig:   hnsw.UserConfig{Distance: vectorIndex.DefaultDistanceMetric},
		}
	)

	err := mi.AddClass(ctx, class1, &sharding.State{})
	r.Nil(err)

	err = mi.AddClass(ctx, class1, &sharding.State{})
	r.Nil(err)
	r.Equal(1, len(mi.db.indices))

	err = mi.AddClass(ctx, class2, &sharding.State{})
	r.Nil(err)
	r.Equal(2, len(mi.db.indices))
}

func TestDropClass(t *testing.T) {
	var (
		ctx    = context.Background()
		r      = require.New(t)
		mi     = newMigrator(t, 3)
		class1 = &models.Class{
			Class:               "C1",
			InvertedIndexConfig: &models.InvertedIndexConfig{},
			VectorIndexConfig:   hnsw.UserConfig{Distance: vectorIndex.DefaultDistanceMetric},
		}
	)

	err := mi.DropClass(ctx, "not existing class")
	r.Nil(err)

	err = mi.AddClass(ctx, class1, &sharding.State{})
	r.Nil(err)
	r.Equal(1, len(mi.db.indices))

	// check idempotence
	err = mi.DropClass(ctx, class1.Class)
	r.Nil(err)
	r.Equal(0, len(mi.db.indices))
	err = mi.DropClass(ctx, class1.Class)
	r.Nil(err)
	r.Equal(0, len(mi.db.indices))
}

func TestAddProperty(t *testing.T) {
	var (
		ctx    = context.Background()
		r      = require.New(t)
		mi     = newMigrator(t, 3)
		class1 = &models.Class{
			Class:               "C1",
			InvertedIndexConfig: &models.InvertedIndexConfig{},
			VectorIndexConfig:   hnsw.UserConfig{Distance: vectorIndex.DefaultDistanceMetric},
		}
	)

	err := mi.AddProperty(ctx, "NonExistingClass", &models.Property{
		Name:         "name",
		DataType:     schema.DataTypeText.PropString(),
		Tokenization: models.PropertyTokenizationWhitespace,
	})
	r.NotNil(err)

	err = mi.AddClass(ctx, class1, &sharding.State{})
	r.Nil(err)

	err = mi.AddProperty(ctx, class1.Class, &models.Property{
		Name:         "name",
		DataType:     schema.DataTypeText.PropString(),
		Tokenization: models.PropertyTokenizationWhitespace,
	})
	r.Nil(err)
}

func TestUpdateInvertedIndexConfig(t *testing.T) {
	var (
		ctx    = context.Background()
		r      = require.New(t)
		mi     = newMigrator(t, 3)
		class1 = &models.Class{
			Class:               "C1",
			InvertedIndexConfig: &models.InvertedIndexConfig{},
			VectorIndexConfig:   hnsw.UserConfig{Distance: vectorIndex.DefaultDistanceMetric},
		}

		invertedIdxConfig = &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 60,
			Stopwords: &models.StopwordConfig{
				Preset: "none",
			},
			IndexNullState:      true,
			IndexPropertyLength: true,
		}
	)

	err := mi.UpdateInvertedIndexConfig(ctx, "NonExistingClass", &models.InvertedIndexConfig{})
	r.NotNil(err)

	err = mi.AddClass(ctx, class1, &sharding.State{})
	r.Nil(err)

	err = mi.UpdateInvertedIndexConfig(ctx, class1.Class, invertedIdxConfig)
	r.Nil(err)
	r.True(mi.db.indices[indexID("C1")].getInvertedIndexConfig().IndexNullState)

	invertedIdxConfig.IndexNullState = false
	err = mi.UpdateInvertedIndexConfig(ctx, class1.Class, invertedIdxConfig)
	r.Nil(err)
	r.False(mi.db.indices[indexID("C1")].getInvertedIndexConfig().IndexNullState)
}

func TestUpdateVectorIndexConfig(t *testing.T) {
	var (
		ctx    = context.Background()
		r      = require.New(t)
		mi     = newMigrator(t, 3)
		class1 = &models.Class{
			Class:               "C1",
			InvertedIndexConfig: &models.InvertedIndexConfig{},
			VectorIndexConfig:   hnsw.UserConfig{Distance: vectorIndex.DefaultDistanceMetric},
		}
	)

	err := mi.UpdateVectorIndexConfig(ctx, "NonExistingClass", hnsw.UserConfig{Distance: vectorIndex.DefaultDistanceMetric})
	r.NotNil(err)

	err = mi.AddClass(ctx, class1, &sharding.State{})
	r.Nil(err)

	err = mi.UpdateVectorIndexConfig(ctx, "C1", hnsw.UserConfig{Distance: vectorIndex.DistanceHamming})
	r.Nil(err)
	r.Equal(vectorIndex.DistanceHamming, mi.db.indices[indexID("C1")].getVectorIndexConfig().DistanceName())

	err = mi.UpdateVectorIndexConfig(ctx, "C1", hnsw.UserConfig{Distance: vectorIndex.DistanceManhattan})
	r.Nil(err)
	r.Equal(vectorIndex.DistanceManhattan, mi.db.indices[indexID("C1")].getVectorIndexConfig().DistanceName())

	// confirm idempotence
	err = mi.UpdateVectorIndexConfig(ctx, "C1", hnsw.UserConfig{Distance: vectorIndex.DistanceManhattan})
	r.Nil(err)
	r.Equal(vectorIndex.DistanceManhattan, mi.db.indices[indexID("C1")].getVectorIndexConfig().DistanceName())
}

func newMigrator(t *testing.T, numClasses int, classes ...*models.Class) *Migrator {
	states := make(map[string]*sharding.State, numClasses)
	err := os.MkdirAll("./testdata/", os.ModePerm)
	require.Nil(t, err)
	db := testDB("./testdata/", classes, states)
	db.indices = make(map[string]*Index)
	return NewMigrator(db, db.logger)
}

func classIsInMemoryAndFileSystem(r *require.Assertions, mi *Migrator, c *models.Class, rootpath string) {
	existedIndex := mi.db.GetIndex(schema.ClassName(c.Class))
	r.Equal(existedIndex.getVectorIndexConfig(), c.VectorIndexConfig)
	existedClasses := existedIndex.getSchema.GetSchemaSkipAuth().Objects.Classes
	r.Equal(existedClasses[0], c)
	_, err := os.Stat(fmt.Sprintf("%s%s", rootpath, c.Class))
	r.Nil(err)
	r.True(classEqual(existedClasses[0], c))
}

func classEqual(x, y *models.Class) bool {
	xBytes, err := json.Marshal(x)
	if err != nil {
		return false
	}
	yBytes, err := json.Marshal(y)
	if err != nil {
		return false
	}
	return bytes.Equal(xBytes, yBytes)
}

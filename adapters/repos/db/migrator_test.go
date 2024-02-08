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
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	vectorIndex "github.com/weaviate/weaviate/entities/vectorindex/common"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/sharding"
)

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

func newMigrator(t *testing.T, numClasses int) *Migrator {
	classes := make([]*models.Class, numClasses)
	states := make(map[string]*sharding.State, numClasses)
	db := testDB(t.TempDir(), classes, states)
	db.indices = make(map[string]*Index)
	return NewMigrator(db, db.logger)
}

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

package reindex_test

import (
	"context"
	"testing"
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/entities/models"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
)

// testShardWithSettings preserves the legacy call shape from
// `package db` — third return is the fixture for unexported-field
// access via [db.IndexFixture] methods.
func testShardWithSettings(t *testing.T, ctx context.Context, class *models.Class,
	vic schemaConfig.VectorIndexConfig,
	withStopwords, withCheckpoints, withAsyncIndexingEnabled bool,
	indexOpts ...func(*db.Index),
) (db.ShardLike, *db.Index, *db.IndexFixture) {
	f := db.BuildIndexFixture(t, ctx, db.IndexFixtureOpts{
		Class:                    class,
		VectorIndexConfig:        vic,
		WithStopwords:            withStopwords,
		WithCheckpoints:          withCheckpoints,
		WithAsyncIndexingEnabled: withAsyncIndexingEnabled,
		IndexOpts:                indexOpts,
	})
	return f.Shard, f.Index, f
}

func testCtx() context.Context {
	//nolint:govet
	ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
	return ctx
}

func boolPtr(b bool) *bool { return &b }

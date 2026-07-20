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

package db

import (
	"context"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	resolver "github.com/weaviate/weaviate/adapters/repos/db/sharding"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/loadlimiter"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/monitoring"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// TestShard_PutObject_NilObjectsBucket_ReturnsUnprocessable reproduces the
// lifecycle race where a write reaches putObjectLSM while the objects bucket is
// momentarily absent (shard create/activate/teardown). Before the nil guard the
// write dereferenced a nil *lsmkv.Bucket in GetConsistentView and panicked the
// whole node; it must now return an enterrors.ErrUnprocessable instead. Keeping
// this assertion guards against the nil check being removed or moved below the
// dereference.
func TestShard_PutObject_NilObjectsBucket_ReturnsUnprocessable(t *testing.T) {
	ctx := context.Background()
	logger := logrus.New()

	class := &models.Class{
		Class:               "TestClass",
		InvertedIndexConfig: &models.InvertedIndexConfig{},
		MultiTenancyConfig:  &models.MultiTenancyConfig{Enabled: true},
	}

	mockSchemaGetter := schemaUC.NewMockSchemaGetter(t)
	mockSchemaGetter.On("NodeName").Return("node1")
	mockSchemaGetter.On("ReadOnlyClass", "TestClass").Return(class).Maybe()

	scheduler := queue.NewScheduler(queue.SchedulerOptions{Logger: logger, Workers: 1})

	shardState := &sharding.State{
		Physical: map[string]sharding.Physical{
			"shard1": {Name: "shard1", BelongsToNodes: []string{"node1"}, Status: models.TenantActivityStatusHOT},
		},
		PartitioningEnabled: true,
	}

	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
			return readFunc(class, shardState)
		}).Maybe()

	shardResolver := resolver.NewShardResolver(class.Class, class.MultiTenancyConfig.Enabled, mockSchemaGetter)

	index, err := NewIndex(ctx, IndexConfig{
		ClassName:         schema.ClassName("TestClass"),
		RootPath:          t.TempDir(),
		ReplicationFactor: 1,
		ShardLoadLimiter:  loadlimiter.NewLoadLimiter(monitoring.NoopRegisterer, "dummy", 1),
	}, inverted.ConfigFromModel(class.InvertedIndexConfig),
		hnsw.NewDefaultUserConfig(), nil, nil, shardResolver, mockSchemaGetter, mockSchemaReader, nil, logger, nil, nil, nil, nil, nil, class, nil, scheduler, nil, nil,
		NewShardReindexerV3Noop(), roaringset.NewBitmapBufPoolNoop(), false, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = index.Shutdown(ctx) })

	shard, err := NewShard(ctx, nil, "shard1", index, class, nil, scheduler, nil,
		NewShardReindexerV3Noop(), false, roaringset.NewBitmapBufPoolNoop())
	require.NoError(t, err)
	index.shards.Store("shard1", shard)

	// A healthy shard has the objects bucket.
	require.NotNil(t, shard.store.Bucket(helpers.ObjectsBucketLSM))

	// Simulate the create/activate/teardown race: the objects bucket is gone,
	// so store.Bucket(ObjectsBucketLSM) now returns nil.
	require.NoError(t, shard.store.ShutdownBucket(ctx, helpers.ObjectsBucketLSM))
	require.Nil(t, shard.store.Bucket(helpers.ObjectsBucketLSM))

	obj := &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:    strfmt.UUID("40d3be3e-2ecc-49c8-b37c-d8983164848b"),
			Class: "TestClass",
		},
	}

	var putErr error
	require.NotPanics(t, func() {
		putErr = shard.PutObject(ctx, obj)
	})
	require.Error(t, putErr)

	var unprocessable enterrors.ErrUnprocessable
	require.ErrorAs(t, putErr, &unprocessable)
}

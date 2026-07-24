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
	"sort"
	"sync"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	resolver "github.com/weaviate/weaviate/adapters/repos/db/sharding"
	"github.com/weaviate/weaviate/entities/loadlimiter"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/monitoring"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// Pins the same-opID race: target retries land twice server-side and each
// call's release deletes the other's dir mid-hardlink. With the lock,
// callers serialize and all return a consistent file list.
func TestIncomingCreateReplicaSnapshotConcurrent(t *testing.T) {
	mockSchemaGetter := schemaUC.NewMockSchemaGetter(t)
	mockSchemaGetter.On("NodeName").Return("node1")

	class := &models.Class{
		Class:               "TestClass",
		InvertedIndexConfig: &models.InvertedIndexConfig{},
		MultiTenancyConfig:  &models.MultiTenancyConfig{Enabled: true},
	}
	mockSchemaGetter.On("ReadOnlyClass", "TestClass").Return(class).Maybe()

	logger := logrus.New()
	scheduler := queue.NewScheduler(queue.SchedulerOptions{Logger: logger, Workers: 1})

	ss := &sharding.State{
		Physical: map[string]sharding.Physical{
			"shard1": {
				Name:           "shard1",
				BelongsToNodes: []string{"node1"},
				Status:         models.TenantActivityStatusHOT,
			},
		},
		PartitioningEnabled: true,
	}

	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ string, _ bool, readFunc func(*models.Class, *sharding.State) error) error {
			return readFunc(class, ss)
		}).Maybe()

	shardResolver := resolver.NewShardResolver(class.Class, class.MultiTenancyConfig.Enabled, mockSchemaGetter)

	index, err := NewIndex(context.Background(), IndexConfig{
		ClassName:         schema.ClassName("TestClass"),
		RootPath:          t.TempDir(),
		ReplicationFactor: 1,
		ShardLoadLimiter:  loadlimiter.NewLoadLimiter(monitoring.NoopRegisterer, "dummy", 1),
	}, inverted.ConfigFromModel(class.InvertedIndexConfig),
		hnsw.NewDefaultUserConfig(), nil, nil, shardResolver, mockSchemaGetter, mockSchemaReader,
		nil, logger, nil, nil, nil, nil, nil, class, nil, scheduler, nil, nil,
		NewShardReindexerV3Noop(), roaringset.NewBitmapBufPoolNoop(), false, nil)
	require.NoError(t, err)
	index.db = stubDBWithNoLiveReindex()

	shard, err := NewShard(context.Background(), nil, "shard1", index, class, nil, scheduler, nil,
		NewShardReindexerV3Noop(), false, roaringset.NewBitmapBufPoolNoop())
	require.NoError(t, err)
	index.shards.Store("shard1", shard)

	ctx := context.Background()

	require.NoError(t, index.IncomingPutObject(ctx, "shard1", &storobj.Object{
		MarshallerVersion: 1,
		DocID:             0,
		Object: models.Object{
			ID:    strfmt.UUID("40d3be3e-2ecc-49c8-b37c-d8983164848b"),
			Class: "TestClass",
		},
	}, 0))

	const (
		opID = "00000000-0000-0000-0000-000000000001"
		N    = 16
	)

	start := make(chan struct{})
	var wg sync.WaitGroup
	results := make([][]string, N)
	errs := make([]error, N)

	for i := 0; i < N; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results[i], errs[i] = index.IncomingCreateReplicaSnapshot(ctx, "shard1", opID)
		}()
	}
	close(start)
	wg.Wait()

	for i := 0; i < N; i++ {
		require.NoErrorf(t, errs[i], "goroutine %d failed: %v", i, errs[i])
		require.NotEmpty(t, results[i], "goroutine %d returned empty file list", i)
	}

	expected := append([]string(nil), results[0]...)
	sort.Strings(expected)
	for i := 1; i < N; i++ {
		got := append([]string(nil), results[i]...)
		sort.Strings(got)
		require.Equalf(t, expected, got, "file list mismatch between goroutine 0 and %d", i)
	}

	require.NoError(t, index.IncomingReleaseReplicaSnapshot(ctx, opID))
}

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
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

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

// Pins the fallback-mode bug: read RPCs never reset the watchdog, so long
// transfers force-resumed mid-stream and compaction could race the source.
// Active reads must keep haltForTransferCount > 0; once they stop, the
// watchdog should still fire (sanity post-check that the mechanism is sound).
func TestReplicaSnapshotFallbackInactivityTimerIsReset(t *testing.T) {
	t.Setenv("WEAVIATE_TEST_FORCE_NO_HARDLINK", "true")

	const (
		inactivityTimeout = 200 * time.Millisecond
		resetInterval     = 80 * time.Millisecond
		activeWindow      = 700 * time.Millisecond
	)

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
		ClassName:                 schema.ClassName("TestClass"),
		RootPath:                  t.TempDir(),
		ReplicationFactor:         1,
		ShardLoadLimiter:          loadlimiter.NewLoadLimiter(monitoring.NoopRegisterer, "dummy", 1),
		TransferInactivityTimeout: inactivityTimeout,
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

	const opID = "00000000-0000-0000-0000-000000000002"

	files, err := index.IncomingCreateReplicaSnapshot(ctx, "shard1", opID)
	require.NoError(t, err)
	require.NotEmpty(t, files)

	defer func() {
		// Stop the inactivity goroutine cleanly even if the test fails mid-way.
		_ = index.IncomingReleaseReplicaSnapshot(ctx, opID)
	}()

	// Sanity: the test must actually be exercising fallback mode.
	shard.haltForTransferMux.Lock()
	require.Greater(t, shard.haltForTransferCount, 0,
		"shard must be halted in fallback mode after IncomingCreateReplicaSnapshot")
	shard.haltForTransferMux.Unlock()

	// Drive read RPCs at half-timeout intervals across a window longer than
	// the timeout — resolveReplicaSnapshotPath must reset the timer on each.
	stop := make(chan struct{})
	var done atomic.Bool
	enterrors.GoWrapper(func() {
		ticker := time.NewTicker(resetInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				done.Store(true)
				return
			case <-ticker.C:
				_, _ = index.IncomingGetReplicaSnapshotFileMetadata(ctx, opID, files[0])
			}
		}
	}, logger)

	time.Sleep(activeWindow)

	shard.haltForTransferMux.Lock()
	haltCount := shard.haltForTransferCount
	shard.haltForTransferMux.Unlock()

	close(stop)
	require.Eventually(t, done.Load, 200*time.Millisecond, 10*time.Millisecond)

	require.Greaterf(t, haltCount, 0,
		"haltForTransferCount fell to 0 during active transfer — the watchdog fired "+
			"because the read RPCs did not reset the timer")

	// Once activity stops, the watchdog must still fire — otherwise the
	// test mechanism is broken.
	require.Eventually(t, func() bool {
		shard.haltForTransferMux.Lock()
		defer shard.haltForTransferMux.Unlock()
		return shard.haltForTransferCount == 0
	}, 3*inactivityTimeout, 20*time.Millisecond,
		"watchdog never fired after activity stopped — test mechanism is broken")
}

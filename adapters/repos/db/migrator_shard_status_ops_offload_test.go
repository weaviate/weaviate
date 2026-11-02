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
	"errors"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	command "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/schema/local"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestFreezeAbortRestoresShard(t *testing.T) {
	ctx := context.Background()
	const class = "FreezeAbortRestoresShard"

	sl, idx := testShard(t, ctx, class, asyncSchedulerOption(t, ctx))
	s := concreteShard(t, sl)
	t.Cleanup(func() { _ = sl.Shutdown(ctx) })
	setShardReplicas(t, idx, "node1", "node2")

	cfg := minAsyncReplicationConfig()
	require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidLow, tsFarPast)))
	require.NoError(t, s.store.FlushMemtables(ctx))
	require.NoError(t, s.enableAsyncReplication(ctx, cfg))
	awaitHashtreeInitialized(t, s)

	cases := []struct {
		name      string
		uploadErr error
		wantOp    command.TenantsProcess_Op
	}{
		{name: "this replica's upload failed", uploadErr: errors.New("s3 down"), wantOp: command.TenantsProcess_OP_ABORT},
		{name: "another replica aborted", wantOp: command.TenantsProcess_OP_DONE},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			m := NewMigrator(indexRegistry(idx), logger, "node1")
			m.SetNode("node1")
			cluster := &fakeTransitionCluster{}
			m.SetCluster(cluster)
			m.cloud = &fakeTransitionCloud{empty: map[string]bool{}, uploadErr: tc.uploadErr}

			record := freezeRecord(7)
			record.Class, record.Tenant = class, s.name
			cluster.setRecords(record)
			m.transitions.reconcile(ctx)
			require.Eventually(t, func() bool { return len(cluster.sent()) == 1 }, 5*time.Second, 10*time.Millisecond)
			require.Equal(t, tc.wantOp, cluster.sent()[0].TenantsProcesses[0].Op)
			require.EqualValues(t, 1, s.haltForTransferCount.Load(), "the shard stays halted until the attempt ends")

			cluster.setRecords()
			m.transitions.reconcile(ctx)

			require.Eventually(t, func() bool { return s.haltForTransferCount.Load() == 0 },
				5*time.Second, 10*time.Millisecond, "an aborted freeze must resume maintenance")
			awaitHashtreeInitialized(t, s)
		})
	}
}

func TestCompletedFreezeDoesNotResumeTheShard(t *testing.T) {
	ctx := context.Background()
	const class = "CompletedFreezeStaysHalted"

	sl, idx := testShard(t, ctx, class, asyncSchedulerOption(t, ctx))
	s := concreteShard(t, sl)
	t.Cleanup(func() { _ = sl.Shutdown(ctx) })
	idx.schemaReader = frozenSchemaReader(t, s.name)

	logger, _ := test.NewNullLogger()
	m := NewMigrator(indexRegistry(idx), logger, "node1")
	m.SetNode("node1")
	cluster := &fakeTransitionCluster{}
	m.SetCluster(cluster)
	m.cloud = &fakeTransitionCloud{empty: map[string]bool{}}

	record := freezeRecord(7)
	record.Class, record.Tenant = class, s.name
	cluster.setRecords(record)
	m.transitions.reconcile(ctx)
	require.Eventually(t, func() bool { return len(cluster.sent()) == 1 }, 5*time.Second, 10*time.Millisecond)

	cluster.setRecords()
	m.transitions.reconcile(ctx)

	require.Never(t, func() bool { return s.haltForTransferCount.Load() == 0 },
		200*time.Millisecond, 10*time.Millisecond)
	require.NoError(t, s.resumeMaintenanceCycles(ctx))
}

func frozenSchemaReader(t *testing.T, tenant string) *local.MockSchemaReader {
	t.Helper()
	reader := local.NewMockSchemaReader(t)
	reader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ string, _ bool, read func(*models.Class, *sharding.State) error) error {
			return read(nil, &sharding.State{Physical: map[string]sharding.Physical{
				tenant: {Name: tenant, Status: models.TenantActivityStatusFROZEN, CloudAttempt: 7},
			}})
		}).Maybe()
	reader.EXPECT().Shards(mock.Anything).Return([]string{tenant}, nil).Maybe()
	reader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	reader.EXPECT().ReadOnlyClass(mock.Anything).Return(&models.Class{}).Maybe()
	reader.EXPECT().ReadOnlySchema().Return(models.Schema{}).Maybe()
	reader.EXPECT().WaitForUpdate(mock.Anything, mock.Anything).Return(nil).Maybe()
	return reader
}

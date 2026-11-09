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

package cluster

import (
	"context"
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/cluster/utils"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/cluster/mocks"
	"github.com/weaviate/weaviate/usecases/fakes"
)

func TestStoreOpenLatchesStartedWithoutRaftState(t *testing.T) {
	ctx := context.Background()
	m := NewMockStore(t, "Node-1", utils.MustGetFreeTCPPort())
	s := NewFSM(m.cfg, nil, prometheus.NewPedanticRegistry())
	m.store = &s
	srv := NewRaft(mocks.NewMockNodeSelector(), m.store, nil)
	m.indexer.On("Open", mock.Anything).Return(nil)
	m.indexer.On("Close", mock.Anything).Return(nil)
	require.NoError(t, srv.Open(ctx, m.indexer))
	t.Cleanup(func() { require.NoError(t, srv.Close(ctx)) })

	require.True(t, srv.store.startedWithoutRaftState.Load())
}

type ctxRecordingIndexer struct {
	*fakes.MockSchemaExecutor
	ctx context.Context
}

func (r *ctxRecordingIndexer) ReloadLocalDB(ctx context.Context, all []cmd.UpdateClassRequest) error {
	r.ctx = ctx
	return r.MockSchemaExecutor.ReloadLocalDB(ctx, all)
}

func TestReloadDBFromSchemaTagsStartedWithoutRaftState(t *testing.T) {
	for _, started := range []bool{true, false} {
		t.Run(fmt.Sprintf("started_without_raft_state=%v", started), func(t *testing.T) {
			ms := NewMockStore(t, fmt.Sprintf("reload-ctx-%v", started), utils.MustGetFreeTCPPort())
			rec := &ctxRecordingIndexer{MockSchemaExecutor: fakes.NewMockSchemaExecutor()}
			cfg := ms.cfg
			cfg.DB = rec
			replicationFSM := schema.NewMockreplicationFSM(t)
			replicationFSM.EXPECT().HasActiveReplicationForCollection(mock.Anything).Return(false).Maybe()
			replicationFSM.EXPECT().HasActiveReplicationForShard(mock.Anything, mock.Anything).Return(false).Maybe()
			st := NewFSM(cfg, nil, prometheus.NewPedanticRegistry())
			st.schemaManager.SetReplicationFSM(replicationFSM)
			require.NoError(t, st.init())
			rec.On("TriggerSchemaUpdateCallbacks").Return()
			st.startedWithoutRaftState.Store(started)

			st.reloadDBFromSchema()

			require.NotNil(t, rec.ctx)
			require.Equal(t, started, enterrors.IsStartedWithoutRaftState(rec.ctx))
		})
	}
}

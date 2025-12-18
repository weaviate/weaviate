//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package clusterapi_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/clients"
	"github.com/weaviate/weaviate/usecases/backup"
	"github.com/weaviate/weaviate/usecases/cluster"
)

func TestInternalBackupsAPI(t *testing.T) {
	nodes := []*backupNode{
		{
			name:          "node1",
			backupManager: &fakeBackupManager{},
		},
		{
			name:          "node2",
			backupManager: &fakeBackupManager{},
		},
	}

	for _, node := range nodes {
		node.backupManager.On("OnCanCommit", &backup.Request{Method: backup.OpCreate}).
			Return(&backup.CanCommitResponse{})
		node.backupManager.On("OnCommit", &backup.StatusRequest{}).Return(nil)
		node.backupManager.On("OnAbort", &backup.AbortRequest{}).Return(nil)
	}

	resolver := cluster.NewMockNodeResolver(t)
	resolver.EXPECT().AllHostnames().Return([]string{})
	coord := newFakeCoordinator(resolver)

	t.Run("can commit, commit", func(t *testing.T) {
		err := coord.Backup(context.Background(), &backup.Request{Method: backup.OpCreate}, false)
		require.Nil(t, err)
	})

	t.Run("abort", func(t *testing.T) {
		err := coord.Backup(context.Background(), &backup.Request{Method: backup.OpCreate}, true)
		require.Nil(t, err)
	})
}

type backupNode struct {
	name          string
	backupManager *fakeBackupManager
}

func newFakeCoordinator(resolver cluster.NodeResolver) *fakeCoordinator {
	return &fakeCoordinator{
		client:       clients.NewClusterBackups(&http.Client{}),
		nodeResolver: resolver,
	}
}

type fakeCoordinator struct {
	client       *clients.ClusterBackups
	nodeResolver cluster.NodeResolver
}

func (c *fakeCoordinator) Backup(ctx context.Context, req *backup.Request, abort bool) error {
	if abort {
		return c.abort(ctx)
	}

	for _, host := range c.nodeResolver.AllHostnames() {
		_, err := c.client.CanCommit(ctx, host, req)
		if err != nil {
			return err
		}
	}

	for _, host := range c.nodeResolver.AllHostnames() {
		err := c.client.Commit(ctx, host, &backup.StatusRequest{})
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *fakeCoordinator) abort(ctx context.Context) error {
	for _, host := range c.nodeResolver.AllHostnames() {
		err := c.client.Abort(ctx, host, &backup.AbortRequest{})
		if err != nil {
			return err
		}
	}

	return nil
}

type fakeBackupManager struct {
	mock.Mock
}

func (m *fakeBackupManager) OnCanCommit(ctx context.Context, req *backup.Request) *backup.CanCommitResponse {
	args := m.Called(req)
	return args.Get(0).(*backup.CanCommitResponse)
}

func (m *fakeBackupManager) OnCommit(ctx context.Context, req *backup.StatusRequest) error {
	args := m.Called(req)
	return args.Error(0)
}

func (m *fakeBackupManager) OnAbort(ctx context.Context, req *backup.AbortRequest) error {
	args := m.Called(req)
	return args.Error(0)
}

func (m *fakeBackupManager) OnStatus(ctx context.Context, req *backup.StatusRequest) *backup.StatusResponse {
	args := m.Called(req)
	return args.Get(0).(*backup.StatusResponse)
}

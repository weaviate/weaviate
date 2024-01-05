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

package clusterapi_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/clients"
	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi"
	"github.com/weaviate/weaviate/usecases/backup"
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
	hosts := setupClusterAPI(t, nodes)

	for _, node := range nodes {
		node.backupManager.On("OnCanCommit", &backup.Request{Method: backup.OpCreate}).
			Return(&backup.CanCommitResponse{})
		node.backupManager.On("OnCommit", &backup.StatusRequest{}).Return(nil)
		node.backupManager.On("OnAbort", &backup.AbortRequest{}).Return(nil)
	}

	coord := newFakeCoordinator(newFakeNodeResolver(hosts))

	t.Run("can commit, commit", func(t *testing.T) {
		err := coord.Backup(context.Background(), &backup.Request{Method: backup.OpCreate}, false)
		require.Nil(t, err)
	})

	t.Run("abort", func(t *testing.T) {
		err := coord.Backup(context.Background(), &backup.Request{Method: backup.OpCreate}, true)
		require.Nil(t, err)
	})
}

func setupClusterAPI(t *testing.T, nodes []*backupNode) map[string]string {
	hosts := make(map[string]string)

	for _, node := range nodes {
		backupsHandler := clusterapi.NewBackups(node.backupManager, clusterapi.NewNoopAuthHandler())

		mux := http.NewServeMux()
		mux.Handle("/backups/can-commit", backupsHandler.CanCommit())
		mux.Handle("/backups/commit", backupsHandler.Commit())
		mux.Handle("/backups/abort", backupsHandler.Abort())
		mux.Handle("/backups/status", backupsHandler.Status())
		server := httptest.NewServer(mux)

		parsedURL, err := url.Parse(server.URL)
		require.Nil(t, err)

		hosts[node.name] = parsedURL.Host
	}

	return hosts
}

type backupNode struct {
	name          string
	backupManager *fakeBackupManager
}

func newFakeNodeResolver(hosts map[string]string) *fakeNodeResolver {
	return &fakeNodeResolver{hosts: hosts}
}

type fakeNodeResolver struct {
	hosts map[string]string
}

func (r *fakeNodeResolver) NodeHostName(nodeName string) (string, bool) {
	if host, ok := r.hosts[nodeName]; ok {
		return host, true
	}
	return "", false
}

func (r *fakeNodeResolver) HostNames() []string {
	hosts := make([]string, len(r.hosts))
	count := 0
	for _, host := range r.hosts {
		hosts[count] = host
		count++
	}
	return hosts
}

func newFakeCoordinator(resolver *fakeNodeResolver) *fakeCoordinator {
	return &fakeCoordinator{
		client:       clients.NewClusterBackups(&http.Client{}),
		nodeResolver: resolver,
	}
}

type fakeCoordinator struct {
	client       *clients.ClusterBackups
	nodeResolver *fakeNodeResolver
}

func (c *fakeCoordinator) Backup(ctx context.Context, req *backup.Request, abort bool) error {
	if abort {
		return c.abort(ctx)
	}

	for _, host := range c.nodeResolver.HostNames() {
		_, err := c.client.CanCommit(ctx, host, req)
		if err != nil {
			return err
		}
	}

	for _, host := range c.nodeResolver.HostNames() {
		err := c.client.Commit(ctx, host, &backup.StatusRequest{})
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *fakeCoordinator) abort(ctx context.Context) error {
	for _, host := range c.nodeResolver.HostNames() {
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

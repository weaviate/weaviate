//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

//go:build integrationTest
// +build integrationTest

package clusterintegrationtest

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"

	"github.com/semi-technologies/weaviate/adapters/clients"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/clusterapi"
	"github.com/semi-technologies/weaviate/adapters/repos/db"
	"github.com/semi-technologies/weaviate/entities/backup"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/schema"
	ubak "github.com/semi-technologies/weaviate/usecases/backup"
	"github.com/semi-technologies/weaviate/usecases/sharding"
	"github.com/sirupsen/logrus/hooks/test"
)

type node struct {
	name             string
	shardingState    *sharding.State
	repo             *db.DB
	schemaManager    *fakeSchemaManager
	backupManager    *ubak.Manager
	scheduler        *ubak.Scheduler
	clusterAPIServer *httptest.Server
	migrator         *db.Migrator
	hostname         string
}

func (n *node) init(dirName string, shardStateRaw []byte,
	allNodes *[]*node,
) {
	localDir := path.Join(dirName, n.name)
	logger, _ := test.NewNullLogger()

	nodeResolver := &nodeResolver{
		nodes: allNodes,
		local: n.name,
	}

	shardState, err := sharding.StateFromJSON(shardStateRaw, nodeResolver)
	if err != nil {
		panic(err)
	}

	client := clients.NewRemoteIndex(&http.Client{})
	nodesClient := clients.NewRemoteNode(&http.Client{})
	n.repo = db.New(logger, db.Config{
		FlushIdleAfter:            60,
		RootPath:                  localDir,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, client, nodeResolver, nodesClient, nil)
	n.schemaManager = &fakeSchemaManager{
		shardState: shardState,
		schema:     schema.Schema{Objects: &models.Schema{}},
	}

	n.repo.SetSchemaGetter(n.schemaManager)
	err = n.repo.WaitForStartup(context.Background())
	if err != nil {
		panic(err)
	}

	backendProvider := &fakeBackupBackendProvider{}
	n.backupManager = ubak.NewManager(
		n.name, logger, &fakeAuthorizer{}, n.schemaManager, n.repo, backendProvider)

	backupClient := clients.NewClusterBackups(&http.Client{})
	n.scheduler = ubak.NewScheduler(
		&fakeAuthorizer{}, backupClient, n.repo, backendProvider, nodeResolver, logger)

	n.migrator = db.NewMigrator(n.repo, logger)

	indices := clusterapi.NewIndices(sharding.NewRemoteIndexIncoming(n.repo))
	mux := http.NewServeMux()
	mux.Handle("/indices/", indices.Indices())

	backups := clusterapi.NewBackups(n.backupManager)
	mux.Handle("/backups/can-commit", backups.CanCommit())
	mux.Handle("/backups/commit", backups.Commit())
	mux.Handle("/backups/abort", backups.Abort())
	mux.Handle("/backups/status", backups.Status())

	srv := httptest.NewServer(mux)
	u, err := url.Parse(srv.URL)
	if err != nil {
		panic(err)
	}
	n.hostname = u.Host
}

type fakeNodes struct {
	nodes []string
}

func (f fakeNodes) AllNames() []string {
	return f.nodes
}

func (f fakeNodes) LocalName() string {
	return f.nodes[0]
}

type fakeSchemaManager struct {
	schema     schema.Schema
	shardState *sharding.State
}

func (f *fakeSchemaManager) GetSchemaSkipAuth() schema.Schema {
	return f.schema
}

func (f *fakeSchemaManager) ShardingState(class string) *sharding.State {
	return f.shardState
}

func (f *fakeSchemaManager) RestoreClass(ctx context.Context, d *backup.ClassDescriptor) error {
	return nil
}

func (f *fakeSchemaManager) Nodes() []string {
	return []string{"node1"}
}

func (f *fakeSchemaManager) NodeName() string {
	return "node1"
}

func (f *fakeSchemaManager) ClusterHealthScore() int {
	return 0
}

type nodeResolver struct {
	nodes *[]*node
	local string
}

func (r nodeResolver) AllNames() []string {
	panic("node resolving not implemented yet")
}

func (r nodeResolver) LocalName() string {
	return r.local
}

func (r nodeResolver) NodeHostname(nodeName string) (string, bool) {
	for _, node := range *r.nodes {
		if node.name == nodeName {
			return node.hostname, true
		}
	}

	return "", false
}

type fakeBackupBackendProvider struct{}

func (f *fakeBackupBackendProvider) BackupBackend(backend string) (modulecapabilities.BackupBackend, error) {
	backupsPath := os.Getenv("BACKUP_FILESYSTEM_PATH")

	return &fakeBackupBackend{
		store:       make(map[string][]byte),
		backupsPath: backupsPath,
	}, nil
}

type fakeBackupBackend struct {
	store       map[string][]byte
	backupsPath string
}

func (f *fakeBackupBackend) HomeDir(backupID string) string {
	return path.Join(f.backupsPath, backupID)
}

func (f *fakeBackupBackend) GetObject(ctx context.Context, backupID, key string) ([]byte, error) {
	storeKey := path.Join(backupID, key)
	if val, ok := f.store[storeKey]; ok {
		return val, nil
	}
	return nil, backup.ErrNotFound{}
}

func (f *fakeBackupBackend) WriteToFile(ctx context.Context, backupID, key, destPath string) error {
	storeKey := path.Join(backupID, key)
	contents, ok := f.store[storeKey]
	if !ok {
		return backup.ErrNotFound{}
	}

	if err := os.WriteFile(destPath, contents, os.ModePerm); err != nil {
		return err
	}

	return nil
}

func (f *fakeBackupBackend) SourceDataPath() string {
	return f.backupsPath
}

func (f *fakeBackupBackend) PutFile(ctx context.Context, backupID, key, srcPath string) error {
	contents, err := os.ReadFile(srcPath)
	if err != nil {
		return err
	}

	storeKey := path.Join(backupID, key)
	f.store[storeKey] = contents
	return nil
}

func (f *fakeBackupBackend) PutObject(ctx context.Context, backupID, key string, byes []byte) error {
	storeKey := path.Join(backupID, key)
	f.store[storeKey] = byes
	return nil
}

func (f *fakeBackupBackend) Initialize(ctx context.Context, backupID string) error {
	return nil
}

type fakeObjectStore struct {
	modulecapabilities.BackupBackend
}

func (f *fakeObjectStore) Meta(ctx context.Context, backupID string) (*backup.BackupDescriptor, error) {
	return nil, nil
}

func (f *fakeObjectStore) PutMeta(ctx context.Context, desc *backup.BackupDescriptor) error {
	return nil
}

func (f *fakeObjectStore) GlobalMeta(ctx context.Context, backupID string) (*backup.DistributedBackupDescriptor, error) {
	return nil, nil
}

func (f *fakeObjectStore) PutGlobalMeta(ctx context.Context, desc *backup.DistributedBackupDescriptor) error {
	return nil
}

type fakeAuthorizer struct{}

func (f *fakeAuthorizer) Authorize(_ *models.Principal, _, _ string) error {
	return nil
}

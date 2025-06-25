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

//go:build integrationTest

package clusterintegrationtest

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"sync"
	"time"

	"github.com/sirupsen/logrus/hooks/test"

	"github.com/weaviate/weaviate/adapters/clients"
	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/schema"
	modstgfs "github.com/weaviate/weaviate/modules/backup-filesystem"
	ubak "github.com/weaviate/weaviate/usecases/backup"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/cluster/mocks"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/modules"
	"github.com/weaviate/weaviate/usecases/sharding"
)

type node struct {
	name          string
	repo          *db.DB
	schemaManager *fakeSchemaManager
	backupManager *ubak.Handler
	scheduler     *ubak.Scheduler
	migrator      *db.Migrator
	hostname      string
}

func (n *node) init(dirName string, shardStateRaw []byte,
	allNodes *[]*node,
) {
	localDir := path.Join(dirName, n.name)
	logger, _ := test.NewNullLogger()

	var names []string
	for _, node := range *allNodes {
		names = append(names, node.name)
	}
	nodeResolver := &nodeResolver{
		NodeSelector: mocks.NewMockNodeSelector(names...),
		nodes:        allNodes,
		local:        n.name,
	}

	os.Setenv("ASYNC_INDEXING_STALE_TIMEOUT", "1s")
	shardState, err := sharding.StateFromJSON(shardStateRaw, nodeResolver)
	if err != nil {
		panic(err)
	}

	client := clients.NewRemoteIndex(&http.Client{})
	nodesClient := clients.NewRemoteNode(&http.Client{})
	replicaClient := clients.NewReplicationClient(&http.Client{})
	n.repo, err = db.New(logger, db.Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  localDir,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, client, nodeResolver, nodesClient, replicaClient, nil, memwatch.NewDummyMonitor())
	if err != nil {
		panic(err)
	}
	n.schemaManager = &fakeSchemaManager{
		shardState:   shardState,
		schema:       schema.Schema{Objects: &models.Schema{}},
		nodeResolver: nodeResolver,
	}

	n.repo.SetSchemaGetter(n.schemaManager)
	err = n.repo.WaitForStartup(context.Background())
	if err != nil {
		panic(err)
	}

	backendProvider := newFakeBackupBackendProvider(localDir)
	n.backupManager = ubak.NewHandler(
		logger, &fakeAuthorizer{}, n.schemaManager, n.repo, backendProvider, fakeRbacBackupWrapper{}, fakeRbacBackupWrapper{},
	)

	backupClient := clients.NewClusterBackups(&http.Client{})
	n.scheduler = ubak.NewScheduler(
		&fakeAuthorizer{}, backupClient, n.repo, backendProvider, nodeResolver, n.schemaManager, logger)

	n.migrator = db.NewMigrator(n.repo, logger)

	indices := clusterapi.NewIndices(sharding.NewRemoteIndexIncoming(n.repo, n.schemaManager, modules.NewProvider(logger, config.Config{})),
		n.repo, clusterapi.NewNoopAuthHandler(), func() bool { return false }, logger)
	mux := http.NewServeMux()
	mux.Handle("/indices/", indices.Indices())

	backups := clusterapi.NewBackups(n.backupManager, clusterapi.NewNoopAuthHandler())
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

type fakeRbacBackupWrapper struct{}

func (r fakeRbacBackupWrapper) GetBackupItems(context.Context) (map[string][]byte, error) {
	return nil, nil
}

func (r fakeRbacBackupWrapper) WriteBackupItems(context.Context, map[string][]byte) error {
	return nil
}

func (r fakeRbacBackupWrapper) Snapshot() ([]byte, error) {
	return nil, nil
}

func (r fakeRbacBackupWrapper) Restore([]byte) error {
	return nil
}

type fakeSchemaManager struct {
	schema       schema.Schema
	shardState   *sharding.State
	nodeResolver *nodeResolver
}

func (f *fakeSchemaManager) GetSchemaSkipAuth() schema.Schema {
	return f.schema
}

func (f *fakeSchemaManager) ReadOnlyClass(class string) *models.Class {
	return f.schema.GetClass(class)
}

func (f *fakeSchemaManager) ReadOnlyClassWithVersion(ctx context.Context, class string, version uint64,
) (*models.Class, error) {
	return f.schema.GetClass(class), nil
}

func (f *fakeSchemaManager) CopyShardingState(class string) *sharding.State {
	return f.shardState
}

func (f *fakeSchemaManager) Statistics() map[string]any {
	return nil
}

func (f *fakeSchemaManager) ShardOwner(class, shard string) (string, error) {
	ss := f.shardState
	x, ok := ss.Physical[shard]
	if !ok {
		return "", fmt.Errorf("shard not found")
	}
	if len(x.BelongsToNodes) < 1 || x.BelongsToNodes[0] == "" {
		return "", fmt.Errorf("owner node not found")
	}
	return ss.Physical[shard].BelongsToNodes[0], nil
}

func (f *fakeSchemaManager) ShardReplicas(class, shard string) ([]string, error) {
	ss := f.shardState
	x, ok := ss.Physical[shard]
	if !ok {
		return nil, fmt.Errorf("shard not found")
	}
	return x.BelongsToNodes, nil
}

func (f *fakeSchemaManager) TenantsShards(_ context.Context, class string, tenants ...string) (map[string]string, error) {
	res := map[string]string{}
	for _, t := range tenants {
		res[t] = models.TenantActivityStatusHOT
	}
	return res, nil
}

func (f *fakeSchemaManager) OptimisticTenantStatus(_ context.Context, class string, tenant string) (map[string]string, error) {
	res := map[string]string{}
	res[tenant] = models.TenantActivityStatusHOT
	return res, nil
}

func (f *fakeSchemaManager) ShardFromUUID(class string, uuid []byte) string {
	ss := f.shardState
	return ss.Shard("", string(uuid))
}

func (f *fakeSchemaManager) RestoreClass(ctx context.Context, d *backup.ClassDescriptor, nodeMapping map[string]string) error {
	return nil
}

func (f *fakeSchemaManager) Nodes() []string {
	return []string{"NOT SET"}
}

func (f *fakeSchemaManager) NodeName() string {
	return f.nodeResolver.local
}

func (f *fakeSchemaManager) ClusterHealthScore() int {
	return 0
}

func (f *fakeSchemaManager) ResolveParentNodes(_ string, shard string,
) (map[string]string, error) {
	return nil, nil
}

func (f *fakeSchemaManager) StorageCandidates() []string {
	return []string{}
}

type nodeResolver struct {
	cluster.NodeSelector
	nodes *[]*node
	local string
}

func (r nodeResolver) AllHostnames() []string {
	panic("node resolving not implemented yet")
}

func (r nodeResolver) AllNames() []string {
	xs := []string{}
	for _, n := range *r.nodes {
		xs = append(xs, n.name)
	}
	return xs
}

func (r nodeResolver) NodeCount() int {
	return len(*r.nodes)
}

// LocalName keep it to override the common mock of cluster.NodeSelector
func (r nodeResolver) LocalName() string {
	return r.local
}

// NodeHostname keep it to override the common mock of cluster.NodeSelector
func (r nodeResolver) NodeHostname(nodeName string) (string, bool) {
	for _, node := range *r.nodes {
		if node.name == nodeName {
			return node.hostname, true
		}
	}

	return "", false
}

func (r nodeResolver) LeaderID() string {
	if r.nodes != nil && len(*r.nodes) > 0 {
		return (*r.nodes)[0].name
	}
	return ""
}

func newFakeBackupBackendProvider(backupsPath string) *fakeBackupBackendProvider {
	return &fakeBackupBackendProvider{
		backupsPath: backupsPath,
	}
}

type fakeBackupBackendProvider struct {
	backupsPath string
}

func (f *fakeBackupBackendProvider) BackupBackend(name string) (modulecapabilities.BackupBackend, error) {
	backend.setLocal(name == modstgfs.Name)
	return backend, nil
}

func (f *fakeBackupBackendProvider) EnabledBackupBackends() []modulecapabilities.BackupBackend {
	return nil
}

type fakeBackupBackend struct {
	sync.Mutex
	backupsPath string
	backupID    string
	counter     int
	isLocal     bool
	startedAt   time.Time
}

func (f *fakeBackupBackend) HomeDir(backupID, overrideBucket, overridePath string) string {
	f.Lock()
	defer f.Unlock()
	if overridePath != "" {
		if overrideBucket != "" {
			return path.Join(overrideBucket, overridePath, backupID)
		} else {
			return path.Join(overridePath, backupID)
		}
	} else {
		if overrideBucket != "" {
			return path.Join(overrideBucket, f.backupsPath, backupID)
		} else {
			return path.Join(f.backupsPath, backupID)
		}
	}
}

func (f *fakeBackupBackend) AllBackups(context.Context) ([]*backup.DistributedBackupDescriptor, error) {
	return nil, fmt.Errorf("not implemented")
}

func (f *fakeBackupBackend) GetObject(ctx context.Context, backupID, key, overrideBucket, overridePath string) ([]byte, error) {
	f.Lock()
	defer f.Unlock()

	f.counter++

	if f.counter <= 2 {
		return nil, backup.ErrNotFound{}
	}

	var resp interface{}

	if key == ubak.GlobalBackupFile {
		resp = f.successGlobalMeta()
	} else {
		resp = f.successLocalMeta()
	}

	b, _ := json.Marshal(resp)
	return b, nil
}

func (f *fakeBackupBackend) WriteToFile(ctx context.Context, backupID, key, destPath, overrideBucket, overridePath string) error {
	f.Lock()
	defer f.Unlock()
	return nil
}

func (f *fakeBackupBackend) Write(ctx context.Context, backupID, key, overrideBucket, overridePath string, r io.ReadCloser) (int64, error) {
	f.Lock()
	defer f.Unlock()
	defer r.Close()
	return 0, nil
}

func (f *fakeBackupBackend) Read(ctx context.Context, backupID, key, overrideBucket, overridePath string, w io.WriteCloser) (int64, error) {
	f.Lock()
	defer f.Unlock()
	defer w.Close()
	return 0, nil
}

func (f *fakeBackupBackend) SourceDataPath() string {
	f.Lock()
	defer f.Unlock()
	return f.backupsPath
}

func (f *fakeBackupBackend) setLocal(v bool) {
	f.Lock()
	defer f.Unlock()
	f.isLocal = v
}

func (f *fakeBackupBackend) IsExternal() bool {
	f.Lock()
	defer f.Unlock()
	return !f.isLocal
}

func (f *fakeBackupBackend) Name() string {
	return "fakeBackupBackend"
}

func (f *fakeBackupBackend) PutFile(ctx context.Context, backupID, key, srcPath, bucket, bucketPath string) error {
	f.Lock()
	defer f.Unlock()
	return nil
}

func (f *fakeBackupBackend) PutObject(ctx context.Context, backupID, key, bucket, bucketPath string, byes []byte) error {
	f.Lock()
	defer f.Unlock()
	return nil
}

func (f *fakeBackupBackend) Initialize(ctx context.Context, backupID, overrideBucket, overridePath string) error {
	f.Lock()
	defer f.Unlock()
	return nil
}

func (f *fakeBackupBackend) successGlobalMeta() backup.DistributedBackupDescriptor {
	return backup.DistributedBackupDescriptor{
		StartedAt: f.startedAt,
		ID:        f.backupID,
		Nodes: map[string]*backup.NodeDescriptor{
			"node-0": {
				Classes: []string{distributedClass},
				Status:  "SUCCESS",
			},
		},
		Status:        "SUCCESS",
		Version:       ubak.Version,
		ServerVersion: "x.x.x",
	}
}

func (f *fakeBackupBackend) successLocalMeta() backup.BackupDescriptor {
	return backup.BackupDescriptor{
		ID:            f.backupID,
		Status:        "SUCCESS",
		ServerVersion: "x.x.x",
		Version:       ubak.Version,
		StartedAt:     f.startedAt,
		Classes: []backup.ClassDescriptor{
			{
				Name: distributedClass,
				Shards: []*backup.ShardDescriptor{
					{
						Name:                  "123",
						Node:                  "node-0",
						Files:                 []string{"some-file.db"},
						DocIDCounter:          []byte("1"),
						DocIDCounterPath:      ".",
						Version:               []byte("1"),
						ShardVersionPath:      ".",
						PropLengthTracker:     []byte("1"),
						PropLengthTrackerPath: ".",
					},
				},
				ShardingState: []byte("sharding state!"),
				Schema:        []byte("schema!"),
			},
		},
	}
}

func (f *fakeBackupBackend) reset() {
	f.counter = 0
}

type fakeAuthorizer struct{}

func (f *fakeAuthorizer) Authorize(ctx context.Context, _ *models.Principal, _ string, _ ...string) error {
	return nil
}

func (f *fakeAuthorizer) AuthorizeSilent(ctx context.Context, _ *models.Principal, _ string, _ ...string) error {
	return nil
}

func (f *fakeAuthorizer) FilterAuthorizedResources(ctx context.Context, _ *models.Principal, _ string, resources ...string) ([]string, error) {
	return resources, nil
}

package shard

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	routerTypes "github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
)

const (
	replicatorTestClass = "ReplicatorTestClass"
	replicatorTestShard = "replicator-test-shard"
	replicatorTestNode  = "node-1"
)

type fakeReplicatorShard struct {
	putCalls int
}

func (f *fakeReplicatorShard) PutObject(ctx context.Context, obj *storobj.Object) error {
	f.putCalls++
	return nil
}

func (f *fakeReplicatorShard) DeleteObject(ctx context.Context, id strfmt.UUID, deletionTime time.Time) error {
	return nil
}

func (f *fakeReplicatorShard) MergeObject(ctx context.Context, merge objects.MergeDocument) error {
	return nil
}

func (f *fakeReplicatorShard) PutObjectBatch(ctx context.Context, objects []*storobj.Object) []error {
	return make([]error, len(objects))
}

func (f *fakeReplicatorShard) DeleteObjectBatch(ctx context.Context, uuids []strfmt.UUID, deletionTime time.Time, dryRun bool) objects.BatchSimpleObjects {
	out := make(objects.BatchSimpleObjects, len(uuids))
	for i, id := range uuids {
		out[i] = objects.BatchSimpleObject{UUID: id}
	}
	return out
}

func (f *fakeReplicatorShard) AddReferencesBatch(ctx context.Context, refs objects.BatchReferences) []error {
	return make([]error, len(refs))
}

func (f *fakeReplicatorShard) FlushMemtables(ctx context.Context) error {
	return nil
}

func (f *fakeReplicatorShard) CreateTransferSnapshot(ctx context.Context) (TransferSnapshot, error) {
	return TransferSnapshot{}, nil
}

func (f *fakeReplicatorShard) ReleaseTransferSnapshot(snapshotID string) error {
	return nil
}

func (f *fakeReplicatorShard) Name() string {
	return replicatorTestShard
}

func newReplicatorTestStore(t *testing.T, members []string) (*Store, *fakeReplicatorShard) {
	t.Helper()

	_, transport := raft.NewInmemTransport("")
	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)

	cfg := StoreConfig{
		ClassName:          replicatorTestClass,
		ShardName:          replicatorTestShard,
		NodeID:             replicatorTestNode,
		DataPath:           t.TempDir(),
		Members:            members,
		Logger:             logger,
		Transport:          transport,
		HeartbeatTimeout:   150 * time.Millisecond,
		ElectionTimeout:    150 * time.Millisecond,
		LeaderLeaseTimeout: 100 * time.Millisecond,
		SnapshotInterval:   10 * time.Second,
		SnapshotThreshold:  1024,
	}

	store, err := NewStore(cfg)
	require.NoError(t, err)

	fakeShard := &fakeReplicatorShard{}
	store.SetShard(fakeShard)

	require.NoError(t, store.Start(context.Background()))
	t.Cleanup(func() { _ = store.Stop() })

	return store, fakeShard
}

func newReplicatorWithStore(store *Store) *replicator {
	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)

	r := &Raft{
		config: RaftConfig{
			ClassName: replicatorTestClass,
			Logger:    logger,
		},
		started: true,
		log: logger.WithFields(logrus.Fields{
			"component": "index_raft_manager",
			"class":     replicatorTestClass,
		}),
	}
	r.stores.Store(replicatorTestShard, store)

	return Newreplicator(RouterConfig{
		Logger:    logger,
		Raft:      r,
		ClassName: replicatorTestClass,
	})
}

func testObject() *storobj.Object {
	obj := &models.Object{
		Class: replicatorTestClass,
		ID:    strfmt.UUID(uuid.NewString()),
	}
	return storobj.FromObject(obj, nil, nil, nil)
}

func TestReplicatorPutObject_WaitsForLeader_Timeout(t *testing.T) {
	store, _ := newReplicatorTestStore(t, []string{replicatorTestNode, "missing-node-2", "missing-node-3"})
	rep := newReplicatorWithStore(store)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err := rep.PutObject(ctx, replicatorTestShard, testObject(), routerTypes.ConsistencyLevelEventual, 1)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrLeaderElectionTimeout))
}

func TestReplicatorPutObject_WaitsForLeader_Succeeds(t *testing.T) {
	store, fakeShard := newReplicatorTestStore(t, []string{replicatorTestNode})
	rep := newReplicatorWithStore(store)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, rep.PutObject(ctx, replicatorTestShard, testObject(), routerTypes.ConsistencyLevelEventual, 1))
	require.Equal(t, 1, fakeShard.putCalls)
}

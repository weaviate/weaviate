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

package backup

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func marshalState(t *testing.T, shardReplicas map[string][]string) []byte {
	t.Helper()
	state := sharding.State{Physical: map[string]sharding.Physical{}}
	for shard, nodes := range shardReplicas {
		state.Physical[shard] = sharding.Physical{Name: shard, BelongsToNodes: nodes}
	}
	raw, err := json.Marshal(state)
	require.NoError(t, err)
	return raw
}

type strictResolver struct{ *fakeNodeResolver }

func (r *strictResolver) NodeHostname(name string) (string, bool) {
	host, ok := r.hosts[name]
	return host, ok
}

func TestExpandParticipantsForDedupe(t *testing.T) {
	const class = "Class-A"

	newCoord := func(resolved ...string) (*coordinator, *Request, []backup.ClassDescriptor) {
		fc := newFakeCoordinator(&strictResolver{newFakeNodeResolver(resolved)})
		c := fc.coordinator()
		c.descriptor = &backup.DistributedBackupDescriptor{
			ID:             "1",
			DedupeReplicas: true,
			Nodes:          map[string]*backup.NodeDescriptor{"N1": {Classes: []string{class}}},
		}
		schema := []backup.ClassDescriptor{{
			Name:          class,
			ShardingState: marshalState(t, map[string][]string{"s1": {"N1", "N2", "N3"}}),
		}}
		return c, &Request{Method: OpRestore, ID: "1", Classes: []string{class}}, schema
	}

	t.Run("adds every replica as participant", func(t *testing.T) {
		c, req, schema := newCoord("N1", "N2", "N3")
		require.NoError(t, c.expandParticipantsForDedupe(req, schema))
		assert.Equal(t, []string{"N1"}, req.SourceNodes)
		assert.True(t, req.DedupeReplicas)
		require.Len(t, c.descriptor.Nodes, 3)
		for _, node := range []string{"N1", "N2", "N3"} {
			require.Contains(t, c.descriptor.Nodes, node)
			assert.Equal(t, []string{class}, c.descriptor.Nodes[node].Classes)
		}
	})

	t.Run("classes excluded from the restore are ignored", func(t *testing.T) {
		c, req, schema := newCoord("N1", "N2", "N3")
		schema = append(schema, backup.ClassDescriptor{
			Name:          "Class-B",
			ShardingState: marshalState(t, map[string][]string{"t1": {"N1", "NX"}}),
		})
		require.NoError(t, c.expandParticipantsForDedupe(req, schema))
		require.Len(t, c.descriptor.Nodes, 3)
		assert.NotContains(t, c.descriptor.Nodes, "NX")
		for _, nd := range c.descriptor.Nodes {
			assert.NotContains(t, nd.Classes, "Class-B")
		}
	})

	t.Run("non-injective node mapping refused", func(t *testing.T) {
		c, req, schema := newCoord("N1", "N2", "N3")
		c.descriptor.NodeMapping = map[string]string{"N2": "N1", "N3": "N1"}
		err := c.expandParticipantsForDedupe(req, schema)
		require.ErrorContains(t, err, "injective node_mapping")
	})

	t.Run("unresolvable replica named with hint", func(t *testing.T) {
		c, req, schema := newCoord("N1", "N2")
		err := c.expandParticipantsForDedupe(req, schema)
		require.ErrorContains(t, err, "N3")
		require.ErrorContains(t, err, "node_mapping")
	})

	t.Run("corrupt sharding state refused", func(t *testing.T) {
		c, req, _ := newCoord("N1")
		err := c.expandParticipantsForDedupe(req, []backup.ClassDescriptor{{Name: class, ShardingState: []byte("{")}})
		require.ErrorContains(t, err, "sharding state")
	})
}

func TestResolveShardSource(t *testing.T) {
	const class = "C"
	meta := func(node string, shards ...string) sourceMeta {
		cd := backup.ClassDescriptor{Name: class}
		for _, s := range shards {
			cd.Shards = append(cd.Shards, &backup.ShardDescriptor{Name: s, Node: node})
		}
		return sourceMeta{node: node, meta: &backup.BackupDescriptor{Classes: []backup.ClassDescriptor{cd}}}
	}

	n1, n2, n3 := meta("N1", "s1", "s2"), meta("N2", "s2"), meta("N3", "s2", "s3")
	metas := []sourceMeta{n1, n2, n3}

	t.Run("own copy preferred", func(t *testing.T) {
		src, err := resolveShardSource(metas, &n1, class, "s2")
		require.NoError(t, err)
		assert.Equal(t, "N1", src)
	})

	t.Run("single foreign holder of a deduped shard", func(t *testing.T) {
		src, err := resolveShardSource(metas, &n2, class, "s1")
		require.NoError(t, err)
		assert.Equal(t, "N1", src)
	})

	t.Run("no holder means nothing to restore", func(t *testing.T) {
		src, err := resolveShardSource(metas, &n1, class, "s9")
		require.NoError(t, err)
		assert.Empty(t, src)
	})

	t.Run("multiple foreign holders means own copy was empty", func(t *testing.T) {
		empty := meta("N4")
		src, err := resolveShardSource([]sourceMeta{n1, n3, empty}, &empty, class, "s2")
		require.NoError(t, err)
		assert.Empty(t, src)
	})

	t.Run("shard claiming another node is inconsistent", func(t *testing.T) {
		bad := meta("N9", "s1")
		bad.node = "N1"
		_, err := resolveShardSource([]sourceMeta{bad}, nil, class, "s1")
		require.ErrorContains(t, err, "inconsistent backup")
	})
}

func TestFilterClassDescriptor(t *testing.T) {
	desc := &backup.ClassDescriptor{
		Name: "C",
		Shards: []*backup.ShardDescriptor{
			{Name: "s1", Node: "N1"},
			{Name: "s2", Node: "N1"},
		},
		Chunks: map[int32][]string{
			1: {"s1"},
			2: {"s2"},
			3: {"s1"},
		},
		Schema:        []byte("schema"),
		ShardingState: []byte("state"),
	}

	got := filterClassDescriptor(desc, []string{"s1"})
	require.Len(t, got.Shards, 1)
	assert.Equal(t, "s1", got.Shards[0].Name)
	assert.Equal(t, map[int32][]string{1: {"s1"}, 3: {"s1"}}, got.Chunks)
	assert.Equal(t, desc.Schema, got.Schema)
	require.Len(t, desc.Shards, 2)
	require.Len(t, desc.Chunks, 3)
}

func TestBuildFanoutPlan(t *testing.T) {
	const (
		backupID = "fanout-plan"
		class    = "Class-A"
	)
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	nodeMeta := func(node string, dedupe bool, shards ...string) []byte {
		cd := backup.ClassDescriptor{
			Name:          class,
			ShardingState: marshalState(t, map[string][]string{"s1": {"N1", "N2"}, "s2": {"N1", "N2"}}),
			Schema:        []byte("schema"),
		}
		for i, s := range shards {
			cd.Shards = append(cd.Shards, &backup.ShardDescriptor{Name: s, Node: node})
			cd.Chunks = map[int32][]string{int32(i + 1): {s}}
		}
		meta := backup.BackupDescriptor{
			ID:             backupID,
			StartedAt:      time.Now().UTC(),
			Status:         backup.Success,
			Version:        VersionDedupeReplicas,
			ServerVersion:  "1.35",
			DedupeReplicas: dedupe,
			Classes:        []backup.ClassDescriptor{cd},
		}
		if !dedupe {
			meta.Version = Version
		}
		raw, err := json.Marshal(meta)
		require.NoError(t, err)
		return raw
	}

	newRestorer := func(backend *fakeBackend) *restorer {
		provider := NewMockBackupBackendProvider(t)
		provider.EXPECT().BackupBackend("s3", mock.Anything).Return(backend, nil)
		return &restorer{node: "N2", logger: logger, backends: provider}
	}

	t.Run("fan-out target restores deduped shards from the archiving node", func(t *testing.T) {
		backend := newFakeBackend()
		backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("bucket/" + backupID)
		backend.On("GetObject", mock.Anything, backupID+"/N1", BackupFile).Return(nodeMeta("N1", true, "s1", "s2"), nil)

		r := newRestorer(backend)
		req := &Request{
			Method: OpRestore, ID: backupID, Backend: "s3", Classes: []string{class},
			DedupeReplicas: true, SourceNodes: []string{"N1"},
		}

		plan, err := r.buildFanoutPlan(ctx, "N2", req)
		require.NoError(t, err)
		require.Len(t, plan.classes, 1)
		require.Len(t, plan.classes[0].sources, 1)
		src := plan.classes[0].sources[0]
		assert.Equal(t, "N1", src.node)
		assert.Len(t, src.desc.Shards, 2)
	})

	t.Run("not-deduped source descriptor refused", func(t *testing.T) {
		backend := newFakeBackend()
		backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("bucket/" + backupID)
		backend.On("GetObject", mock.Anything, backupID+"/N1", BackupFile).Return(nodeMeta("N1", false, "s1"), nil)

		r := newRestorer(backend)
		req := &Request{
			Method: OpRestore, ID: backupID, Backend: "s3", Classes: []string{class},
			DedupeReplicas: true, SourceNodes: []string{"N1"},
		}

		_, err := r.buildFanoutPlan(ctx, "N2", req)
		require.ErrorContains(t, err, "not marked replica-deduped")
	})

	t.Run("missing source nodes refused", func(t *testing.T) {
		r := &restorer{node: "N2", logger: logger}
		_, err := r.buildFanoutPlan(ctx, "N2", &Request{DedupeReplicas: true})
		require.ErrorContains(t, err, "without source nodes")
	})
}

func TestRestoreFanoutStagesFromMultipleSources(t *testing.T) {
	e := newIncrementalTestEnv(t, config.Backup{})
	e.writeFile("s1/segment-1.db", []byte("shard-one-data"))
	e.writeFile("s2/segment-1.db", []byte("shard-two-data"))
	const backupID = "fanout-stage"

	upload := func(prefix string, sd *backup.ShardDescriptor) map[int32][]string {
		mockBackend := modulecapabilities.NewMockBackupBackend(t)
		mockBackend.EXPECT().SourceDataPath().Return(e.sourceDir)
		mockBackend.EXPECT().Write(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(e.storeWriterFn(prefix))
		u := &uploader{
			cfg:       e.cfg,
			backend:   nodeStore{objectStore{backend: mockBackend, backupId: prefix}},
			zipConfig: zipConfig{Level: int(NoCompression), GoPoolSize: 1},
			log:       logrus.New(),
		}
		var lastChunk atomic.Int32
		chunks := map[int32][]string{}
		results, err := u.processShard(context.Background(), sd, e.className, &lastChunk, "", "", u.backend.SourceDataPath())
		require.NoError(t, err)
		for _, res := range results {
			chunks[res.chunk] = res.shards
		}
		return chunks
	}

	sdA := e.makeShardDesc("s1", []string{"s1/segment-1.db"})
	sdA.Node = "nodeA"
	chunksA := upload(backupID+"/nodeA", sdA)
	sdB := e.makeShardDesc("s2", []string{"s2/segment-1.db"})
	sdB.Node = "nodeB"
	chunksB := upload(backupID+"/nodeB", sdB)
	require.Contains(t, chunksA, int32(1))
	require.Contains(t, chunksB, int32(1))

	restoreDir := t.TempDir()
	restoreMock := modulecapabilities.NewMockBackupBackend(t)
	restoreMock.EXPECT().SourceDataPath().Return(restoreDir)
	restoreMock.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(e.serveReaderFn())

	mkSource := func(node string, sd *backup.ShardDescriptor, chunks map[int32][]string) classSource {
		return classSource{
			node:  node,
			store: nodeStore{objectStore{backend: restoreMock, backupId: backupID + "/" + node, node: node}},
			desc:  &backup.ClassDescriptor{Name: e.className, Shards: []*backup.ShardDescriptor{sd}, Chunks: chunks},
		}
	}
	cp := classPlan{name: e.className, sources: []classSource{
		mkSource("nodeA", sdA, chunksA),
		mkSource("nodeB", sdB, chunksB),
	}}

	logger, _ := test.NewNullLogger()
	r := &restorer{node: "nodeB", logger: logger}
	require.NoError(t, r.restoreOneFanout(context.Background(), cp, backup.CompressionNone, 1, "", "", false))

	classTempDir := filepath.Join(restoreDir, TempDirectory, e.className)
	e.verify(classTempDir, []string{"s1/segment-1.db", "s2/segment-1.db"})
}

func TestCoordinatedRestoreFanout(t *testing.T) {
	t.Parallel()
	const (
		backendName = "s3"
		backupID    = "fanout-coord"
		class       = "Class-A"
	)
	var (
		any   = mock.Anything
		ctx   = context.Background()
		nodes = []string{"N1", "N2", "N3"}
		now   = time.Now().UTC()
		sReq  = &StatusRequest{OpRestore, backupID, backendName, "", "", ""}
		sresp = &StatusResponse{Status: backup.Success, ID: backupID, Method: OpRestore}
	)

	genDesc := func() *backup.DistributedBackupDescriptor {
		return &backup.DistributedBackupDescriptor{
			StartedAt:      now,
			ID:             backupID,
			Status:         backup.Success,
			Version:        VersionDedupeReplicas,
			ServerVersion:  "1.35",
			DedupeReplicas: true,
			Nodes: map[string]*backup.NodeDescriptor{
				"N1": {Classes: []string{class}, Status: backup.Success},
			},
		}
	}
	schema := []backup.ClassDescriptor{{
		Name:          class,
		ShardingState: marshalState(t, map[string][]string{"s1": {"N1", "N2", "N3"}}),
		Schema:        []byte("schema"),
	}}
	matchFanout := func(r *Request) bool {
		return r.Method == OpRestore && r.ID == backupID && r.DedupeReplicas &&
			assert.ObjectsAreEqual([]string{"N1"}, r.SourceNodes)
	}
	ack := &CanCommitResponse{Method: OpRestore, ID: backupID, Timeout: 1, DedupeHonored: true}

	t.Run("every replica participates and schema applies once", func(t *testing.T) {
		t.Parallel()
		fc := newFakeCoordinator(newFakeNodeResolver(nodes))
		for _, n := range nodes {
			fc.client.On("CanCommit", any, n, mock.MatchedBy(matchFanout)).Return(ack, nil)
			fc.client.On("Commit", any, n, sReq).Return(nil)
			fc.client.On("Status", any, n, sReq).Return(sresp, nil)
		}
		fc.backend.On("HomeDir", any, any, backupID).Return("bucket/" + backupID)
		fc.backend.On("GetObject", ctx, backupID, GlobalRestoreFile).Return(nil, backup.ErrNotFound{})
		fc.backend.On("PutObject", any, backupID, GlobalRestoreFile, any).Return(nil)

		coordinator := *fc.coordinator()
		store := coordStore{objectStore{fc.backend, backupID, "", "", ""}}
		req := newReq([]string{class}, backendName, backupID)
		req.Method = OpRestore

		require.NoError(t, coordinator.Restore(ctx, store, &req, genDesc(), schema, rolesAndUsersBlobs{}))
		<-fc.backend.doneChan
		assert.Equal(t, backup.Success, fc.backend.glMeta.Status)
		assert.Len(t, coordinator.descriptor.Nodes, 3)
		assert.Equal(t, int32(1), fc.schema.restoreClassCalls.Load())
	})

	t.Run("missing ack from old participant fails the restore", func(t *testing.T) {
		t.Parallel()
		fc := newFakeCoordinator(newFakeNodeResolver(nodes))
		fc.client.On("CanCommit", any, "N1", mock.MatchedBy(matchFanout)).Return(ack, nil).Maybe()
		fc.client.On("CanCommit", any, "N2", mock.MatchedBy(matchFanout)).Return(ack, nil).Maybe()
		fc.client.On("CanCommit", any, "N3", mock.MatchedBy(matchFanout)).
			Return(&CanCommitResponse{Method: OpRestore, ID: backupID, Timeout: 1}, nil)
		for _, n := range nodes {
			fc.client.On("Abort", any, n, any).Return(nil).Maybe()
		}
		fc.backend.On("HomeDir", any, any, backupID).Return("bucket/" + backupID)
		fc.backend.On("GetObject", ctx, backupID, GlobalRestoreFile).Return(nil, backup.ErrNotFound{})

		coordinator := *fc.coordinator()
		store := coordStore{objectStore{fc.backend, backupID, "", "", ""}}
		req := newReq([]string{class}, backendName, backupID)
		req.Method = OpRestore

		err := coordinator.Restore(ctx, store, &req, genDesc(), schema, rolesAndUsersBlobs{})
		require.ErrorContains(t, err, "does not support dedupeReplicas")
		assert.Equal(t, int32(0), fc.schema.restoreClassCalls.Load())
	})

	t.Run("one failed participant means schema is never applied", func(t *testing.T) {
		t.Parallel()
		fc := newFakeCoordinator(newFakeNodeResolver(nodes))
		failed := &StatusResponse{Status: backup.Failed, ID: backupID, Method: OpRestore, Err: "disk full"}
		for _, n := range nodes {
			fc.client.On("CanCommit", any, n, mock.MatchedBy(matchFanout)).Return(ack, nil)
			fc.client.On("Commit", any, n, sReq).Return(nil)
			fc.client.On("Abort", any, n, any).Return(nil).Maybe()
		}
		fc.client.On("Status", any, "N1", sReq).Return(sresp, nil)
		fc.client.On("Status", any, "N2", sReq).Return(failed, nil)
		fc.client.On("Status", any, "N3", sReq).Return(sresp, nil)
		fc.backend.On("HomeDir", any, any, backupID).Return("bucket/" + backupID)
		fc.backend.On("GetObject", ctx, backupID, GlobalRestoreFile).Return(nil, backup.ErrNotFound{})
		fc.backend.On("PutObject", any, backupID, GlobalRestoreFile, any).Return(nil)

		coordinator := *fc.coordinator()
		store := coordStore{objectStore{fc.backend, backupID, "", "", ""}}
		req := newReq([]string{class}, backendName, backupID)
		req.Method = OpRestore

		require.NoError(t, coordinator.Restore(ctx, store, &req, genDesc(), schema, rolesAndUsersBlobs{}))
		<-fc.backend.doneChan
		assert.Equal(t, backup.Failed, fc.backend.glMeta.Status)
		assert.Equal(t, int32(0), fc.schema.restoreClassCalls.Load())
	})
}

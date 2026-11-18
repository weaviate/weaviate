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

package copier_test

import (
	"bytes"
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/test/bufconn"

	grpchandlers "github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/grpc"
	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/grpc/generated/protocol"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/replication/changelog"
	"github.com/weaviate/weaviate/cluster/replication/copier"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/fakes"
	"github.com/weaviate/weaviate/usecases/file"
	"github.com/weaviate/weaviate/usecases/sharding"
)

const (
	getReplicaSnapshotFileMethod = "/clusterapi.FileReplicationService/GetReplicaSnapshotFile"
	getChangeLogMethod           = "/clusterapi.FileReplicationService/GetChangeLog"
)

// compressionRecorder captures the grpc-encoding each RPC is received and answered with, keyed by full method.
type compressionRecorder struct {
	mu  sync.Mutex
	in  map[string]string
	out map[string]string
}

type methodCtxKey struct{}

func newCompressionRecorder() *compressionRecorder {
	return &compressionRecorder{in: map[string]string{}, out: map[string]string{}}
}

func (r *compressionRecorder) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	return context.WithValue(ctx, methodCtxKey{}, info.FullMethodName)
}

func (r *compressionRecorder) HandleRPC(ctx context.Context, s stats.RPCStats) {
	method, _ := ctx.Value(methodCtxKey{}).(string)
	r.mu.Lock()
	defer r.mu.Unlock()
	switch h := s.(type) {
	case *stats.InHeader:
		r.in[method] = h.Compression
	case *stats.OutHeader:
		r.out[method] = h.Compression
	}
}

func (r *compressionRecorder) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (r *compressionRecorder) HandleConn(context.Context, stats.ConnStats) {}

func (r *compressionRecorder) get(method string) (in, out string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.in[method], r.out[method]
}

// snapshotFakeIndex serves replica snapshot files from dir and change-log tails from log.
type snapshotFakeIndex struct {
	sharding.RemoteIndexIncomingRepo
	dir   string
	files []string
	log   *changelog.ChangeLog
}

func (f *snapshotFakeIndex) IncomingCreateReplicaSnapshot(context.Context, string, string) ([]string, error) {
	return f.files, nil
}

func (f *snapshotFakeIndex) IncomingReleaseReplicaSnapshot(context.Context, string) error {
	return nil
}

func (f *snapshotFakeIndex) IncomingGetReplicaSnapshotFileMetadata(_ context.Context, _, rel string) (file.FileMetadata, error) {
	return file.GetFileMetadata(filepath.Join(f.dir, rel))
}

func (f *snapshotFakeIndex) IncomingGetReplicaSnapshotFile(_ context.Context, _, rel string) (io.ReadCloser, error) {
	return os.Open(filepath.Join(f.dir, rel))
}

func (f *snapshotFakeIndex) IncomingGetChangeLog(_ context.Context, _, _ string, untilLSN uint64) (*changelog.Tailer, error) {
	return f.log.NewTailerWithCap(0, untilLSN)
}

type snapshotFakeRepo struct {
	idx *snapshotFakeIndex
}

func (r *snapshotFakeRepo) GetIndexForIncomingSharding(context.Context, schema.ClassName) sharding.RemoteIndexIncomingRepo {
	return r.idx
}

// zeroIndexDB hands TailAndApply a non-nil index; it is never applied to because the drained log is empty.
type zeroIndexDB struct{}

func (zeroIndexDB) GetIndex(schema.ClassName) *db.Index {
	return &db.Index{}
}

func (zeroIndexDB) GetOneNodeStatus(context.Context, string, string, string, string) (*models.NodeStatus, error) {
	return nil, nil
}

type compressionFixture struct {
	recorder    *compressionRecorder
	copier      *copier.Copier
	remoteFiles map[string][]byte
	localRoot   string
}

func newCompressionFixture(t *testing.T) *compressionFixture {
	t.Helper()
	logger, _ := test.NewNullLogger()

	remoteShardDir := filepath.Join(t.TempDir(), "collection", "shard")
	remoteFiles := map[string][]byte{
		"segment.db":        bytes.Repeat([]byte("weaviate-compressible-"), 32*1024),
		"nested/commit.log": bytes.Repeat([]byte{0x42}, 200*1024),
	}
	names := make([]string, 0, len(remoteFiles))
	for rel, content := range remoteFiles {
		p := filepath.Join(remoteShardDir, rel)
		require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
		require.NoError(t, os.WriteFile(p, content, 0o644))
		names = append(names, rel)
	}

	log, err := changelog.Open(filepath.Join(t.TempDir(), "op1.log"), logger)
	require.NoError(t, err)

	idx := &snapshotFakeIndex{dir: remoteShardDir, files: names, log: log}
	svc := grpchandlers.NewFileReplicationService(&snapshotFakeRepo{idx: idx}, bufconnFakeSchema{}, 64*1024, 0)

	recorder := newCompressionRecorder()
	lis := bufconn.Listen(1 << 20)
	server := grpc.NewServer(grpc.StatsHandler(recorder))
	protocol.RegisterFileReplicationServiceServer(server, svc)
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	pbConn := protocol.NewFileReplicationServiceClient(conn)
	localRoot := t.TempDir()
	c := copier.New(
		func(context.Context, string) (copier.FileReplicationServiceClient, error) { return pbConn, nil },
		nil,
		fakes.NewFakeClusterState("source"),
		2,
		localRoot,
		zeroIndexDB{},
		"target",
		logger,
	)

	t.Cleanup(func() {
		require.NoError(t, conn.Close())
		server.Stop()
		require.NoError(t, <-serveDone)
		require.NoError(t, log.Deactivate())
	})

	return &compressionFixture{recorder: recorder, copier: c, remoteFiles: remoteFiles, localRoot: localRoot}
}

func TestCopierBulkStreamsAreGzipped(t *testing.T) {
	tests := []struct {
		name        string
		method      string
		wantOutGzip bool
		run         func(t *testing.T, fx *compressionFixture)
	}{
		{
			name:        "replica snapshot files",
			method:      getReplicaSnapshotFileMethod,
			wantOutGzip: true,
			run: func(t *testing.T, fx *compressionFixture) {
				opID := strfmt.UUID("11111111-1111-1111-1111-111111111111")
				require.NoError(t, fx.copier.CopyReplicaFiles(context.Background(), opID, "source", "collection", "shard", 0))
				for rel, want := range fx.remoteFiles {
					got, err := os.ReadFile(filepath.Join(fx.localRoot, "collection", "shard", rel))
					require.NoError(t, err)
					require.Equal(t, want, got, rel)
				}
			},
		},
		{
			name:   "change-log tail",
			method: getChangeLogMethod,
			run: func(t *testing.T, fx *compressionFixture) {
				lastApplied, err := fx.copier.TailAndApply(context.Background(), "source", "collection", "shard", "op1", 0)
				require.NoError(t, err)
				require.Zero(t, lastApplied)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fx := newCompressionFixture(t)
			tt.run(t, fx)

			in, out := fx.recorder.get(tt.method)
			require.Equal(t, "gzip", in)
			if tt.wantOutGzip {
				require.Equal(t, "gzip", out)
			}
		})
	}
}

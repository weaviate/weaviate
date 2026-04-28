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
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	grpchandlers "github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/grpc"
	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/grpc/generated/protocol"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/replication/changelog"
	"github.com/weaviate/weaviate/cluster/replication/copier"
	"github.com/weaviate/weaviate/cluster/replication/copier/internal/changelogdrain"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/fakes"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// bufconnFakeIndex implements only the 4 change-log methods; the other
// RemoteIndexIncomingRepo methods panic via the embedded nil interface, which
// is what we want — the tests must not touch them.
type bufconnFakeIndex struct {
	sharding.RemoteIndexIncomingRepo
	log *changelog.ChangeLog
}

// IncomingStartChangeCapture is a no-op; the fixture pre-opens the log so
// tests can append before tailing.
func (f *bufconnFakeIndex) IncomingStartChangeCapture(_ context.Context, _, _ string) error {
	return nil
}

func (f *bufconnFakeIndex) IncomingGetChangeLog(_ context.Context, _, _ string) (*changelog.Tailer, error) {
	return f.log.NewTailer(0)
}

func (f *bufconnFakeIndex) IncomingFinalizeChangeLog(_ context.Context, _, _ string) (uint64, error) {
	return f.log.Finalize()
}

func (f *bufconnFakeIndex) IncomingStopChangeCapture(_ context.Context, _, _ string) error {
	return f.log.Deactivate()
}

type bufconnFakeRepo struct {
	idx *bufconnFakeIndex
}

func (r *bufconnFakeRepo) GetIndexForIncomingSharding(schema.ClassName) sharding.RemoteIndexIncomingRepo {
	return r.idx
}

// bufconnFixture wires a real FileReplicationService + *changelog.ChangeLog
// over bufconn. tailAndApply bypasses Copier.TailAndApply (which would need a
// real *db.Index) and runs the drain loop directly.
type bufconnFixture struct {
	log     *changelog.ChangeLog
	copier  *copier.Copier
	pbConn  protocol.FileReplicationServiceClient
	applied []db.ChangeLogReplayEntry
}

func (fx *bufconnFixture) tailAndApply(ctx context.Context, opID string) (uint64, error) {
	stream, err := fx.pbConn.GetChangeLog(ctx, &protocol.GetChangeLogRequest{
		IndexName: "ClassOne",
		ShardName: "shard1",
		OpId:      opID,
	})
	if err != nil {
		return 0, err
	}
	return changelogdrain.Drain(ctx, stream, func(_ context.Context, batch []db.ChangeLogReplayEntry) error {
		fx.applied = append(fx.applied, batch...)
		return nil
	})
}

func newBufconnFixture(t *testing.T) *bufconnFixture {
	t.Helper()

	logger, _ := test.NewNullLogger()
	logPath := filepath.Join(t.TempDir(), "op1.log")
	log, err := changelog.Open(logPath, logger)
	require.NoError(t, err)

	fakeIdx := &bufconnFakeIndex{log: log}
	svc := grpchandlers.NewFileReplicationService(&bufconnFakeRepo{idx: fakeIdx}, nil, 64*1024)

	lis := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	protocol.RegisterFileReplicationServiceServer(server, svc)
	go func() { _ = server.Serve(lis) }()

	dialer := func(_ context.Context, _ string) (net.Conn, error) { return lis.Dial() }
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	pbConn := protocol.NewFileReplicationServiceClient(conn)
	clientFactory := func(_ context.Context, _ string) (copier.FileReplicationServiceClient, error) {
		return pbConn, nil
	}

	fx := &bufconnFixture{
		log:    log,
		pbConn: pbConn,
	}
	// nil for remoteIndex and dbWrapper — Start/Finalize/Stop don't touch them,
	// and tailAndApply bypasses Copier.TailAndApply.
	fx.copier = copier.New(
		clientFactory,
		nil,
		fakes.NewFakeClusterState("source"),
		0,
		"",
		nil,
		"",
		logger,
	)

	t.Cleanup(func() {
		server.Stop()
		conn.Close()
		lis.Close()
	})

	return fx
}

func TestChangeCapture_EndToEnd(t *testing.T) {
	fx := newBufconnFixture(t)

	// Span a batch boundary to exercise mid-stream + EOF flushes.
	const total = changelogdrain.BatchSize + 37
	for i := 1; i <= total; i++ {
		var u [16]byte
		u[0] = byte(i)
		u[1] = byte(i >> 8)
		_, err := fx.log.AppendPut(u, int64(1_000_000+i), []byte("payload"))
		require.NoError(t, err)
	}

	err := fx.copier.StartChangeCapture(context.Background(), "source", "ClassOne", "shard1", "op1")
	require.NoError(t, err)

	drainDone := make(chan drainResult, 1)
	go func() {
		lastLSN, err := fx.tailAndApply(context.Background(), "op1")
		drainDone <- drainResult{lastLSN: lastLSN, err: err}
	}()

	// Let the drain goroutine reach its blocked Recv before Finalize — turns
	// the assertion below into a real wait-then-resume, not a race.
	time.Sleep(20 * time.Millisecond)

	finalLSN, err := fx.copier.FinalizeChangeLog(context.Background(), "source", "ClassOne", "shard1", "op1")
	require.NoError(t, err)
	require.Equal(t, uint64(total), finalLSN)

	select {
	case res := <-drainDone:
		require.NoError(t, res.err)
		require.Equal(t, finalLSN, res.lastLSN)
	case <-time.After(5 * time.Second):
		t.Fatal("TailAndApply did not return after Finalize; possible stream leak")
	}

	require.Equal(t, total, len(fx.applied))
	for i, entry := range fx.applied {
		require.Equal(t, int64(1_000_000+int64(i+1)), entry.LastUpdateTimeUnixMilli,
			"entry %d out of LSN order", i)
	}

	require.NoError(t, fx.copier.StopChangeCapture(context.Background(), "source", "ClassOne", "shard1", "op1"))
}

// TestChangeCapture_CancelMidTail guards against a handler leaking the tailer
// when the client cancels mid-stream with the log still active.
func TestChangeCapture_CancelMidTail(t *testing.T) {
	fx := newBufconnFixture(t)

	// Sub-batch-size: entries reach the client but never trigger a flush.
	for i := 1; i <= 3; i++ {
		var u [16]byte
		u[0] = byte(i)
		_, err := fx.log.AppendPut(u, int64(i), []byte("x"))
		require.NoError(t, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	drainDone := make(chan drainResult, 1)
	go func() {
		lastLSN, err := fx.tailAndApply(ctx, "op1")
		drainDone <- drainResult{lastLSN: lastLSN, err: err}
	}()

	// Drive the tailer into its Next-blocked state before cancelling.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case res := <-drainDone:
		require.Error(t, res.err)
	case <-time.After(2 * time.Second):
		t.Fatal("TailAndApply did not return after cancel; handler leaked")
	}

	// Opening a fresh tailer proves the server unwound cleanly.
	tailer, err := fx.log.NewTailer(0)
	require.NoError(t, err)
	require.NoError(t, tailer.Close())

	require.NoError(t, fx.copier.StopChangeCapture(context.Background(), "source", "ClassOne", "shard1", "op1"))
}

type drainResult struct {
	lastLSN uint64
	err     error
}

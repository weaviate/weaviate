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
	"math"
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
	"github.com/weaviate/weaviate/entities/models"
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

func (f *bufconnFakeIndex) IncomingGetChangeLog(_ context.Context, _, _ string, untilLSN uint64) (*changelog.Tailer, error) {
	return f.log.NewTailerWithCap(0, untilLSN)
}

func (f *bufconnFakeIndex) IncomingSnapshotChangeLogLSN(_ context.Context, _, _ string) (uint64, error) {
	return f.log.LSN(), nil
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

// bufconnFakeSchema satisfies the StartChangeCapture schema-version barrier;
// these tests pass schemaVersion 0, so the barrier is always a no-op.
type bufconnFakeSchema struct{}

func (bufconnFakeSchema) ReadOnlyClassWithVersion(context.Context, string, uint64) (*models.Class, error) {
	return nil, nil
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

func (fx *bufconnFixture) tailAndApply(ctx context.Context, opID string, untilLSN uint64) (uint64, error) {
	stream, err := fx.pbConn.GetChangeLog(ctx, &protocol.GetChangeLogRequest{
		IndexName: "ClassOne",
		ShardName: "shard1",
		OpId:      opID,
		UntilLsn:  untilLSN,
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
	svc := grpchandlers.NewFileReplicationService(&bufconnFakeRepo{idx: fakeIdx}, bufconnFakeSchema{}, 64*1024)

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

// End-to-end exercise of the single-log movement shape over the wire:
// snapshot → cap'd drain → more writes → Finalize → un-cap'd drain. Catches
// regressions where the cap'd drain blocks for Finalize, where
// SnapshotChangeLogLSN seals the log, or where the second tailer fails to
// resume past the cap.
func TestChangeCapture_SingleLogMovementFlow(t *testing.T) {
	fx := newBufconnFixture(t)

	// Span a batch boundary so the cap'd drain exercises both mid-stream and
	// EOF flushes inside changelogdrain.
	const phase1 = changelogdrain.BatchSize + 7
	for i := 1; i <= phase1; i++ {
		var u [16]byte
		u[0] = byte(i)
		u[1] = byte(i >> 8)
		_, err := fx.log.AppendPut(u, int64(1_000_000+i), []byte("payload"))
		require.NoError(t, err)
	}

	require.NoError(t, fx.copier.StartChangeCapture(context.Background(),
		"source", "ClassOne", "shard1", "op1", 0))

	snap, err := fx.copier.SnapshotChangeLogLSN(context.Background(),
		"source", "ClassOne", "shard1", "op1")
	require.NoError(t, err)
	require.Equal(t, uint64(phase1), snap)

	lastApplied, err := fx.tailAndApply(context.Background(), "op1", snap)
	require.NoError(t, err)
	require.Equal(t, snap, lastApplied)
	require.Equal(t, phase1, len(fx.applied))

	// Post-snapshot writes must keep landing in the still-writable log.
	const phase2 = 30
	for i := phase1 + 1; i <= phase1+phase2; i++ {
		var u [16]byte
		u[0] = byte(i)
		u[1] = byte(i >> 8)
		_, err := fx.log.AppendPut(u, int64(1_000_000+i), []byte("payload"))
		require.NoError(t, err)
	}

	finalLSN, err := fx.copier.FinalizeChangeLog(context.Background(),
		"source", "ClassOne", "shard1", "op1")
	require.NoError(t, err)
	require.Equal(t, uint64(phase1+phase2), finalLSN)

	// Drain through finalLSN. The tailer opens a fresh stream from LSN 0, so
	// phase-1 entries are re-applied; LWW handles this in production.
	lastApplied, err = fx.tailAndApply(context.Background(), "op1", finalLSN)
	require.NoError(t, err)
	require.Equal(t, finalLSN, lastApplied)

	require.Equal(t, phase1+int(finalLSN), len(fx.applied))
	for i, entry := range fx.applied[:phase1] {
		require.Equal(t, int64(1_000_000+int64(i+1)), entry.LastUpdateTimeUnixMilli,
			"phase1[%d] out of LSN order", i)
	}
	for i, entry := range fx.applied[phase1:] {
		require.Equal(t, int64(1_000_000+int64(i+1)), entry.LastUpdateTimeUnixMilli,
			"phase2 stream out of order at %d", i)
	}

	require.NoError(t, fx.copier.StopChangeCapture(context.Background(),
		"source", "ClassOne", "shard1", "op1"))
}

// A cap'd drain on a wide-open log must EOF at the cap rather than block
// waiting for Finalize.
func TestChangeCapture_CappedDrainStopsAtCap(t *testing.T) {
	fx := newBufconnFixture(t)

	const total = 5
	for i := 1; i <= total; i++ {
		var u [16]byte
		u[0] = byte(i)
		_, err := fx.log.AppendPut(u, int64(i), []byte("x"))
		require.NoError(t, err)
	}

	require.NoError(t, fx.copier.StartChangeCapture(context.Background(),
		"source", "ClassOne", "shard1", "op1", 0))

	const cap = 3
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	lastApplied, err := fx.tailAndApply(ctx, "op1", cap)
	require.NoError(t, err)
	require.Equal(t, uint64(cap), lastApplied)
	require.Equal(t, cap, len(fx.applied))

	// Cap must not have sealed: another append after the drain still works.
	var u [16]byte
	u[0] = byte(total + 1)
	_, err = fx.log.AppendPut(u, int64(total+1), []byte("after-cap"))
	require.NoError(t, err)

	require.NoError(t, fx.copier.StopChangeCapture(context.Background(),
		"source", "ClassOne", "shard1", "op1"))
}

// On a quiet shard (no writes between StartChangeCapture and the FINALIZING
// boundary) Snapshot returns 0, and the consumer's cap'd drain must complete
// rather than hang. Regression guard for the untilLSN=0 semantics.
func TestChangeCapture_QuietShardSnapshotZeroCompletes(t *testing.T) {
	fx := newBufconnFixture(t)

	require.NoError(t, fx.copier.StartChangeCapture(context.Background(),
		"source", "ClassOne", "shard1", "op1", 0))

	snap, err := fx.copier.SnapshotChangeLogLSN(context.Background(),
		"source", "ClassOne", "shard1", "op1")
	require.NoError(t, err)
	require.Equal(t, uint64(0), snap)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	lastApplied, err := fx.tailAndApply(ctx, "op1", snap)
	require.NoError(t, err)
	require.Equal(t, uint64(0), lastApplied)
	require.Empty(t, fx.applied)

	require.NoError(t, fx.copier.StopChangeCapture(context.Background(),
		"source", "ClassOne", "shard1", "op1"))
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
		// Effectively-unbounded cap so the tailer drains the 3 entries and
		// then blocks on the next Recv, ready for cancel to unblock it.
		lastLSN, err := fx.tailAndApply(ctx, "op1", math.MaxUint64)
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

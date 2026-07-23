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

package db

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	resolver "github.com/weaviate/weaviate/adapters/repos/db/sharding"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/multi"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/objects"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// refCountTestIndex returns a single-shard index wired with a real shard
// resolver and remote index, plus its only shard. The node resolver never
// resolves a host, so remote forwards fail without touching the network.
func refCountTestIndex(t *testing.T, className string) (*Index, *Shard) {
	t.Helper()

	nodeResolver := cluster.NewMockNodeResolver(t)
	nodeResolver.EXPECT().NodeHostname(mock.Anything).Return("", false).Maybe()

	shard, idx := testShard(t, t.Context(), className, func(i *Index) {
		i.shardResolver = resolver.NewShardResolver(className, false, i.getSchema)
		i.remote = sharding.NewRemoteIndex(className, i.getSchema,
			nodeResolver, &FakeRemoteClient{})
	})

	return idx, underlyingShard(t, shard)
}

// releaseMisuseHook captures what the index logs, so a test can assert on the
// release misuse reported by preventShutdown.
func releaseMisuseHook(t *testing.T, idx *Index) *logrustest.Hook {
	t.Helper()

	logger, ok := idx.logger.(*logrus.Logger)
	require.True(t, ok, "the test index must carry a concrete logger to hook")
	return logrustest.NewLocal(logger)
}

// releaseMisuse returns the release misuse captured so far. The logger is shared
// with the index's background work, so unrelated entries are ignored.
func releaseMisuse(hook *logrustest.Hook) []*logrus.Entry {
	var out []*logrus.Entry
	for _, entry := range hook.AllEntries() {
		if strings.Contains(entry.Message, msgReleasedMoreThanOnce) {
			out = append(out, entry)
		}
	}
	return out
}

// forwardToRemote points the router at a peer node so the local shard lookup
// yields a nil shard and the operation is forwarded instead.
func forwardToRemote(t *testing.T, idx *Index, className, shardName string) {
	t.Helper()

	router := types.NewMockRouter(t)
	router.EXPECT().GetWriteReplicasLocation(className, mock.Anything, mock.Anything).Return(
		types.WriteReplicaSet{
			Replicas: []types.Replica{{NodeName: "node2", ShardName: shardName, HostAddr: "127.0.0.2"}},
		}, nil,
	).Maybe()
	router.EXPECT().GetReadReplicasLocation(className, mock.Anything, mock.Anything).Return(
		types.ReadReplicaSet{
			Replicas: []types.Replica{{NodeName: "node2", ShardName: shardName, HostAddr: "127.0.0.2"}},
		}, nil,
	).Maybe()
	idx.router = router
}

// batchDeleteErr collapses the per-object errors of a delete batch, which the
// call reports inside the result rather than as its own error.
func batchDeleteErr(objs objects.BatchSimpleObjects, err error) error {
	if err != nil {
		return err
	}
	errs := make([]error, 0, len(objs))
	for _, obj := range objs {
		errs = append(errs, obj.Err)
	}
	return errors.Join(errs...)
}

// TestShardRefCountArity asserts that every data-path operation releases the
// shard exactly as often as it acquired it, on both the local and the
// forwarded-to-peer branch. A positive counter permanently blocks unloading. An
// extra release is absorbed by preventShutdown and so leaves the counter at
// zero; only the misuse it reports shows that a call site released twice.
func TestShardRefCountArity(t *testing.T) {
	className := "RefCountArity"

	tests := []struct {
		name   string
		remote bool
		// wantErr is set where the exercised branch cannot succeed, which also
		// pins that the operation took the intended branch.
		wantErr bool
		run     func(t *testing.T, idx *Index, shardName string) error
	}{
		{
			name: "putObjectBatch local",
			run: func(t *testing.T, idx *Index, shardName string) error {
				return errors.Join(idx.putObjectBatch(t.Context(),
					[]*storobj.Object{testObject(className)}, nil, 0)...)
			},
		},
		{
			name: "putObjectBatch forwarded", remote: true, wantErr: true,
			run: func(t *testing.T, idx *Index, shardName string) error {
				return errors.Join(idx.putObjectBatch(t.Context(),
					[]*storobj.Object{testObject(className)}, nil, 0)...)
			},
		},
		{
			name: "batchDeleteObjects local",
			run: func(t *testing.T, idx *Index, shardName string) error {
				return batchDeleteErr(idx.batchDeleteObjects(t.Context(),
					map[string][]strfmt.UUID{shardName: {strfmt.UUID(uuid.NewString())}},
					time.Now(), false, nil, 0, ""))
			},
		},
		{
			name: "batchDeleteObjects forwarded", remote: true, wantErr: true,
			run: func(t *testing.T, idx *Index, shardName string) error {
				return batchDeleteErr(idx.batchDeleteObjects(t.Context(),
					map[string][]strfmt.UUID{shardName: {strfmt.UUID(uuid.NewString())}},
					time.Now(), false, nil, 0, ""))
			},
		},
		{
			name: "AddReferencesBatch forwarded", remote: true, wantErr: true,
			run: func(t *testing.T, idx *Index, shardName string) error {
				return errors.Join(idx.AddReferencesBatch(t.Context(), objects.BatchReferences{{
					From: &crossref.RefSource{TargetID: strfmt.UUID(uuid.NewString())},
					To:   &crossref.Ref{TargetID: strfmt.UUID(uuid.NewString())},
				}}, nil, 0)...)
			},
		},
		{
			name: "multiObjectByID local",
			run: func(t *testing.T, idx *Index, shardName string) error {
				_, err := idx.multiObjectByID(t.Context(),
					[]multi.Identifier{{ID: uuid.NewString(), ClassName: className}}, "")
				return err
			},
		},
		{
			name: "multiObjectByID forwarded", remote: true, wantErr: true,
			run: func(t *testing.T, idx *Index, shardName string) error {
				_, err := idx.multiObjectByID(t.Context(),
					[]multi.Identifier{{ID: uuid.NewString(), ClassName: className}}, "")
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			idx, shard := refCountTestIndex(t, className)
			hook := releaseMisuseHook(t, idx)
			if test.remote {
				forwardToRemote(t, idx, className, shard.name)
			}

			for i := 0; i < 3; i++ {
				err := test.run(t, idx, shard.name)
				if test.wantErr {
					require.Error(t, err, "the exercised branch must be the one under test")
				} else {
					require.NoError(t, err)
				}
				require.Equalf(t, int64(0), shard.inUseCounter.Load(),
					"after %d operation(s) every acquire must have exactly one release", i+1)
				require.Emptyf(t, releaseMisuse(hook),
					"after %d operation(s) no call site may release twice", i+1)
			}
		})
	}
}

// TestShardRefCountSchemaWaitFailure covers the write paths that wait for the
// schema version themselves and then again inside
// getShardForDirectLocalOperation, which by then already holds a reference. If
// the context is cancelled between the two waits the second one fails, and that
// reference still has to be released.
func TestShardRefCountSchemaWaitFailure(t *testing.T) {
	className := "RefCountSchemaWait"
	const schemaVersion = uint64(7)

	tests := []struct {
		name string
		run  func(t *testing.T, idx *Index, id strfmt.UUID) error
	}{
		{
			name: "deleteObject",
			run: func(t *testing.T, idx *Index, id strfmt.UUID) error {
				return idx.deleteObject(t.Context(), id, time.Now(), nil, "", schemaVersion)
			},
		},
		{
			name: "mergeObject",
			run: func(t *testing.T, idx *Index, id strfmt.UUID) error {
				return idx.mergeObject(t.Context(),
					objects.MergeDocument{Class: className, ID: id}, nil, "", schemaVersion)
			},
		},
		{
			name: "AddReferencesBatch",
			run: func(t *testing.T, idx *Index, id strfmt.UUID) error {
				return errors.Join(idx.AddReferencesBatch(t.Context(), objects.BatchReferences{{
					From: &crossref.RefSource{TargetID: id},
					To:   &crossref.Ref{TargetID: strfmt.UUID(uuid.NewString())},
				}}, nil, schemaVersion)...)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			idx, shard := refCountTestIndex(t, className)

			// the caller's own wait succeeds, the one inside the shard lookup does not
			schemaReader := idx.schemaReader.(*schemaUC.MockSchemaReader)
			schemaReader.EXPECT().WaitForUpdate(mock.Anything, schemaVersion).Return(nil).Once()
			schemaReader.EXPECT().WaitForUpdate(mock.Anything, schemaVersion).
				Return(context.Canceled).Once()

			require.Error(t, test.run(t, idx, strfmt.UUID(uuid.NewString())))
			require.Equal(t, int64(0), shard.inUseCounter.Load(),
				"a failed schema wait must still release the shard")
		})
	}
}

// TestWithShardOrRemoteRunsOneArm asserts that withShardOrRemote runs the local
// arm only when it has a usable local shard, the remote arm whenever it has not,
// and neither when the lookup fails — releasing the reference and passing the
// arm's error on in every case. A local arm that ran with no shard would touch a
// nil shard; a skipped remote arm would silently drop the operation.
func TestWithShardOrRemoteRunsOneArm(t *testing.T) {
	className := "WithShardOrRemote"
	const schemaVersion = uint64(7)

	errArm := errors.New("arm failed")

	tests := []struct {
		name string
		// forwarded points the router at a peer, so there is no usable local shard
		forwarded bool
		// failSchemaWait makes the lookup fail before it can pick a branch
		failSchemaWait bool
		// failArm makes the branch that runs return errArm
		failArm    bool
		operation  localShardOperation
		wantLocal  bool
		wantRemote bool
	}{
		{name: "local read", operation: localShardOperationRead, wantLocal: true},
		{name: "local write", operation: localShardOperationWrite, wantLocal: true},
		{name: "forwarded read", forwarded: true, operation: localShardOperationRead, wantRemote: true},
		{name: "forwarded write", forwarded: true, operation: localShardOperationWrite, wantRemote: true},
		{name: "failed lookup", failSchemaWait: true, operation: localShardOperationWrite},
		{name: "failing local arm", failArm: true, operation: localShardOperationWrite, wantLocal: true},
		{
			name: "failing remote arm", forwarded: true, failArm: true,
			operation: localShardOperationWrite, wantRemote: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			idx, shard := refCountTestIndex(t, className)
			if test.forwarded {
				forwardToRemote(t, idx, className, shard.name)
			}

			var version uint64
			if test.failSchemaWait {
				version = schemaVersion
				schemaReader := idx.schemaReader.(*schemaUC.MockSchemaReader)
				schemaReader.EXPECT().WaitForUpdate(mock.Anything, schemaVersion).
					Return(context.Canceled).Once()
			}

			armErr := error(nil)
			if test.failArm {
				armErr = errArm
			}

			var ranLocal, ranRemote bool
			err := idx.withShardOrRemote(t.Context(), "", shard.name, test.operation, version,
				func(got ShardLike) error {
					ranLocal = true
					require.NotNil(t, got, "the local arm must never be handed a nil shard")
					return armErr
				},
				func() error {
					ranRemote = true
					return armErr
				})

			switch {
			case test.failArm:
				require.ErrorIs(t, err, errArm, "the arm's error must reach the caller")
			case test.failSchemaWait:
				require.Error(t, err)
			default:
				require.NoError(t, err)
			}
			require.Equal(t, test.wantLocal, ranLocal, "local arm")
			require.Equal(t, test.wantRemote, ranRemote, "remote arm")
			require.Equal(t, int64(0), shard.inUseCounter.Load(),
				"every branch must release the shard reference")
		})
	}
}

// TestShardLookupReleasesReplacedReference asserts that getShardForWrite and
// getShardForRead release the reference they were handed whenever they hand back
// a different one. The caller only defers the returned release, so a dropped one
// keeps the counter above zero and blocks unloading for good.
func TestShardLookupReleasesReplacedReference(t *testing.T) {
	className := "RefCountReplacedReference"

	tests := []struct {
		name string
		// blockInit exercises the branch where the shard cannot be initialized,
		// which still has to release the reference it was handed
		blockInit bool
		run       func(ctx context.Context, idx *Index, shardName string, release func()) (ShardLike, func(), error)
	}{
		{
			name: "write",
			run: func(ctx context.Context, idx *Index, shardName string, release func()) (ShardLike, func(), error) {
				return idx.getShardForWrite(ctx, className, "", shardName, nil, release)
			},
		},
		{
			name: "read",
			run: func(ctx context.Context, idx *Index, shardName string, release func()) (ShardLike, func(), error) {
				return idx.getShardForRead(ctx, className, "", shardName, nil, release)
			},
		},
		{
			name: "write with failing init", blockInit: true,
			run: func(ctx context.Context, idx *Index, shardName string, release func()) (ShardLike, func(), error) {
				return idx.getShardForWrite(ctx, className, "", shardName, nil, release)
			},
		},
		{
			name: "read with failing init", blockInit: true,
			run: func(ctx context.Context, idx *Index, shardName string, release func()) (ShardLike, func(), error) {
				return idx.getShardForRead(ctx, className, "", shardName, nil, release)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			idx, shard := refCountTestIndex(t, className)
			hook := releaseMisuseHook(t, idx)

			// the reference handed over along with the nil shard that makes the
			// lookup initialize one
			release, err := shard.preventShutdown()
			require.NoError(t, err)
			require.Equal(t, int64(1), shard.inUseCounter.Load())

			wantInUse := int64(1)
			if test.blockInit {
				idx.shards.LoadAndDelete(shard.name)
				idx.backupProtectedShards.Store(shard.name, struct{}{})
				wantInUse = 0
			}

			got, gotRelease, err := test.run(t.Context(), idx, shard.name, release)
			require.NotNil(t, gotRelease, "the returned release is what the caller defers")
			if test.blockInit {
				require.Error(t, err)
				require.Nil(t, got)
			} else {
				require.NoError(t, err)
				require.NotNil(t, got, "the shard must be initialized")
			}
			require.Equal(t, wantInUse, shard.inUseCounter.Load(),
				"the reference that is not handed back must be released")

			gotRelease()
			require.Equal(t, int64(0), shard.inUseCounter.Load())
			require.Empty(t, releaseMisuse(hook), "no reference may be released twice")
		})
	}
}

// TestShardShutdownRefusedWhileInUse asserts that the in-use guard still holds
// after data-path traffic. A counter driven negative by an over-release lets
// performShutdown tear the store down under an active reader.
func TestShardShutdownRefusedWhileInUse(t *testing.T) {
	className := "RefCountGuard"

	tests := []struct {
		name string
		run  func(t *testing.T, idx *Index, shardName string)
	}{
		{
			name: "after putObjectBatch",
			run: func(t *testing.T, idx *Index, shardName string) {
				idx.putObjectBatch(t.Context(), []*storobj.Object{testObject(className)}, nil, 0)
			},
		},
		{
			name: "after batchDeleteObjects",
			run: func(t *testing.T, idx *Index, shardName string) {
				_, err := idx.batchDeleteObjects(t.Context(),
					map[string][]strfmt.UUID{shardName: {strfmt.UUID(uuid.NewString())}},
					time.Now(), false, nil, 0, "")
				require.NoError(t, err)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			idx, shard := refCountTestIndex(t, className)

			test.run(t, idx, shard.name)

			_, release, err := idx.GetShard(t.Context(), shard.name)
			require.NoError(t, err)
			defer release()

			require.Error(t, shard.performShutdown(t.Context()),
				"shutdown must be refused while a reference is held")
		})
	}
}

// hnswVectorIndex returns a real hnsw index, which is what DebugResetVectorIndex
// requires before it starts its background work.
func hnswVectorIndex(t *testing.T) VectorIndex {
	t.Helper()

	shard, _ := testShardWithSettings(t, t.Context(), &models.Class{Class: "RefCountDebugHNSW"},
		enthnsw.NewDefaultUserConfig(), false, false, false)
	vidx, ok := underlyingShard(t, shard).GetVectorIndex("")
	require.True(t, ok, "the test shard must carry a vector index")
	return vidx
}

const debugShardName = "debug-shard"

// debugBackgroundEndpoint is a debug endpoint that hands work to a background
// goroutine.
type debugBackgroundEndpoint struct {
	name string
	// expectBeforeWork registers the shard calls the endpoint makes before it
	// takes the reference for its background work.
	expectBeforeWork func(t *testing.T, shard *MockShardLike)
	// expectWork registers the background work and everything the endpoint does
	// once it holds that reference. The work closes started, blocks until finish
	// is closed and then returns workErr.
	expectWork func(shard *MockShardLike, started, finish chan struct{}, workErr error)
	run        func(idx *Index) error
}

func debugBackgroundEndpoints() []debugBackgroundEndpoint {
	return []debugBackgroundEndpoint{
		{
			name: "DebugResetVectorIndex",
			expectBeforeWork: func(t *testing.T, shard *MockShardLike) {
				shard.EXPECT().GetVectorIndex("").Return(hnswVectorIndex(t), true)
			},
			expectWork: func(shard *MockShardLike, started, finish chan struct{}, workErr error) {
				shard.EXPECT().DebugResetVectorIndex(mock.Anything, "").Return(nil)
				shard.EXPECT().FillQueue("", uint64(0)).RunAndReturn(func(string, uint64) error {
					close(started)
					<-finish
					return workErr
				})
			},
			run: func(idx *Index) error {
				return idx.DebugResetVectorIndex(context.Background(), debugShardName, "")
			},
		},
		{
			name:             "DebugRepairIndex",
			expectBeforeWork: func(t *testing.T, shard *MockShardLike) {},
			expectWork: func(shard *MockShardLike, started, finish chan struct{}, workErr error) {
				shard.EXPECT().RepairIndex(mock.Anything, "").RunAndReturn(func(context.Context, string) error {
					close(started)
					<-finish
					return workErr
				})
			},
			run: func(idx *Index) error {
				return idx.DebugRepairIndex(context.Background(), debugShardName, "")
			},
		},
		{
			name:             "DebugRequantizeIndex",
			expectBeforeWork: func(t *testing.T, shard *MockShardLike) {},
			expectWork: func(shard *MockShardLike, started, finish chan struct{}, workErr error) {
				shard.EXPECT().RequantizeIndex(mock.Anything, "").RunAndReturn(func(context.Context, string) error {
					close(started)
					<-finish
					return workErr
				})
			},
			run: func(idx *Index) error {
				return idx.DebugRequantizeIndex(context.Background(), debugShardName, "")
			},
		},
	}
}

// TestDebugEndpointsHoldShardWhileBackgroundWorkRuns asserts that the debug
// endpoints handing work to a background goroutine keep the shard referenced
// until that work returns, whether it succeeds or fails. Releasing on return
// lets the shard shut down while the work is still using it.
func TestDebugEndpointsHoldShardWhileBackgroundWorkRuns(t *testing.T) {
	workResults := []struct {
		name    string
		workErr error
	}{
		{name: "work succeeds"},
		{name: "work fails", workErr: errors.New("background work failed")},
	}

	for _, endpoint := range debugBackgroundEndpoints() {
		for _, result := range workResults {
			t.Run(endpoint.name+"/"+result.name, func(t *testing.T) {
				idx := newDescriptorTestIndex(t, t.TempDir(), "RefCountDebug", singleShardState())

				var inUse atomic.Int64
				shard := NewMockShardLike(t)
				shard.EXPECT().Name().Return(debugShardName).Maybe()
				shard.EXPECT().preventShutdown().RunAndReturn(func() (func(), error) {
					inUse.Add(1)
					return func() { inUse.Add(-1) }, nil
				})
				idx.shards.Store(debugShardName, shard)

				started, finish := make(chan struct{}), make(chan struct{})
				endpoint.expectBeforeWork(t, shard)
				endpoint.expectWork(shard, started, finish, result.workErr)

				require.NoError(t, endpoint.run(idx))

				select {
				case <-started:
				case <-time.After(5 * time.Second):
					t.Fatal("the background work never started")
				}
				require.Equal(t, int64(1), inUse.Load(),
					"the running background work must be the only reference left")

				close(finish)
				require.Eventually(t, func() bool { return inUse.Load() == 0 }, 5*time.Second, 10*time.Millisecond,
					"the background work must release its reference when it returns")
			})
		}
	}
}

// TestDebugEndpointsFailWhileShardShutsDown asserts that an endpoint reports the
// failure and does nothing once the shard can no longer be referenced. Starting
// the work anyway runs it on a shard that is being torn down, and resetting a
// vector index without a reindex to follow leaves it empty.
func TestDebugEndpointsFailWhileShardShutsDown(t *testing.T) {
	for _, endpoint := range debugBackgroundEndpoints() {
		t.Run(endpoint.name, func(t *testing.T) {
			idx := newDescriptorTestIndex(t, t.TempDir(), "RefCountDebugShutdown", singleShardState())

			var released atomic.Bool
			shard := NewMockShardLike(t)
			// the lookup still gets a reference, the background work no longer does
			shard.EXPECT().preventShutdown().Return(func() { released.Store(true) }, nil).Once()
			shard.EXPECT().preventShutdown().Return(func() {}, errShutdownInProgress).Once()
			idx.shards.Store(debugShardName, shard)

			// expectWork is left unregistered, so the mock fails the test if the
			// endpoint calls any of it
			endpoint.expectBeforeWork(t, shard)

			require.ErrorIs(t, endpoint.run(idx), errShutdownInProgress)
			require.True(t, released.Load(), "the lookup's reference must be released")
		})
	}
}

// TestPreventShutdownReleaseIsIdempotent asserts that a caller releasing more
// than once per acquire cannot drive the counter negative, and that the misuse
// is reported rather than silently absorbed.
func TestPreventShutdownReleaseIsIdempotent(t *testing.T) {
	idx, shard := refCountTestIndex(t, "RefCountIdempotent")
	hook := releaseMisuseHook(t, idx)

	release, err := shard.preventShutdown()
	require.NoError(t, err)
	require.Equal(t, int64(1), shard.inUseCounter.Load())

	release()
	require.Empty(t, releaseMisuse(hook), "one release per acquire is not a misuse")

	release()
	release()

	require.Equal(t, int64(0), shard.inUseCounter.Load())
	entries := releaseMisuse(hook)
	require.Len(t, entries, 2, "each extra release must be reported")
	for _, entry := range entries {
		require.Equal(t, logrus.ErrorLevel, entry.Level)
	}
}

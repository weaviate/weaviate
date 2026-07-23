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
	"github.com/weaviate/weaviate/entities/multi"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/entities/storobj"
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

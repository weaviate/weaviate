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
	"encoding/binary"
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

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	resolver "github.com/weaviate/weaviate/adapters/repos/db/sharding"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/additional"
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

// refCountTestIndex returns a single-shard index plus its only shard. The shard
// carries a real vector index so the search paths can run, and the node resolver
// never resolves a host, so remote forwards fail without touching the network.
func refCountTestIndex(t *testing.T, className string) (*Index, *Shard) {
	t.Helper()

	nodeResolver := cluster.NewMockNodeResolver(t)
	nodeResolver.EXPECT().NodeHostname(mock.Anything).Return("", false).Maybe()

	shard, idx := testShardWithSettings(t, t.Context(), &models.Class{Class: className},
		enthnsw.NewDefaultUserConfig(), false, false, false, func(i *Index) {
			i.shardResolver = resolver.NewShardResolver(className, false, i.getSchema)
			i.remote = sharding.NewRemoteIndex(className, i.getSchema,
				nodeResolver, &FakeRemoteClient{})
		})

	localShard := underlyingShard(t, shard)
	router, ok := idx.router.(*types.MockRouter)
	require.True(t, ok, "the test index must carry a mock router to extend")
	expectReadRoutingPlan(router, localShard.name, "node1", "127.0.0.1")

	return idx, localShard
}

// expectReadRoutingPlan answers the routing plan the search paths build before
// they look a shard up, routing every read to the given node's replica.
func expectReadRoutingPlan(router *types.MockRouter, shardName, nodeName, hostAddr string) {
	router.EXPECT().BuildReadRoutingPlan(mock.Anything).Return(
		types.ReadRoutingPlan{
			ReplicaSet: types.ReadReplicaSet{
				Replicas: []types.Replica{{NodeName: nodeName, ShardName: shardName, HostAddr: hostAddr}},
			},
		}, nil,
	).Maybe()
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
	expectReadRoutingPlan(router, shardName, "node2", "127.0.0.2")
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

// putTestObject stores an object under a fixed id, so a later step can read it
// back or corrupt it.
func putTestObject(t *testing.T, shard *Shard, className, id string) *storobj.Object {
	t.Helper()

	obj := testObject(className)
	obj.Object.ID = strfmt.UUID(id)
	require.NoError(t, shard.PutObject(t.Context(), obj))
	return obj
}

// corruptStoredObject replaces an object's stored value with one that cannot be
// decoded, so reading it back fails.
func corruptStoredObject(t *testing.T, shard *Shard, obj *storobj.Object) {
	t.Helper()

	key, err := uuid.MustParse(obj.ID().String()).MarshalBinary()
	require.NoError(t, err)
	docID := make([]byte, 8)
	binary.LittleEndian.PutUint64(docID, obj.DocID)
	// marshaller version 2 does not exist, so decoding fails
	require.NoError(t, shard.Store().Bucket(helpers.ObjectsBucketLSM).
		Put(key, []byte{2}, lsmkv.WithSecondaryKey(
			helpers.ObjectsBucketLSMDocIDSecondaryIndex, docID)))
}

// TestShardRefCountArity asserts that every data-path operation releases the
// shard exactly as often as it acquired it, locally and forwarded to a peer. A
// positive counter blocks unloading; an extra release shows up as logged misuse.
func TestShardRefCountArity(t *testing.T) {
	className := "RefCountArity"

	const (
		blockedShard        = "blocked-shard"
		readableObjectID    = "11111111-1111-1111-1111-111111111111"
		undecodableObjectID = "22222222-2222-2222-2222-222222222222"
	)

	tests := []struct {
		name   string
		remote bool
		// wantErr is set where the exercised branch cannot succeed, which also
		// pins that the operation took the intended branch.
		wantErr bool
		// wantErrContains is set where both branches fail, so only the error text
		// tells them apart.
		wantErrContains string
		// setup prepares the index or the shard before the operation runs.
		setup func(t *testing.T, idx *Index, shard *Shard)
		run   func(t *testing.T, idx *Index, shard *Shard) error
	}{
		{
			name: "putObjectBatch local",
			run: func(t *testing.T, idx *Index, shard *Shard) error {
				return errors.Join(idx.putObjectBatch(t.Context(),
					[]*storobj.Object{testObject(className)}, nil, 0)...)
			},
		},
		{
			name: "putObjectBatch forwarded", remote: true, wantErr: true,
			run: func(t *testing.T, idx *Index, shard *Shard) error {
				return errors.Join(idx.putObjectBatch(t.Context(),
					[]*storobj.Object{testObject(className)}, nil, 0)...)
			},
		},
		{
			name: "batchDeleteObjects local",
			run: func(t *testing.T, idx *Index, shard *Shard) error {
				return batchDeleteErr(idx.batchDeleteObjects(t.Context(),
					map[string][]strfmt.UUID{shard.name: {strfmt.UUID(uuid.NewString())}},
					time.Now(), false, nil, 0, ""))
			},
		},
		{
			name: "batchDeleteObjects forwarded", remote: true, wantErr: true,
			run: func(t *testing.T, idx *Index, shard *Shard) error {
				return batchDeleteErr(idx.batchDeleteObjects(t.Context(),
					map[string][]strfmt.UUID{shard.name: {strfmt.UUID(uuid.NewString())}},
					time.Now(), false, nil, 0, ""))
			},
		},
		{
			// only the group whose shard may not be initialized fails, and the
			// group that acquired a shard still has to release it
			name: "batchDeleteObjects partial group failure", wantErr: true,
			setup: func(t *testing.T, idx *Index, shard *Shard) {
				idx.backupProtectedShards.Store(blockedShard, struct{}{})
			},
			run: func(t *testing.T, idx *Index, shard *Shard) error {
				objs, err := idx.batchDeleteObjects(t.Context(), map[string][]strfmt.UUID{
					shard.name:   {strfmt.UUID(uuid.NewString())},
					blockedShard: {strfmt.UUID(uuid.NewString())},
				}, time.Now(), false, nil, 0, "")
				require.NoError(t, err)

				failed := 0
				for _, obj := range objs {
					if obj.Err != nil {
						failed++
					}
				}
				require.Equal(t, 1, failed, "only the blocked group may fail")
				return batchDeleteErr(objs, nil)
			},
		},
		{
			// the referenced source object does not exist, so the shard rejects the write
			name: "AddReferencesBatch local", wantErr: true, wantErrContains: "ref batch",
			run: func(t *testing.T, idx *Index, shard *Shard) error {
				return errors.Join(idx.AddReferencesBatch(t.Context(), objects.BatchReferences{{
					From: &crossref.RefSource{TargetID: strfmt.UUID(uuid.NewString())},
					To:   &crossref.Ref{TargetID: strfmt.UUID(uuid.NewString())},
				}}, nil, 0)...)
			},
		},
		{
			name: "AddReferencesBatch forwarded", remote: true, wantErr: true,
			wantErrContains: "resolve node name",
			run: func(t *testing.T, idx *Index, shard *Shard) error {
				return errors.Join(idx.AddReferencesBatch(t.Context(), objects.BatchReferences{{
					From: &crossref.RefSource{TargetID: strfmt.UUID(uuid.NewString())},
					To:   &crossref.Ref{TargetID: strfmt.UUID(uuid.NewString())},
				}}, nil, 0)...)
			},
		},
		{
			// objectVectorSearch looks the shard up itself instead of going through
			// withShardOrRemote
			name: "objectVectorSearch local",
			run: func(t *testing.T, idx *Index, shard *Shard) error {
				_, _, err := idx.objectVectorSearch(t.Context(), []models.Vector{[]float32{1, 2, 3}},
					[]string{""}, 0, 10, nil, nil, nil, additional.Properties{}, nil, "", nil, nil)
				return err
			},
		},
		{
			name: "objectVectorSearch forwarded", remote: true, wantErr: true,
			run: func(t *testing.T, idx *Index, shard *Shard) error {
				_, _, err := idx.objectVectorSearch(t.Context(), []models.Vector{[]float32{1, 2, 3}},
					[]string{""}, 0, 10, nil, nil, nil, additional.Properties{}, nil, "", nil, nil)
				return err
			},
		},
		{
			name: "multiObjectByID local",
			setup: func(t *testing.T, idx *Index, shard *Shard) {
				putTestObject(t, shard, className, readableObjectID)
			},
			run: func(t *testing.T, idx *Index, shard *Shard) error {
				found, err := idx.multiObjectByID(t.Context(),
					[]multi.Identifier{{ID: readableObjectID, ClassName: className}}, "")
				if err != nil {
					return err
				}
				require.Len(t, found, 1)
				require.Equal(t, strfmt.UUID(readableObjectID), found[0].ID())
				return nil
			},
		},
		{
			// a failed local read has to surface; returning the objects collected
			// so far with a nil error reports a partial result as a complete one
			name: "multiObjectByID local read error", wantErr: true,
			setup: func(t *testing.T, idx *Index, shard *Shard) {
				corruptStoredObject(t, shard,
					putTestObject(t, shard, className, undecodableObjectID))
			},
			run: func(t *testing.T, idx *Index, shard *Shard) error {
				_, err := idx.multiObjectByID(t.Context(),
					[]multi.Identifier{{ID: undecodableObjectID, ClassName: className}}, "")
				return err
			},
		},
		{
			name: "multiObjectByID forwarded", remote: true, wantErr: true,
			run: func(t *testing.T, idx *Index, shard *Shard) error {
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
			if test.setup != nil {
				test.setup(t, idx, shard)
			}

			for i := 0; i < 3; i++ {
				err := test.run(t, idx, shard)
				if test.wantErr {
					require.Error(t, err, "the exercised branch must be the one under test")
				} else {
					require.NoError(t, err)
				}
				if test.wantErrContains != "" {
					require.ErrorContains(t, err, test.wantErrContains,
						"the failure must be the one the exercised branch produces")
				}
				require.Equalf(t, uint64(0), shard.lifecycle.inUse(),
					"after %d operation(s) every acquire must have exactly one release", i+1)
				require.Emptyf(t, releaseMisuse(hook),
					"after %d operation(s) no call site may release twice", i+1)
			}
		})
	}
}

// TestShardRefCountSchemaWaitFailure covers the write paths that wait for the
// schema version a second time inside getShardForDirectLocalOperation, which by
// then already holds a reference: a failed wait still has to release it.
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
			require.Equal(t, uint64(0), shard.lifecycle.inUse(),
				"a failed schema wait must still release the shard")
		})
	}
}

// TestWithShardOrRemoteRunsOneArm asserts that withShardOrRemote runs exactly
// one arm — local when it has a usable shard, remote when it has not, neither
// when the lookup fails — passing on its error and always releasing the shard.
func TestWithShardOrRemoteRunsOneArm(t *testing.T) {
	className := "WithShardOrRemote"
	const schemaVersion = uint64(7)

	errArm := errors.New("arm failed")
	const localPanic, remotePanic = "local arm panicked", "remote arm panicked"

	tests := []struct {
		name string
		// forwarded points the router at a peer, so there is no usable local shard
		forwarded bool
		// failSchemaWait makes the lookup fail before it can pick a branch
		failSchemaWait bool
		// failArm makes the branch that runs return errArm
		failArm bool
		// wantPanic makes the branch that runs panic with this value
		wantPanic  string
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
		{
			name: "panicking local arm", wantPanic: localPanic,
			operation: localShardOperationWrite, wantLocal: true,
		},
		{
			name: "panicking remote arm", forwarded: true, wantPanic: remotePanic,
			operation: localShardOperationWrite, wantRemote: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			idx, shard := refCountTestIndex(t, className)
			hook := releaseMisuseHook(t, idx)
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
			var err error
			run := func() {
				err = idx.withShardOrRemote(t.Context(), "", shard.name, test.operation, version,
					func(got ShardLike) error {
						ranLocal = true
						require.NotNil(t, got, "the local arm must never be handed a nil shard")
						require.Positive(t, shard.lifecycle.inUse(),
							"the shard must stay referenced for as long as the arm runs")
						if test.wantPanic != "" {
							panic(localPanic)
						}
						return armErr
					},
					func() error {
						ranRemote = true
						if test.wantPanic != "" {
							panic(remotePanic)
						}
						return armErr
					})
			}

			switch {
			case test.wantPanic != "":
				require.PanicsWithValue(t, test.wantPanic, run,
					"the panic must come from the arm under test")
			case test.failArm:
				run()
				require.ErrorIs(t, err, errArm, "the arm's error must reach the caller")
			case test.failSchemaWait:
				run()
				require.Error(t, err)
			default:
				run()
				require.NoError(t, err)
			}
			require.Equal(t, test.wantLocal, ranLocal, "local arm")
			require.Equal(t, test.wantRemote, ranRemote, "remote arm")
			require.Equal(t, uint64(0), shard.lifecycle.inUse(),
				"every branch must release the shard reference")
			require.Empty(t, releaseMisuse(hook), "no branch may release twice")
		})
	}
}

// TestShardLookupReleasesReplacedReference asserts that getShardForWrite and
// getShardForRead release the reference they were handed whenever they hand back
// a different one; the caller only defers the returned release.
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
			require.Equal(t, uint64(1), shard.lifecycle.inUse())

			wantInUse := uint64(1)
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
			require.Equal(t, wantInUse, shard.lifecycle.inUse(),
				"the reference that is not handed back must be released")

			gotRelease()
			require.Equal(t, uint64(0), shard.lifecycle.inUse())
			require.Empty(t, releaseMisuse(hook), "no reference may be released twice")
		})
	}
}

// TestPreventShutdownReleaseIsIdempotent asserts that a caller releasing more
// than once per acquire cannot drive the counter negative, which would disable
// the in-use guard, and that the misuse is reported rather than silently
// absorbed.
func TestPreventShutdownReleaseIsIdempotent(t *testing.T) {
	idx, shard := refCountTestIndex(t, "RefCountIdempotent")
	hook := releaseMisuseHook(t, idx)

	release, err := shard.preventShutdown()
	require.NoError(t, err)
	require.Equal(t, uint64(1), shard.lifecycle.inUse())

	release()
	require.Empty(t, releaseMisuse(hook), "one release per acquire is not a misuse")

	release()
	release()

	require.Equal(t, uint64(0), shard.lifecycle.inUse())
	entries := releaseMisuse(hook)
	require.Len(t, entries, 2, "each extra release must be reported")
	for _, entry := range entries {
		require.Equal(t, logrus.ErrorLevel, entry.Level)
	}
}

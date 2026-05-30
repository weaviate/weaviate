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

// Package replica_test write-gate regression tests (B1 + B2).
//
// B1: gate-closed IfVersion must return a clear "CAS not active" error - never
// silently pass. A silent pass is a lost-update vector: the write would be
// treated as unconditional while the caller believes it was conditional.
//
// B2: PutObjects (batch path) must mint a coherent local version before
// coord.Push when the gate is open, so batch-written objects do not carry
// Version=0 forever.
package replica_test

import (
	"context"
	"strings"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	routerTypes "github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/replica"
)

// --------------------------------------------------------------------------
// B1: gate-closed IfVersion returns a clear "CAS not active" error
// --------------------------------------------------------------------------

// TestB1_GateClosed_IfVersionReturnsCASNotActiveError verifies that when the
// write-gate is closed (PERSISTENCE_OBJECT_VERSION_WRITE != 2), a conditional
// write request (IfVersion != nil) returns a clear "version-CAS not active"
// error rather than silently passing as an unconditional write.
//
// Causal link: before the fix, there was no gate-closed guard in PutObject.
// A request with IfVersion set would proceed through the lock, the digest read,
// and the mint - at v1 the Version field is not persisted, so the CAS check
// would compare 0 == 0 (or stale value) and pass silently. The client believes
// the write was conditional; the server wrote unconditionally. This test catches
// that regression: with gate closed the error must appear before any network
// call (the mock expectations are not configured, so any attempt to dial a
// replica would panic the test).
func TestB1_GateClosed_IfVersionReturnsCASNotActiveError(t *testing.T) {
	orig := storobj.GetWriteMarshallerVersion()
	storobj.SetWriteMarshallerVersion(1)
	defer storobj.SetWriteMarshallerVersion(orig)

	const (
		cls   = "CASTest"
		shard = "SH1"
	)
	nodes := []string{"A", "B", "C"}
	f := newFakeFactory(t, cls, shard, nodes, false)
	rep := f.newReplicator()

	objID := strfmt.UUID("11111111-1111-4111-8111-111111111111")
	ifVersion := uint64(5)
	obj := &storobj.Object{}
	obj.Object.ID = objID
	obj.Conditional.IfVersion = &ifVersion

	err := rep.PutObject(context.Background(), shard, obj, routerTypes.ConsistencyLevelQuorum, 0)

	require.Error(t, err, "gate-closed conditional write must return an error")
	assert.True(t, strings.Contains(err.Error(), "version-CAS not active"),
		"error must contain 'version-CAS not active'; got: %v", err)
	assert.False(t, strings.Contains(err.Error(), "precondition"),
		"gate-closed error must NOT look like a 412 precondition failure; got: %v", err)
}

// TestB1_GateClosed_UnconditionalWritePassesThrough verifies that gate-closed
// does NOT block unconditional writes (IfVersion == nil). Phase-1
// OnlyIfNotExists/OnlyIfExists semantics must still work.
//
// Causal link: the gate-closed guard must only reject IfVersion, not all writes.
// A too-broad guard would break all writes on the default installation (gate closed).
func TestB1_GateClosed_UnconditionalWritePassesThrough(t *testing.T) {
	orig := storobj.GetWriteMarshallerVersion()
	storobj.SetWriteMarshallerVersion(1)
	defer storobj.SetWriteMarshallerVersion(orig)

	const (
		cls   = "UnconditionalTest"
		shard = "SH1"
	)
	nodes := []string{"A"}
	f := newFakeFactory(t, cls, shard, nodes, false)
	rep := f.newReplicator()

	objID := strfmt.UUID("22222222-2222-4222-8222-222222222222")
	obj := &storobj.Object{}
	obj.Object.ID = objID
	// IfVersion is nil: unconditional write.

	// Wire mock: expect the write to proceed to the network layer (not stopped by the gate).
	okResp := replica.SimpleResponse{}
	f.RClient.On("DigestObjects", mock.Anything, mock.Anything, cls, shard,
		[]strfmt.UUID{objID}, 0,
	).Return([]routerTypes.RepairResponse{{ID: objID.String(), Version: 0}}, nil).Maybe()
	f.WClient.On("PutObject", mock.Anything, "A", cls, shard, mock.Anything, obj, uint64(0)).
		Return(okResp, nil)
	f.WClient.On("Commit", mock.Anything, "A", cls, shard, mock.Anything, mock.Anything).
		Return(nil).RunFn = func(args mock.Arguments) {
		resp := args[5].(*replica.SimpleResponse)
		*resp = okResp
	}

	err := rep.PutObject(context.Background(), shard, obj, routerTypes.ConsistencyLevelOne, 0)
	assert.NoError(t, err, "gate-closed unconditional write must not be blocked by the gate guard")
}

// --------------------------------------------------------------------------
// B2: batch path mints version when gate is open
// --------------------------------------------------------------------------

// TestB2_PutObjects_MintsVersionWhenGateOpen verifies that PutObjects mints a
// non-zero version on each object before fanning out to replicas when the
// write-gate is open (v2). Before the fix, PutObjects did not call
// resolveCurrentVersion and all batch-written objects carried Version=0.
//
// Causal link: with Version=0, a later single-object If-Match:0 PUT would
// unconditionally succeed against a batch-upserted-many-times object - the CAS
// guarantee is violated. This test catches that regression by verifying the
// object's Version is >= 1 after the batch write.
func TestB2_PutObjects_MintsVersionWhenGateOpen(t *testing.T) {
	orig := storobj.GetWriteMarshallerVersion()
	storobj.SetWriteMarshallerVersion(2)
	defer storobj.SetWriteMarshallerVersion(orig)

	const (
		cls   = "BatchTest"
		shard = "SH1"
	)
	nodes := []string{"A", "B", "C"}
	f := newFakeFactory(t, cls, shard, nodes, false)
	rep := f.newReplicator()

	objID := strfmt.UUID("33333333-3333-4333-8333-333333333333")
	obj := &storobj.Object{}
	obj.Object.ID = objID
	obj.Version = 0 // explicit zero to confirm it gets bumped

	// Wire mock: DigestObjects for the version resolution (CL=ONE → one node).
	const prevVersion = int64(7)
	f.RClient.On("DigestObjects", mock.Anything, mock.Anything, cls, shard,
		[]strfmt.UUID{objID}, 0,
	).Return([]routerTypes.RepairResponse{{ID: objID.String(), Version: prevVersion}}, nil).Maybe()

	// Wire mock: PutObjects and Commit.
	okResp := replica.SimpleResponse{}
	for _, n := range nodes {
		f.WClient.On("PutObjects", mock.Anything, n, cls, shard, mock.Anything, []*storobj.Object{obj}, uint64(0)).
			Return(okResp, nil)
		f.WClient.On("Commit", mock.Anything, n, cls, shard, mock.Anything, mock.Anything).
			Return(nil).RunFn = func(args mock.Arguments) {
			resp := args[5].(*replica.SimpleResponse)
			*resp = okResp
		}
	}

	errs := rep.PutObjects(context.Background(), shard, []*storobj.Object{obj}, routerTypes.ConsistencyLevelAll, 0)
	for _, err := range errs {
		require.NoError(t, err, "PutObjects must succeed")
	}

	assert.Equal(t, uint64(prevVersion+1), obj.Version,
		"batch-written object must carry Version = prevVersion+1 (not 0) after PutObjects; "+
			"a later If-Match:0 must correctly 412")
	assert.Equal(t, uint8(2), obj.MarshallerVersion,
		"batch-written object must use MarshallerVersion=2 when gate is open")
}

// TestB2_PutObjects_DoesNotMintWhenGateClosed verifies that PutObjects does
// NOT touch obj.Version when the write-gate is closed (v1). Gate-closed objects
// must keep Version=0 (not persisted) and MarshallerVersion=1.
func TestB2_PutObjects_DoesNotMintWhenGateClosed(t *testing.T) {
	orig := storobj.GetWriteMarshallerVersion()
	storobj.SetWriteMarshallerVersion(1)
	defer storobj.SetWriteMarshallerVersion(orig)

	const (
		cls   = "BatchClosedTest"
		shard = "SH1"
	)
	nodes := []string{"A"}
	f := newFakeFactory(t, cls, shard, nodes, false)
	rep := f.newReplicator()

	objID := strfmt.UUID("44444444-4444-4444-8444-444444444444")
	obj := &storobj.Object{}
	obj.Object.ID = objID
	obj.Version = 0

	okResp := replica.SimpleResponse{}
	f.WClient.On("PutObjects", mock.Anything, "A", cls, shard, mock.Anything, []*storobj.Object{obj}, uint64(0)).
		Return(okResp, nil)
	f.WClient.On("Commit", mock.Anything, "A", cls, shard, mock.Anything, mock.Anything).
		Return(nil).RunFn = func(args mock.Arguments) {
		resp := args[5].(*replica.SimpleResponse)
		*resp = okResp
	}

	errs := rep.PutObjects(context.Background(), shard, []*storobj.Object{obj}, routerTypes.ConsistencyLevelOne, 0)
	for _, err := range errs {
		require.NoError(t, err, "PutObjects must succeed gate-closed")
	}

	assert.Equal(t, uint64(0), obj.Version,
		"gate-closed batch write must NOT mint Version (Version stays 0; not persisted in v1 binary)")
	// MarshallerVersion should remain 0 (not set by the gate-closed path).
}

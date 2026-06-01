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

package replica_test

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	routerTypes "github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/replica"
)

// mkBatchObj returns a storobj.Object with the given UUID and no conditions set.
func mkBatchObj(id strfmt.UUID) *storobj.Object {
	return storobj.FromObject(&models.Object{
		Class: "C1",
		ID:    id,
	}, nil, nil, nil)
}

// mkBatchObjWithExistence returns an object with OnlyIfNotExists set.
func mkBatchObjWithExistence(id strfmt.UUID) *storobj.Object {
	obj := mkBatchObj(id)
	obj.Conditional.OnlyIfNotExists = true
	return obj
}

// mkBatchObjWithOnlyIfExists returns an object with OnlyIfExists set.
func mkBatchObjWithOnlyIfExists(id strfmt.UUID) *storobj.Object {
	obj := mkBatchObj(id)
	obj.Conditional.OnlyIfExists = true
	return obj
}

// mkBatchObjWithIfVersion returns an object with IfVersion set.
func mkBatchObjWithIfVersion(id strfmt.UUID, v uint64) *storobj.Object {
	obj := mkBatchObj(id)
	obj.Conditional.IfVersion = &v
	return obj
}

// mkBatchObjWithUpdateIf returns an object with a field-predicate (UpdateIf) set.
func mkBatchObjWithUpdateIf(id strfmt.UUID, prop, val string) *storobj.Object {
	obj := mkBatchObj(id)
	obj.Conditional.UpdateIf = &storobj.Predicate{
		PropertyName:  prop,
		ExpectedValue: val,
		ValueType:     storobj.PredicateValueText,
		Operator:      storobj.EqOperator,
	}
	return obj
}

// TestBatchConditionalAdmissionGuard verifies that PutObjects rejects any object
// carrying a non-empty Conditional (all three forms) with a clear per-object error,
// while non-conditional objects in the same batch proceed through the normal AP path
// and succeed.
//
// Causal link: without the guard, PutObjects fans out ALL objects (including conditional
// ones) to replicas as unconditional writes. The condition is silently dropped. This
// test catches that failure mode because it asserts that conditional objects receive
// errConditionalInBatch and that the error message names the limitation. A test run
// against the pre-guard code would see nil errors for the conditional objects, failing
// the assert.Error checks.
func TestBatchConditionalAdmissionGuard(t *testing.T) {
	const (
		cls   = "C1"
		shard = "SH1"
	)

	unconditionalID := strfmt.UUID("00000000-0000-0000-0000-000000000010")
	conditionalID := strfmt.UUID("00000000-0000-0000-0000-000000000011")
	nodes := []string{"A", "B", "C"}

	type row struct {
		name          string
		conditionalFn func(strfmt.UUID) *storobj.Object
	}

	rows := []row{
		{
			name:          "OnlyIfNotExists (existence-phase1)",
			conditionalFn: mkBatchObjWithExistence,
		},
		{
			name: "OnlyIfExists (existence-phase1b)",
			conditionalFn: func(id strfmt.UUID) *storobj.Object {
				return mkBatchObjWithOnlyIfExists(id)
			},
		},
		{
			name: "IfVersion (version-CAS phase2)",
			conditionalFn: func(id strfmt.UUID) *storobj.Object {
				return mkBatchObjWithIfVersion(id, 42)
			},
		},
		{
			name: "UpdateIf (field-predicate phase3)",
			conditionalFn: func(id strfmt.UUID) *storobj.Object {
				return mkBatchObjWithUpdateIf(id, "status", "active")
			},
		},
	}

	for _, r := range rows {
		r := r
		t.Run(r.name, func(t *testing.T) {
			f := newFakeFactory(t, cls, shard, nodes, false)
			rep := f.newReplicator()

			unconditionalObj := mkBatchObj(unconditionalID)
			conditionalObj := r.conditionalFn(conditionalID)

			okResp := replica.SimpleResponse{}

			// TestMain opens the write-gate to v2 for all tests in this package, so
			// PutObjects will call resolveCurrentVersion (a digest read) for the
			// unconditional object before fanning out. Wire DigestObjects to return
			// version 0 (object absent) for the unconditional object.
			wireDigestResponse(f.RClient, cls, shard, unconditionalID, 0)

			// Wire the unconditional object's prepare + commit path for all nodes.
			// The conditional object must NOT reach the replicas at all.
			var commitWG sync.WaitGroup
			for _, n := range nodes {
				f.WClient.On("PutObjects", mock.Anything, n, cls, shard, mock.Anything, mock.Anything, uint64(0)).
					Return(okResp, nil).Maybe()
				commitWG.Add(1)
				f.WClient.On("Commit", mock.Anything, n, cls, shard, mock.Anything, mock.Anything).
					Return(nil).Maybe().
					RunFn = func(args mock.Arguments) {
					defer commitWG.Done()
					resp := args[5].(*replica.SimpleResponse)
					*resp = okResp
				}
				f.WClient.On("Abort", mock.Anything, n, cls, shard, mock.Anything).
					Return(okResp, nil).Maybe()
			}

			ctx := context.Background()
			errs := rep.PutObjects(ctx, shard,
				[]*storobj.Object{unconditionalObj, conditionalObj},
				routerTypes.ConsistencyLevelQuorum, 0)

			commitWG.Wait()

			// unconditionalObj is at index 0: must succeed.
			assert.NoError(t, errs[0],
				"[%s] non-conditional object in batch must succeed via AP path", r.name)

			// conditionalObj is at index 1: must receive a clear error naming the limitation.
			assert.Error(t, errs[1],
				"[%s] conditional object in batch must receive an error", r.name)
			assert.True(t,
				strings.Contains(errs[1].Error(), "conditional writes are not supported in batch"),
				"[%s] error message must name the limitation; got: %v", r.name, errs[1])
			assert.True(t,
				strings.Contains(errs[1].Error(), "submit individually"),
				"[%s] error message must say 'submit individually'; got: %v", r.name, errs[1])
		})
	}
}

// TestBatchAllConditionalReturnsEarlyNoFanOut verifies that when every object in the
// batch carries a condition, PutObjects returns all errors immediately without
// performing any fan-out (no RPC is issued to replicas).
//
// Causal link: the fast-return path exits before coord.Push is called. Without the
// guard, coord.Push would be called with all objects, silently dropping conditions.
// This test catches that because the mock client has no expectations registered for
// PutObjects; any unexpected call would fail the test.
func TestBatchAllConditionalReturnsEarlyNoFanOut(t *testing.T) {
	const (
		cls   = "C1"
		shard = "SH1"
	)
	nodes := []string{"A", "B", "C"}
	f := newFakeFactory(t, cls, shard, nodes, false)
	rep := f.newReplicator()

	id1 := strfmt.UUID("00000000-0000-0000-0000-000000000020")
	id2 := strfmt.UUID("00000000-0000-0000-0000-000000000021")

	// Both objects carry conditions; no WClient expectations are set, so any
	// unexpected call (PutObjects / Commit / Abort) will fail the mock assertions.
	obj1 := mkBatchObjWithExistence(id1)
	obj2 := mkBatchObjWithIfVersion(id2, 1)

	ctx := context.Background()
	errs := rep.PutObjects(ctx, shard,
		[]*storobj.Object{obj1, obj2},
		routerTypes.ConsistencyLevelQuorum, 0)

	assert.Len(t, errs, 2)
	for i, err := range errs {
		assert.Error(t, err, "object[%d] must have an error", i)
		assert.True(t,
			strings.Contains(err.Error(), "conditional writes are not supported in batch"),
			"object[%d] error must name the limitation; got: %v", i, err)
	}
}

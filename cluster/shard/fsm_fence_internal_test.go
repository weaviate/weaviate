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

package shard

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	shardproto "github.com/weaviate/weaviate/cluster/shard/proto"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	"google.golang.org/protobuf/proto"
)

// fenceShard is a configurable shard stub for schema-fence tests: it records
// fence interactions and routes writes to counters. Unset hooks use permissive
// defaults.
type fenceShard struct {
	stubShard

	waitCalls    []uint64
	waitFn       func(version uint64) error
	presentCalls int
	presentFn    func() bool

	putSingles int
	putBatches int
}

func (f *fenceShard) WaitForSchemaVersion(_ context.Context, version uint64) error {
	f.waitCalls = append(f.waitCalls, version)
	if f.waitFn != nil {
		return f.waitFn(version)
	}
	return nil
}

func (f *fenceShard) ClassPresent() bool {
	f.presentCalls++
	if f.presentFn != nil {
		return f.presentFn()
	}
	return true
}

func (f *fenceShard) PutObject(context.Context, *storobj.Object) error {
	f.putSingles++
	return nil
}

func (f *fenceShard) PutObjectBatch(_ context.Context, objs []*storobj.Object) []error {
	f.putBatches++
	return make([]error, len(objs))
}

// fencePayload builds one marshalled ApplyRequest payload (the post-strip
// command body DispatchBatch consumes) carrying the given schema version.
func fencePayload(t *testing.T, typ shardproto.ApplyRequest_Type, version uint64) []byte {
	t.Helper()
	obj := &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:                 strfmt.UUID("00000000-0000-4000-8000-00000000fe0e"),
			Class:              "FenceC",
			CreationTimeUnix:   1000000,
			LastUpdateTimeUnix: 1000000,
		},
		Vector:    []float32{0.1},
		VectorLen: 1,
	}
	raw, err := obj.MarshalBinary()
	require.NoError(t, err)

	var sub proto.Message
	switch typ {
	case shardproto.ApplyRequest_TYPE_PUT_OBJECT:
		sub = &shardproto.PutObjectRequest{Object: raw, SchemaVersion: version}
	case shardproto.ApplyRequest_TYPE_PUT_OBJECTS_BATCH:
		sub = &shardproto.PutObjectsBatchRequest{Objects: [][]byte{raw}, SchemaVersion: version}
	default:
		t.Fatalf("unsupported fence payload type %v", typ)
	}
	subCmd, err := proto.Marshal(sub)
	require.NoError(t, err)
	body, err := proto.Marshal(&shardproto.ApplyRequest{
		Type: typ, Class: "FenceC", Shard: "FenceS", SubCommand: subCmd,
	})
	require.NoError(t, err)
	return body
}

// dispatchFence drives one payload through DispatchBatch, returning the park
// outcome and the collected responses.
func dispatchFence(t *testing.T, f *FSM, payload []byte, index uint64) (*entryPark, []Response) {
	t.Helper()
	var got []Response
	parked, ok := f.DispatchBatch([]fsmCmd{{payload: payload, index: index}},
		func(_, _ int, resps []Response) bool {
			got = append(got, resps...)
			return true
		})
	require.True(t, ok)
	return parked, got
}

func newFenceFSM(t *testing.T, class string, sh shard) *FSM {
	t.Helper()
	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)
	f := NewFSM(class, "FenceS", "n1", logger)
	f.SetShard(sh)
	return f
}

func classDroppedDelta(class string) func() float64 {
	before := testutil.ToFloat64(shardRaftApplySkipped.WithLabelValues(class, "FenceS", skipReasonClassDropped))
	return func() float64 {
		return testutil.ToFloat64(shardRaftApplySkipped.WithLabelValues(class, "FenceS", skipReasonClassDropped)) - before
	}
}

// TestFSM_SchemaFence_WaitThenLand pins the fence's happy path on both write
// shapes: an entry stamped with a schema version waits on the local schema to
// reach that exact version, then materializes without error.
func TestFSM_SchemaFence_WaitThenLand(t *testing.T) {
	tests := []struct {
		name    string
		typ     shardproto.ApplyRequest_Type
		landed  func(sh *fenceShard) int
		version uint64
	}{
		{name: "put batch", typ: shardproto.ApplyRequest_TYPE_PUT_OBJECTS_BATCH, landed: func(sh *fenceShard) int { return sh.putBatches }, version: 7},
		{name: "single put", typ: shardproto.ApplyRequest_TYPE_PUT_OBJECT, landed: func(sh *fenceShard) int { return sh.putSingles }, version: 9},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sh := &fenceShard{}
			f := newFenceFSM(t, "FenceWait", sh)

			parked, resps := dispatchFence(t, f, fencePayload(t, tc.typ, tc.version), 5)
			require.Nil(t, parked)
			require.Len(t, resps, 1)
			require.NoError(t, resps[0].Error)
			require.Equal(t, []uint64{tc.version}, sh.waitCalls,
				"materialization must fence on exactly the stamped schema version")
			require.Equal(t, 1, tc.landed(sh))
			require.Equal(t, uint64(5), f.LastAppliedIndex())
		})
	}
}

// TestFSM_SchemaFence_DropAfterAdmission_AbandonsDeterministically pins the
// drop disambiguation: at or past the stamped version with the class absent,
// the write was admitted before a class drop — the entry is abandoned
// deterministically (skip counted, applied advances, shard never called),
// with no error-string logic anywhere.
func TestFSM_SchemaFence_DropAfterAdmission_AbandonsDeterministically(t *testing.T) {
	tests := []struct {
		name string
		typ  shardproto.ApplyRequest_Type
	}{
		{name: "put batch", typ: shardproto.ApplyRequest_TYPE_PUT_OBJECTS_BATCH},
		{name: "single put", typ: shardproto.ApplyRequest_TYPE_PUT_OBJECT},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sh := &fenceShard{presentFn: func() bool { return false }}
			f := newFenceFSM(t, "FenceDrop", sh)
			dropped := classDroppedDelta("FenceDrop")

			parked, resps := dispatchFence(t, f, fencePayload(t, tc.typ, 7), 5)
			require.Nil(t, parked, "a dropped class abandons — it must never park")
			require.Len(t, resps, 1)
			require.NoError(t, resps[0].Error)
			require.Equal(t, uint64(5), f.LastAppliedIndex(), "abandon advances the applied index")
			require.Zero(t, sh.putBatches+sh.putSingles, "an abandoned entry must not touch the shard")
			require.Equal(t, float64(1), dropped(), "the abandon must be counted")
		})
	}
}

// TestFSM_SchemaFence_LegacyVersionZeroPassthrough pins the rolling-upgrade
// contract: a command stamped with version 0 (a legacy proposer) bypasses the
// fence entirely — no wait, no presence check — and materializes as before.
func TestFSM_SchemaFence_LegacyVersionZeroPassthrough(t *testing.T) {
	sh := &fenceShard{}
	f := newFenceFSM(t, "FenceLegacy", sh)

	parked, resps := dispatchFence(t, f, fencePayload(t, shardproto.ApplyRequest_TYPE_PUT_OBJECTS_BATCH, 0), 5)
	require.Nil(t, parked)
	require.Len(t, resps, 1)
	require.NoError(t, resps[0].Error)
	require.Empty(t, sh.waitCalls, "version 0 must not fence")
	require.Zero(t, sh.presentCalls, "version 0 must not consult class presence")
	require.Equal(t, 1, sh.putBatches)
	require.Equal(t, uint64(5), f.LastAppliedIndex())
}

// TestFSM_SchemaFence_CreateThenImport_NoFalseRejection pins the class-birth
// window: when the local schema lags the stamped version, the fence PARKS the
// entry (wait failure — never an abandon), and the retry re-fences and lands
// once the schema catches up. A create-then-import therefore suffers zero
// false rejections, and the applied index never advances over the fenced
// entry while it waits. Also covers the fence splitting a window: the
// unfenced entry before it lands first.
func TestFSM_SchemaFence_CreateThenImport_NoFalseRejection(t *testing.T) {
	behind := true
	sh := &fenceShard{waitFn: func(version uint64) error {
		if behind {
			return fmt.Errorf("schema version %d not reached", version)
		}
		return nil
	}}
	f := newFenceFSM(t, "FenceBirth", sh)
	dropped := classDroppedDelta("FenceBirth")

	cmds := []fsmCmd{
		{payload: fencePayload(t, shardproto.ApplyRequest_TYPE_PUT_OBJECTS_BATCH, 0), index: 5},
		{payload: fencePayload(t, shardproto.ApplyRequest_TYPE_PUT_OBJECTS_BATCH, 7), index: 6},
	}
	var got []Response
	parked, ok := f.DispatchBatch(cmds, func(_, _ int, resps []Response) bool {
		got = append(got, resps...)
		return true
	})
	require.True(t, ok)
	require.NotNil(t, parked, "a fence wait failure must park the entry")
	require.Equal(t, uint64(6), parked.index)
	require.Equal(t, uint64(5), f.LastAppliedIndex(),
		"the window before the fenced entry lands; applied stops exactly there")
	require.Equal(t, 1, sh.putBatches, "the unfenced entry must have materialized")

	// The schema catches up; the worker's retry re-dispatches from the
	// parked entry, which now fences clean and lands.
	behind = false
	parked2, resps2 := dispatchFence(t, f, cmds[1].payload, 6)
	require.Nil(t, parked2)
	require.Len(t, resps2, 1)
	require.NoError(t, resps2[0].Error)
	require.Equal(t, uint64(6), f.LastAppliedIndex())
	require.Equal(t, 2, sh.putBatches)
	require.Equal(t, float64(0), dropped(),
		"a lagging schema must never be misread as a dropped class — zero false rejections")
}

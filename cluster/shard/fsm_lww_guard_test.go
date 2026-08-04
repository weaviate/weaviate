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

package shard_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/shard"
	"github.com/weaviate/weaviate/cluster/shard/mocks"
	shardproto "github.com/weaviate/weaviate/cluster/shard/proto"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
	"google.golang.org/protobuf/proto"
)

// TestFSM_Dispatch_RunsUnderLWWReplayGuard pins the semantic contract that
// makes RAFT's at-least-once apply safe: every FSM dispatch reaches the shard
// under the LWW replay guard, so a re-applied command (server-side retry
// after a leadership transfer, restart re-delivery of the committed suffix)
// that is strictly older than the locally stored object is dropped by the
// shard's timestamp arbitration instead of clobbering a newer same-UUID
// write. The arbitration behavior itself is pinned at the shard level by the
// change-log replay integration tests, which share this guard.
func TestFSM_Dispatch_RunsUnderLWWReplayGuard(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	marshal := func(t *testing.T, typ shardproto.ApplyRequest_Type, sub proto.Message) []byte {
		t.Helper()
		subCmd, err := proto.Marshal(sub)
		require.NoError(t, err)
		payload, err := proto.Marshal(&shardproto.ApplyRequest{
			Type:       typ,
			Class:      testClassName,
			Shard:      testShardName,
			SubCommand: subCmd,
		})
		require.NoError(t, err)
		return payload
	}
	objBytes := func(t *testing.T) []byte {
		t.Helper()
		b, err := makeTestObjectWithID(strfmt.UUID("12345678-1234-4000-8000-00000000abcd")).MarshalBinary()
		require.NoError(t, err)
		return b
	}

	tests := []struct {
		name    string
		payload func(t *testing.T) []byte
		expect  func(t *testing.T, m *mocks.Mockshard, guarded *atomic.Bool)
	}{
		{
			name: "put object",
			payload: func(t *testing.T) []byte {
				return marshal(t, shardproto.ApplyRequest_TYPE_PUT_OBJECT,
					&shardproto.PutObjectRequest{Object: objBytes(t)})
			},
			expect: func(t *testing.T, m *mocks.Mockshard, guarded *atomic.Bool) {
				m.EXPECT().PutObject(mock.Anything, mock.Anything).RunAndReturn(
					func(ctx context.Context, _ *storobj.Object) error {
						guarded.Store(objects.HasLWWReplayGuard(ctx))
						return nil
					},
				)
			},
		},
		{
			name: "delete object",
			payload: func(t *testing.T) []byte {
				return marshal(t, shardproto.ApplyRequest_TYPE_DELETE_OBJECT,
					&shardproto.DeleteObjectRequest{
						Id:               "12345678-1234-4000-8000-00000000abcd",
						DeletionTimeUnix: time.Now().UnixNano(),
					})
			},
			expect: func(t *testing.T, m *mocks.Mockshard, guarded *atomic.Bool) {
				m.EXPECT().DeleteObject(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
					func(ctx context.Context, _ strfmt.UUID, _ time.Time) error {
						guarded.Store(objects.HasLWWReplayGuard(ctx))
						return nil
					},
				)
			},
		},
		{
			name: "put objects batch",
			payload: func(t *testing.T) []byte {
				return marshal(t, shardproto.ApplyRequest_TYPE_PUT_OBJECTS_BATCH,
					&shardproto.PutObjectsBatchRequest{Objects: [][]byte{objBytes(t)}})
			},
			expect: func(t *testing.T, m *mocks.Mockshard, guarded *atomic.Bool) {
				m.EXPECT().PutObjectBatch(mock.Anything, mock.Anything).RunAndReturn(
					func(ctx context.Context, objs []*storobj.Object) []error {
						guarded.Store(objects.HasLWWReplayGuard(ctx))
						return make([]error, len(objs))
					},
				)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := mocks.NewMockshard(t)
			var guarded atomic.Bool
			tc.expect(t, m, &guarded)

			fsm := shard.NewFSM(testClassName, testShardName, testNodeID, logger)
			fsm.SetShard(m)
			resp, completed := shard.DispatchOne(fsm, tc.payload(t), 1)
			require.True(t, completed, "a healthy dispatch must complete without parking")
			require.NoError(t, resp.Error)
			require.Truef(t, guarded.Load(),
				"FSM dispatched %s without the LWW replay guard — a retried/re-delivered apply can clobber a newer same-UUID write", tc.name)
		})
	}
}

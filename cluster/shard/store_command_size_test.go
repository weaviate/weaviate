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
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/shard"
	shardproto "github.com/weaviate/weaviate/cluster/shard/proto"
)

// TestStore_Apply_RejectsOversizedCommand pins the oversized-command guard.
// Without it, a command whose frame exceeds the send-lane byte cap is dropped
// on every re-send and the group wedges permanently (see the empty-lane
// overflow row in TestSendLane_BoundsAndOrder for the mechanism); every
// intermediate quota admits an oversized first item via etcd's first-entry
// exception, so Apply is the only place that can convert the wedge into an
// error. The guard must: reject with the typed, NON-retryable
// ErrCommandTooLarge; reject immediately (no retry-budget burn, no attempt
// timeout); and leave the group fully healthy for subsequent writes.
func TestStore_Apply_RejectsOversizedCommand(t *testing.T) {
	store, mockShard := newTestStore(t)
	startAndWaitForLeader(t, store)

	tests := []struct {
		name       string
		subCmdSize int
	}{
		// The 12-byte proto overhead (type/class/shard fields plus the
		// SubCommand tag+length) pushes a SubCommand at the cap over it.
		{name: "at the cap", subCmdSize: shard.MaxRaftCommandBytes},
		{name: "far over the cap", subCmdSize: 2 * shard.MaxRaftCommandBytes},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := &shardproto.ApplyRequest{
				Type:       shardproto.ApplyRequest_TYPE_PUT_OBJECT,
				Class:      testClassName,
				Shard:      testShardName,
				SubCommand: make([]byte, tc.subCmdSize),
			}

			start := time.Now()
			_, err := store.Apply(context.Background(), req)
			require.ErrorIs(t, err, shard.ErrCommandTooLarge,
				"oversized command must be rejected with the typed sentinel, not proposed")
			require.False(t, shard.IsRetryableApplyErr(err),
				"ErrCommandTooLarge must not classify retryable — retrying burns the budget on a command that can never commit")
			require.Less(t, time.Since(start), 2*time.Second,
				"rejection must be immediate (pre-propose), not a timeout")
		})
	}

	// The group must stay healthy after rejections: an ordinary write still
	// commits and materializes.
	mockShard.EXPECT().PutObject(mock.Anything, mock.Anything).Return(nil)
	_, err := applyAndWait(t, store, buildPutObjectApplyRequest(t, testClassName, testShardName, makeTestObject()))
	require.NoError(t, err, "a small write after an oversized rejection must succeed")
}

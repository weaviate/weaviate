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

package replication

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/weaviate/weaviate/cluster/replication/changelog"
)

// An older consumer only knows isCCLAlreadyGone, so a lost log must never match it: it retries instead of sealing.
func TestChangeLogLostNeverReadsAsGone(t *testing.T) {
	wrap := func(msg string) error {
		return fmt.Errorf("snapshot change-log LSN on node1: %w",
			status.Error(codes.Internal, "snapshot change-log LSN for index \"C\", shard \"s\", op \"7\": "+msg))
	}
	tests := []struct {
		name     string
		err      error
		wantLost bool
		wantGone bool
	}{
		{name: "nil"},
		{name: "unrelated", err: errors.New("boom")},
		{
			name:     "loaded shard lost the log",
			err:      wrap("incoming snapshot change-log LSN: op \"7\": shard: " + changelog.ErrMsgChangeLogLost + " for that op-id, writes it captured are gone"),
			wantLost: true,
		},
		{
			name:     "unloaded shard lost the log",
			err:      wrap("incoming get change log: op \"7\" on unloaded shard \"s\": shard: " + changelog.ErrMsgChangeLogLost + " for that op-id"),
			wantLost: true,
		},
		{
			name:     "stopped log on a loaded shard",
			err:      wrap("incoming snapshot change-log LSN: op \"7\": shard: " + changelog.ErrMsgNoActiveChangeCaptureLog + " for that op-id"),
			wantGone: true,
		},
		{
			name:     "stopped log seen by the tailer",
			err:      wrap("incoming get change log: " + changelog.ErrMsgNoActiveLog + " \"7\" on shard \"s\""),
			wantGone: true,
		},
		{
			name: "unloaded shard without files",
			err:  wrap("incoming snapshot change-log LSN: shard \"s\" is not loaded"),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.wantLost, IsChangeLogLost(tc.err))
			require.Equal(t, tc.wantGone, isCCLAlreadyGone(tc.err))
		})
	}
}

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

package replica

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReadDeleteBatchResponseKeepsAFailedHostOffTheLevel pins the condition a batch delete's
// per-slot errors survive on. A host whose batch carried one must not count toward the consistency
// level, or a level reached without it hides the slot that failed.
func TestReadDeleteBatchResponseKeepsAFailedHostOffTheLevel(t *testing.T) {
	slotErr := errors.New("no delete reported an outcome for this object")
	oneSlot := DeleteBatchResponse{Batch: []UUID2Error{{UUID: "a"}}}

	tests := []struct {
		name              string
		in                Result[DeleteBatchResponse]
		wantDecreaseLevel bool
		wantSuccesses     int
		wantFailures      int
		wantErr           bool
	}{
		{
			name:              "a host that committed counts toward the level",
			in:                Result[DeleteBatchResponse]{Value: oneSlot},
			wantDecreaseLevel: true,
			wantSuccesses:     1,
		},
		{
			// flipping this counts the host and the slot's error disappears
			name:         "a host whose slot failed does not count toward the level",
			in:           Result[DeleteBatchResponse]{Value: oneSlot, Err: slotErr},
			wantFailures: 1,
		},
		{
			name:         "a host that answered nothing surfaces its error",
			in:           Result[DeleteBatchResponse]{Err: slotErr},
			wantFailures: 1,
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var r Replicator // the method reads no field of its receiver
			successes, failures, decreaseLevel, err := r.readDeleteBatchResponse(tt.in, nil, nil)

			assert.Equal(t, tt.wantDecreaseLevel, decreaseLevel)
			assert.Len(t, successes, tt.wantSuccesses)
			assert.Len(t, failures, tt.wantFailures)
			if !tt.wantErr {
				require.NoError(t, err,
					"a host that answered per slot is read from its batch, not from one error")
				return
			}
			require.ErrorIs(t, err, slotErr)
		})
	}
}

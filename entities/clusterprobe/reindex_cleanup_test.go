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

package clusterprobe

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A "no cleanup here" that a node did not send clears the gate for the whole
// cluster, so every payload that cannot be proven to come from a node has to be
// an error rather than the permissive default.
func TestReindexCleanupActivityInProgress(t *testing.T) {
	tests := []struct {
		name       string
		payload    string
		want       bool
		wantErrMsg string
	}{
		{
			name:    "node is still cleaning up",
			payload: `{"probe":"weaviate/reindex-cleanup-activity","cleaningUp":true}`,
			want:    true,
		},
		{
			name:    "node has nothing to clean up",
			payload: `{"probe":"weaviate/reindex-cleanup-activity","cleaningUp":false}`,
		},
		{
			name:       "marker of another probe",
			payload:    `{"probe":"weaviate/backup-node-activity","cleaningUp":false}`,
			wantErrMsg: "did not come from a Weaviate node",
		},
		{
			name:       "no marker at all",
			payload:    `{"cleaningUp":false}`,
			wantErrMsg: "did not come from a Weaviate node",
		},
		{
			// The peer controls the marker, so the error must not echo it whole.
			name:       "oversized marker is truncated in the error",
			payload:    `{"probe":"` + strings.Repeat("x", 300) + `","cleaningUp":false}`,
			wantErrMsg: loggableTruncationMarker,
		},
		{
			name:       "marker but no cleaningUp field",
			payload:    `{"probe":"weaviate/reindex-cleanup-activity"}`,
			wantErrMsg: `has no "cleaningUp" field`,
		},
		{
			name:       "cleaningUp explicitly null",
			payload:    `{"probe":"weaviate/reindex-cleanup-activity","cleaningUp":null}`,
			wantErrMsg: `has no "cleaningUp" field`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var activity ReindexCleanupActivity
			require.NoError(t, json.Unmarshal([]byte(tt.payload), &activity))

			got, err := activity.InProgress()

			if tt.wantErrMsg != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrMsg)
				assert.False(t, got, "a rejected answer must not read as free")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// The handler marshals what the client unmarshals, so the answer a node builds
// has to survive the wire and still say what it meant.
func TestNewReindexCleanupActivityRoundTrip(t *testing.T) {
	for _, cleaningUp := range []bool{true, false} {
		data, err := json.Marshal(NewReindexCleanupActivity(cleaningUp))
		require.NoError(t, err)

		var decoded ReindexCleanupActivity
		require.NoError(t, json.Unmarshal(data, &decoded))

		got, err := decoded.InProgress()
		require.NoError(t, err)
		assert.Equal(t, cleaningUp, got)
	}
}

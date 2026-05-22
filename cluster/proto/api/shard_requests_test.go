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

package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplicationDetailsState_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name                        string
		message                     string
		wantReplicationDetailsState ReplicationDetailsState
		wantErr                     string
	}{
		{
			name:    "errors as []string",
			message: `{"errors": ["a", "b"]}`,
			wantReplicationDetailsState: ReplicationDetailsState{
				Errors: []ReplicationDetailsError{
					{Message: "a"},
					{Message: "b"},
				},
			},
		},
		{
			name:    "errors as []ReplicationDetailsError",
			message: `{"errors": [{"message": "a"}, {"message": "b"}]}`,
			wantReplicationDetailsState: ReplicationDetailsState{
				Errors: []ReplicationDetailsError{
					{Message: "a"},
					{Message: "b"},
				},
			},
		},
		{
			name:    "errors as []ReplicationDetailsError with erroredTimeUnixMs",
			message: `{"errors": [{"message": "a", "erroredTimeUnixMs": 10}, {"message": "b", "erroredTimeUnixMs": 20}]}`,
			wantReplicationDetailsState: ReplicationDetailsState{
				Errors: []ReplicationDetailsError{
					{Message: "a", ErroredTimeUnixMs: 10},
					{Message: "b", ErroredTimeUnixMs: 20},
				},
			},
		},
		{
			name:    "normal message",
			message: `{"state": "HEALTHY"}`,
			wantReplicationDetailsState: ReplicationDetailsState{
				State: "HEALTHY",
			},
		},
		{
			name:    "normal message with startTimeUnixMs",
			message: `{"state": "HEALTHY", "startTimeUnixMs": 100}`,
			wantReplicationDetailsState: ReplicationDetailsState{
				State:           "HEALTHY",
				StartTimeUnixMs: 100,
			},
		},
		{
			name:    "normal message with null errors",
			message: `{"state": "HEALTHY", "errors": null}`,
			wantReplicationDetailsState: ReplicationDetailsState{
				State: "HEALTHY",
			},
		},
		{
			name:    "normal message with empty errors",
			message: `{"state": "HEALTHY", "errors": []}`,
			wantReplicationDetailsState: ReplicationDetailsState{
				State: "HEALTHY",
			},
		},
		{
			name:    "errors as unparseable format",
			message: `{"errors": "something unparseable"}`,
			wantErr: `cannot unmarshal ReplicationDetailsState.Errors field neither to []ReplicationDetailsError or []string: "something unparseable"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var r ReplicationDetailsState
			err := r.UnmarshalJSON([]byte(tt.message))
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.ErrorContains(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantReplicationDetailsState, r)
			}
		})
	}
}

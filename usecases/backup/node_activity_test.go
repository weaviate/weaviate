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

package backup

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNodeActivityResponseActivity(t *testing.T) {
	busy, idle := true, false

	tests := []struct {
		name    string
		in      NodeActivityResponse
		want    NodeActivity
		wantErr string
	}{
		{
			name: "busy with a backup",
			in:   NodeActivityResponse{Probe: "weaviate/backup-node-activity", Busy: &busy, Kind: "backup", ID: "b1"},
			want: NodeActivity{Busy: true, Kind: "backup", ID: "b1"},
		},
		{
			name: "busy with a restore",
			in:   NodeActivityResponse{Probe: "weaviate/backup-node-activity", Busy: &busy, Kind: "restore", ID: "r1"},
			want: NodeActivity{Busy: true, Kind: "restore", ID: "r1"},
		},
		{
			name: "idle",
			in:   NodeActivityResponse{Probe: "weaviate/backup-node-activity", Busy: &idle},
			want: NodeActivity{},
		},
		{
			name:    "wrong marker",
			in:      NodeActivityResponse{Probe: "weaviate/backup-node-activity-v2", Busy: &idle},
			wantErr: "was not written by the node-activity route",
		},
		{
			name:    "no marker at all",
			in:      NodeActivityResponse{Busy: &idle},
			wantErr: "was not written by the node-activity route",
		},
		{
			name:    "busy absent",
			in:      NodeActivityResponse{Probe: "weaviate/backup-node-activity"},
			wantErr: `answer has no "busy" field`,
		},
		{
			name:    "busy with an unknown kind",
			in:      NodeActivityResponse{Probe: "weaviate/backup-node-activity", Busy: &busy, Kind: "compaction", ID: "b1"},
			wantErr: "busy with kind",
		},
		{
			name:    "busy with no kind",
			in:      NodeActivityResponse{Probe: "weaviate/backup-node-activity", Busy: &busy, ID: "b1"},
			wantErr: "busy with kind",
		},
		{
			name:    "busy with no id",
			in:      NodeActivityResponse{Probe: "weaviate/backup-node-activity", Busy: &busy, Kind: "backup"},
			wantErr: "names no operation id",
		},
		{
			name:    "idle but names a kind",
			in:      NodeActivityResponse{Probe: "weaviate/backup-node-activity", Busy: &idle, Kind: "backup"},
			wantErr: "not busy but names",
		},
		{
			name:    "idle but names an id",
			in:      NodeActivityResponse{Probe: "weaviate/backup-node-activity", Busy: &idle, ID: "b1"},
			wantErr: "not busy but names",
		},
		{
			name: "busy with an id exactly at the length cap",
			in:   NodeActivityResponse{Probe: "weaviate/backup-node-activity", Busy: &busy, Kind: "backup", ID: strings.Repeat("b", 128)},
			want: NodeActivity{Busy: true, Kind: "backup", ID: strings.Repeat("b", 128)},
		},
		{
			name:    "busy with an id one byte over the length cap",
			in:      NodeActivityResponse{Probe: "weaviate/backup-node-activity", Busy: &busy, Kind: "backup", ID: strings.Repeat("b", 129)},
			wantErr: "operation id of 129 bytes, over the 128",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.in.Activity()
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				assert.False(t, got.Busy, "a refused answer must never read as a free node")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// A peer controls every string in an answer, so nothing this build cannot
// vouch for may reach a log or an error unquoted.
func TestNodeActivityResponseQuotesPeerStrings(t *testing.T) {
	busy := true
	res := NodeActivityResponse{Probe: "forged\nprobe: still fine", Busy: &busy, Kind: "k", ID: "i"}

	_, err := res.Activity()

	require.Error(t, err)
	assert.NotContains(t, err.Error(), "\n")
}

func TestNewNodeActivityResponse(t *testing.T) {
	tests := []struct {
		name string
		in   NodeActivity
		want string
	}{
		{
			name: "idle omits kind and id but still states busy",
			in:   NodeActivity{},
			want: `{"probe":"weaviate/backup-node-activity","busy":false}`,
		},
		{
			name: "busy names the kind and the id",
			in:   NodeActivity{Busy: true, Kind: "restore", ID: "r-7"},
			want: `{"probe":"weaviate/backup-node-activity","busy":true,"kind":"restore","id":"r-7"}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := json.Marshal(NewNodeActivityResponse(tt.in))
			require.NoError(t, err)
			assert.JSONEq(t, tt.want, string(encoded))

			var decoded NodeActivityResponse
			require.NoError(t, json.Unmarshal(encoded, &decoded))
			got, err := decoded.Activity()
			require.NoError(t, err)
			assert.Equal(t, tt.in, got)
		})
	}
}

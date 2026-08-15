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
			in:   NodeActivityResponse{Probe: "weaviate/backup-node-activity", Node: "node1", Busy: &busy, Kind: "backup", ID: "b1"},
			want: NodeActivity{Answered: true, Busy: true, Kind: "backup", ID: "b1"},
		},
		{
			name: "busy with a restore",
			in:   NodeActivityResponse{Probe: "weaviate/backup-node-activity", Node: "node1", Busy: &busy, Kind: "restore", ID: "r1"},
			want: NodeActivity{Answered: true, Busy: true, Kind: "restore", ID: "r1"},
		},
		{
			name: "idle",
			in:   NodeActivityResponse{Probe: "weaviate/backup-node-activity", Node: "node1", Busy: &idle},
			want: NodeActivity{Answered: true},
		},
		{
			name:    "wrong marker",
			in:      NodeActivityResponse{Probe: "weaviate/backup-node-activity-v2", Node: "node1", Busy: &idle},
			wantErr: "was not written by the node-activity route",
		},
		{
			name:    "no marker at all",
			in:      NodeActivityResponse{Node: "node1", Busy: &idle},
			wantErr: "was not written by the node-activity route",
		},
		{
			name:    "idle, but written by another node",
			in:      NodeActivityResponse{Probe: "weaviate/backup-node-activity", Node: "node2", Busy: &idle},
			wantErr: "written by node",
		},
		{
			name:    "busy, but written by another node",
			in:      NodeActivityResponse{Probe: "weaviate/backup-node-activity", Node: "node2", Busy: &busy, Kind: "backup", ID: "b1"},
			wantErr: "written by node",
		},
		{
			name:    "idle, and names no writer at all",
			in:      NodeActivityResponse{Probe: "weaviate/backup-node-activity", Busy: &idle},
			wantErr: "written by node",
		},
		{
			name:    "busy absent",
			in:      NodeActivityResponse{Probe: "weaviate/backup-node-activity", Node: "node1"},
			wantErr: `answer has no "busy" field`,
		},
		{
			name:    "busy with an unknown kind",
			in:      NodeActivityResponse{Probe: "weaviate/backup-node-activity", Node: "node1", Busy: &busy, Kind: "compaction", ID: "b1"},
			wantErr: "busy with kind",
		},
		{
			name:    "busy with no kind",
			in:      NodeActivityResponse{Probe: "weaviate/backup-node-activity", Node: "node1", Busy: &busy, ID: "b1"},
			wantErr: "busy with kind",
		},
		{
			name:    "busy with no id",
			in:      NodeActivityResponse{Probe: "weaviate/backup-node-activity", Node: "node1", Busy: &busy, Kind: "backup"},
			wantErr: "names no operation id",
		},
		{
			name:    "idle but names a kind",
			in:      NodeActivityResponse{Probe: "weaviate/backup-node-activity", Node: "node1", Busy: &idle, Kind: "backup"},
			wantErr: "not busy but names",
		},
		{
			name:    "idle but names an id",
			in:      NodeActivityResponse{Probe: "weaviate/backup-node-activity", Node: "node1", Busy: &idle, ID: "b1"},
			wantErr: "not busy but names",
		},
		{
			name: "busy with an id exactly at the length cap",
			in:   NodeActivityResponse{Probe: "weaviate/backup-node-activity", Node: "node1", Busy: &busy, Kind: "backup", ID: strings.Repeat("b", 128)},
			want: NodeActivity{Answered: true, Busy: true, Kind: "backup", ID: strings.Repeat("b", 128)},
		},
		{
			name:    "busy with an id one byte over the length cap",
			in:      NodeActivityResponse{Probe: "weaviate/backup-node-activity", Node: "node1", Busy: &busy, Kind: "backup", ID: strings.Repeat("b", 129)},
			wantErr: "operation id of 129 bytes, over the 128",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.in.Activity("node1")
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				assert.False(t, got.Free(), "a refused answer must never read as a free node")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestNodeActivityResponseQuotesPeerStrings(t *testing.T) {
	busy := true
	res := NodeActivityResponse{Probe: "forged\nprobe: still fine", Node: "node1", Busy: &busy, Kind: "k", ID: "i"}

	_, err := res.Activity("node1")

	require.Error(t, err)
	assert.NotContains(t, err.Error(), "\n")
}

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
	"testing"

	"github.com/stretchr/testify/assert"
)

// Pins these as frozen wire values: changing one silently disables the
// reindex gate across the cluster instead of failing loudly.
func TestWireValuesAreFrozen(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{name: "backup node-activity path", got: BackupNodeActivityPath, want: "/backups/node-activity"},
		{name: "reindex cleanup-activity path", got: ReindexCleanupActivityPath, want: "/reindex/cleanup-activity"},
		{name: "backup node-activity marker", got: BackupNodeActivityMarker, want: "weaviate/backup-node-activity"},
		{name: "reindex cleanup marker", got: ReindexCleanupMarker, want: "weaviate/reindex-cleanup-activity"},
		{name: "probe not-wired marker", got: ProbeNotWiredMarker, want: "weaviate/probe-not-wired"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.got, "changing this breaks the contract between releases")
		})
	}
}

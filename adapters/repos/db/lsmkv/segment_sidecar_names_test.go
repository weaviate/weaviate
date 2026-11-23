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

package lsmkv

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

// The expected names are literals: older builds look for exactly these files.
func TestSegmentSidecarPaths(t *testing.T) {
	tests := []struct {
		name        string
		segmentFile string
		want        []string
	}{
		{
			name:        "plain segment",
			segmentFile: "segment-123.db",
			want: []string{
				"segment-123.bloom",
				"segment-123.secondary.0.bloom",
				"segment-123.secondary.1.bloom",
				"segment-123.cna",
				"segment-123.metadata",
			},
		},
		{
			name:        "segment with level and strategy",
			segmentFile: "segment-123.l2.s4.db",
			want: []string{
				"segment-123.l2.s4.bloom",
				"segment-123.l2.s4.secondary.0.bloom",
				"segment-123.l2.s4.secondary.1.bloom",
				"segment-123.l2.s4.cna",
				"segment-123.l2.s4.metadata",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			s := &segment{path: filepath.Join(dir, tt.segmentFile)}
			got := []string{
				s.bloomFilterPath(),
				s.bloomFilterSecondaryPath(0),
				s.bloomFilterSecondaryPath(1),
				s.countNetPath(),
				s.metadataPath(),
			}
			want := make([]string, len(tt.want))
			for i, name := range tt.want {
				want[i] = filepath.Join(dir, name)
			}
			assert.Equal(t, want, got)
		})
	}
}

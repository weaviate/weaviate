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

package docker

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDumpWindowStart(t *testing.T) {
	mkLines := func(n int, markerAt int, marker string) []string {
		lines := make([]string, n)
		for i := range lines {
			lines[i] = fmt.Sprintf("line %d", i)
		}
		if markerAt >= 0 {
			lines[markerAt] = marker
		}
		return lines
	}

	tests := []struct {
		name    string
		lines   []string
		tail    int
		ceiling int
		want    int
	}{
		{
			name:    "short log without marker dumps everything",
			lines:   mkLines(100, -1, ""),
			tail:    300,
			ceiling: 6000,
			want:    0,
		},
		{
			name:    "long log without marker dumps the tail",
			lines:   mkLines(10000, -1, ""),
			tail:    300,
			ceiling: 6000,
			want:    9700,
		},
		{
			name:    "marker near the end extends the window to before the trace",
			lines:   mkLines(10000, 9950, "fatal error: concurrent map writes"),
			tail:    10,
			ceiling: 6000,
			want:    9930, // marker - 20 lines of lead-in
		},
		{
			name:    "marker already inside the tail leaves the tail as-is",
			lines:   mkLines(10000, 9950, "panic: runtime error"),
			tail:    300,
			ceiling: 6000,
			want:    9700,
		},
		{
			name:    "marker further back than the ceiling is cut at the ceiling",
			lines:   mkLines(10000, 50, "WARNING: DATA RACE"),
			tail:    300,
			ceiling: 6000,
			want:    4000,
		},
		{
			name:    "marker within the first 20 lines clamps to zero",
			lines:   mkLines(100, 5, "panic: boom"),
			tail:    10,
			ceiling: 6000,
			want:    0,
		},
		{
			name: "first of several markers wins",
			lines: func() []string {
				lines := mkLines(1000, 800, "panic: first")
				lines[900] = "panic: second"
				return lines
			}(),
			tail:    10,
			ceiling: 6000,
			want:    780,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, dumpWindowStart(tt.lines, tt.tail, tt.ceiling))
		})
	}
}

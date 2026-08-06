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

package db

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestContainedPath pins the path-traversal guard on the replica-snapshot
// file-serving path: no rel input may resolve outside base.
func TestContainedPath(t *testing.T) {
	base := filepath.Join("/data", "idx", "shard1")

	tests := []struct {
		name    string
		rel     string
		want    string
		wantErr bool
	}{
		{name: "in-bounds relative path", rel: filepath.Join("lsm", "objects", "segment-1.db"), want: filepath.Join(base, "lsm", "objects", "segment-1.db")},
		{name: "dot resolves to base itself", rel: ".", want: base},
		{name: "parent escape", rel: filepath.Join("..", "other"), wantErr: true},
		{name: "nested parent escape", rel: filepath.Join("a", "..", "..", "other"), wantErr: true},
		{name: "sibling sharing base as name prefix", rel: filepath.Join("..", "shard1evil"), wantErr: true},
		{name: "absolute input is joined under base, not honored", rel: "/etc/passwd", want: filepath.Join(base, "etc", "passwd")},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := containedPath(base, tc.rel)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

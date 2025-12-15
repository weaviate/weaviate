//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package diskio

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSanitizeFilePathJoin(t *testing.T) {
	tests := []struct {
		name     string
		relative string
		wantErr  bool
	}{
		{name: "valid relative", relative: "sub/file.txt", wantErr: false},
		{name: "escape with dot-dot", relative: filepath.Join("..", "outside", "out.txt"), wantErr: true},
		{name: "absolute path rejected", relative: filepath.Join(string(filepath.Separator), "etc", "passwd"), wantErr: true},
		{name: "only escaping", relative: "..", wantErr: true},
		{name: "normalized traversal inside root", relative: filepath.Join("sub", "..", "sub", "file.txt"), wantErr: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			got, err := SanitizeFilePathJoin(root, tc.relative)

			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			rootPath, err := filepath.EvalSymlinks(root)
			require.NoError(t, err)

			require.Equal(t, filepath.Join(rootPath, "sub", "file.txt"), got)
		})
	}
}

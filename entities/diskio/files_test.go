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

package diskio

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteFileSync(t *testing.T) {
	t.Run("writes content and is readable", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "sentinel.mig")
		require.NoError(t, WriteFileSync(path, []byte("hello"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600))

		got, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, "hello", string(got))
	})

	t.Run("empty content creates an empty file", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "marker.mig")
		require.NoError(t, WriteFileSync(path, nil, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600))

		info, err := os.Stat(path)
		require.NoError(t, err)
		require.Zero(t, info.Size())
	})

	t.Run("O_EXCL error is os.IsExist-branchable", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "once.mig")
		require.NoError(t, WriteFileSync(path, nil, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600))

		err := WriteFileSync(path, nil, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		require.Error(t, err)
		require.True(t, os.IsExist(err), "callers must be able to branch on os.IsExist")
	})

	t.Run("O_TRUNC overwrites existing content", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "payload.mig")
		require.NoError(t, WriteFileSync(path, []byte("old-and-longer"), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600))
		require.NoError(t, WriteFileSync(path, []byte("new"), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600))

		got, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, "new", string(got))
	})

	t.Run("missing parent directory returns an error", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "nope", "sentinel.mig")
		require.Error(t, WriteFileSync(path, nil, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600))
	})
}

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

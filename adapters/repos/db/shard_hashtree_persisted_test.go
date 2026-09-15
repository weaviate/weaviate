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
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

func serializedHashTree(t *testing.T, seed byte) ([]byte, hashtree.Digest) {
	t.Helper()
	ht, err := hashtree.NewHashTree(2)
	require.NoError(t, err)
	require.NoError(t, ht.AggregateLeafWith(0, []byte{seed}))
	var buf bytes.Buffer
	_, err = ht.Serialize(&buf)
	require.NoError(t, err)
	return buf.Bytes(), ht.Root()
}

func writePersistedHashtree(t *testing.T, dir, name string, seed byte) (string, hashtree.Digest) {
	t.Helper()
	payload, root := serializedHashTree(t, seed)
	require.NoError(t, os.MkdirAll(dir, os.ModePerm))
	filename := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(filename, payload, 0o600))
	return filename, root
}

func TestNewestPersistedHashTreeRoot(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(t *testing.T, dir string) (hashtree.Digest, string)
		wantErr error
		errText string
	}{
		{
			name:    "missing dir",
			setup:   func(t *testing.T, dir string) (hashtree.Digest, string) { return hashtree.Digest{}, "" },
			wantErr: errNoPersistedHashtree,
		},
		{
			name: "empty dir",
			setup: func(t *testing.T, dir string) (hashtree.Digest, string) {
				require.NoError(t, os.MkdirAll(dir, os.ModePerm))
				return hashtree.Digest{}, ""
			},
			wantErr: errNoPersistedHashtree,
		},
		{
			name: "tmp and subdir only",
			setup: func(t *testing.T, dir string) (hashtree.Digest, string) {
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "hashtree-0000000000000009.ht"), os.ModePerm))
				payload, _ := serializedHashTree(t, 1)
				require.NoError(t, os.WriteFile(filepath.Join(dir, "hashtree-0000000000000001.ht.tmp"), payload, 0o600))
				return hashtree.Digest{}, ""
			},
			wantErr: errNoPersistedHashtree,
		},
		{
			name: "single snapshot",
			setup: func(t *testing.T, dir string) (hashtree.Digest, string) {
				f, root := writePersistedHashtree(t, dir, "hashtree-0000000000000001.ht", 1)
				return root, f
			},
		},
		{
			name: "newest of two wins",
			setup: func(t *testing.T, dir string) (hashtree.Digest, string) {
				writePersistedHashtree(t, dir, "hashtree-0000000000000001.ht", 1)
				f, root := writePersistedHashtree(t, dir, "hashtree-0000000000000002.ht", 2)
				return root, f
			},
		},
		{
			name: "corrupt newest does not fall back to older",
			setup: func(t *testing.T, dir string) (hashtree.Digest, string) {
				writePersistedHashtree(t, dir, "hashtree-0000000000000001.ht", 1)
				require.NoError(t, os.WriteFile(filepath.Join(dir, "hashtree-0000000000000002.ht"), []byte("garbage"), 0o600))
				return hashtree.Digest{}, ""
			},
			errText: "read hashtree file",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), hashTreeDirName)
			wantRoot, wantFile := tc.setup(t, dir)
			before := entriesOf(t, dir)

			root, filename, err := newestPersistedHashTreeRoot(dir)
			switch {
			case tc.wantErr != nil:
				require.ErrorIs(t, err, tc.wantErr)
			case tc.errText != "":
				require.ErrorContains(t, err, tc.errText)
			default:
				require.NoError(t, err)
				require.Equal(t, wantRoot, root)
				require.Equal(t, wantFile, filename)
			}
			require.Equal(t, before, entriesOf(t, dir), "reader must not touch the directory")
		})
	}
}

func entriesOf(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

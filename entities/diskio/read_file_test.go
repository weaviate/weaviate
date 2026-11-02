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
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadFileExact(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(t *testing.T, path string)
		content []byte
		wantErr error
	}{
		{
			name:    "eight-byte counter",
			content: []byte{42, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			name:    "empty file",
			content: []byte{},
		},
		{
			name:    "one megabyte",
			content: bytes.Repeat([]byte("proplengths"), 1<<20/len("proplengths")),
		},
		{
			name:    "missing file",
			setup:   func(t *testing.T, path string) {},
			wantErr: fs.ErrNotExist,
		},
		{
			name: "directory",
			setup: func(t *testing.T, path string) {
				require.NoError(t, os.Mkdir(path, 0o755))
			},
			wantErr: errNotRegularFile,
		},
	}

	readers := map[string]func(string) ([]byte, error){
		"ReadFileExact":         ReadFileExact,
		"readFileExactPortable": readFileExactPortable,
	}

	for readerName, read := range readers {
		for _, tc := range tests {
			t.Run(readerName+"/"+tc.name, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "indexcount")
				if tc.setup != nil {
					tc.setup(t, path)
				} else {
					require.NoError(t, os.WriteFile(path, tc.content, 0o600))
				}

				got, err := read(path)

				if tc.wantErr != nil {
					require.ErrorIs(t, err, tc.wantErr)
					return
				}
				require.NoError(t, err)
				// os.ReadFile also returns a non-nil slice for an empty file.
				require.NotNil(t, got)
				require.Equal(t, tc.content, got)
				require.Equal(t, len(tc.content), cap(got))
			})
		}
	}
}

func BenchmarkReadFileExact(b *testing.B) {
	path := filepath.Join(b.TempDir(), "indexcount")
	require.NoError(b, os.WriteFile(path, []byte{42, 0, 0, 0, 0, 0, 0, 0}, 0o600))

	b.Run("ReadFileExact", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := ReadFileExact(path); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("os.ReadFile", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := os.ReadFile(path); err != nil {
				b.Fatal(err)
			}
		}
	})
}

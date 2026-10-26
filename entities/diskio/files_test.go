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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetSubdirNames(t *testing.T) {
	tests := []struct {
		name  string
		dirs  []string
		files []string
		// link name -> target, both relative to the temp root
		symlinks map[string]string
		// path handed to the function, relative to the temp root; empty means the root itself
		subPath string
		want    []string
		wantErr bool
	}{
		{name: "empty directory"},
		{name: "only files", files: []string{"segment-1.db", "segment-2.db"}},
		{name: "single subdirectory", dirs: []string{"objects"}, want: []string{"objects"}},
		{
			name:  "files and subdirectories",
			dirs:  []string{"objects", "property_title", "vectors_text", ".migrations"},
			files: []string{"segment-1.db", "segment-1.db.bloom"},
			want:  []string{"objects", "property_title", "vectors_text", ".migrations"},
		},
		{
			name:     "symlink to directory is not reported",
			dirs:     []string{"objects"},
			symlinks: map[string]string{"objects_link": "objects"},
			want:     []string{"objects"},
		},
		{
			name:     "symlink to file is not reported",
			files:    []string{"segment-1.db"},
			symlinks: map[string]string{"segment-link.db": "segment-1.db"},
		},
		{name: "missing directory", subPath: "does-not-exist", wantErr: true},
		{name: "path is a file", files: []string{"segment-1.db"}, subPath: "segment-1.db", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			for _, dir := range tc.dirs {
				require.NoError(t, os.Mkdir(filepath.Join(root, dir), 0o700))
			}
			for _, file := range tc.files {
				require.NoError(t, os.WriteFile(filepath.Join(root, file), []byte("x"), 0o600))
			}
			for link, target := range tc.symlinks {
				require.NoError(t, os.Symlink(filepath.Join(root, target), filepath.Join(root, link)))
			}

			path := filepath.Join(root, tc.subPath)
			got, err := GetSubdirNames(path)
			// Both functions must report the same subdirectories.
			_, wantFromSizes, sizesErr := GetFileWithSizes(path)

			if tc.wantErr {
				require.Error(t, err)
				require.Error(t, sizesErr)
				return
			}
			require.NoError(t, err)
			require.NoError(t, sizesErr)
			require.ElementsMatch(t, tc.want, got)
			require.ElementsMatch(t, wantFromSizes, got)
		})
	}
}

// BenchmarkSubdirNames compares reading only the entry types against stat'ing every entry,
// on a directory shaped like a shard's lsm/ folder.
func BenchmarkSubdirNames(b *testing.B) {
	const subdirCount = 30

	root := b.TempDir()
	for i := range subdirCount {
		require.NoError(b, os.Mkdir(filepath.Join(root, fmt.Sprintf("property_%d", i)), 0o700))
	}
	for i := range 10 {
		require.NoError(b, os.WriteFile(filepath.Join(root, fmt.Sprintf("segment-%d.db", i)), []byte("x"), 0o600))
	}

	b.Run("GetSubdirNames", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			dirs, err := GetSubdirNames(root)
			if err != nil {
				b.Fatal(err)
			}
			if len(dirs) != subdirCount {
				b.Fatalf("got %d subdirectories, want %d", len(dirs), subdirCount)
			}
		}
	})

	b.Run("GetFileWithSizes", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, dirs, err := GetFileWithSizes(root)
			if err != nil {
				b.Fatal(err)
			}
			if len(dirs) != subdirCount {
				b.Fatalf("got %d subdirectories, want %d", len(dirs), subdirCount)
			}
		}
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

func TestRenameAndSync(t *testing.T) {
	tests := []struct {
		name          string
		from, to      string
		missingSource bool
		wantSyncs     []string
		syncFails     bool
		wantErr       bool
		// Set only where the rename succeeded and just the sync failed.
		wantPublished bool
	}{
		{name: "within one directory", from: "a.tmp", to: "a", wantSyncs: []string{"."}},
		{name: "across two directories", from: "a.tmp", to: "d/a", wantSyncs: []string{"d", "."}},
		{name: "over an existing name", from: "a.tmp", to: "taken", wantSyncs: []string{"."}},
		{name: "source is not there", from: "gone.tmp", to: "a", missingSource: true, wantErr: true},
		{name: "target directory is not there", from: "a.tmp", to: "absent/a", wantErr: true},
		{
			name: "a sync that fails fails the rename", from: "a.tmp", to: "a",
			syncFails: true, wantSyncs: []string{"."}, wantErr: true, wantPublished: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			require.NoError(t, os.Mkdir(filepath.Join(root, "d"), 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(root, "taken"), []byte("old"), 0o600))
			from, to := filepath.Join(root, tc.from), filepath.Join(root, tc.to)
			if !tc.missingSource {
				require.NoError(t, os.WriteFile(from, []byte("new"), 0o600))
			}

			var synced []string
			publishedBeforeFirstSync := false
			err := renameAndSync(from, to, func(dir string) error {
				if len(synced) == 0 {
					_, statErr := os.Stat(to)
					publishedBeforeFirstSync = statErr == nil
				}
				rel, relErr := filepath.Rel(root, dir)
				require.NoError(t, relErr)
				synced = append(synced, rel)
				if tc.syncFails {
					return errors.New("no space")
				}
				return nil
			})

			require.Equal(t, tc.wantSyncs, synced,
				"a rename is durable only once the directories holding its names are synced")
			if len(tc.wantSyncs) > 0 {
				require.True(t, publishedBeforeFirstSync,
					"the sync has to follow the rename it makes durable")
			}

			if tc.wantErr {
				require.Error(t, err)
				_, statErr := os.Stat(to)
				require.Equal(t, tc.wantPublished, statErr == nil,
					"only a post-rename failure may leave the target name published")
				return
			}
			require.NoError(t, err)
			moved, err := os.ReadFile(to)
			require.NoError(t, err)
			require.Equal(t, "new", string(moved), "the target must hold what the source held")
			_, statErr := os.Stat(from)
			require.True(t, os.IsNotExist(statErr), "the source name must be gone")
		})
	}
}

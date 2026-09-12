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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
)

type bucketDirsFixture struct {
	t       *testing.T
	lsmPath string
	dirs    bucketDirs
}

func newBucketDirsFixture(t *testing.T) bucketDirsFixture {
	t.Helper()
	lsmPath := filepath.Join(t.TempDir(), "lsm")
	require.NoError(t, os.MkdirAll(lsmPath, 0o777))
	return bucketDirsFixture{t: t, lsmPath: lsmPath, dirs: shardBucketDirs(lsmPath)}
}

func (f bucketDirsFixture) mkdirs(names ...string) {
	f.t.Helper()
	for _, name := range names {
		require.NoError(f.t, os.MkdirAll(filepath.Join(f.lsmPath, name), 0o777))
		require.NoError(f.t, os.WriteFile(filepath.Join(f.lsmPath, name, "segment-1.db"), []byte(name), 0o600))
	}
}

func (f bucketDirsFixture) exists(name string) bool {
	info, err := os.Stat(filepath.Join(f.lsmPath, name))
	return err == nil && info.IsDir()
}

func TestOnlyAHandleNamingOneDirectoryBecomesAPath(t *testing.T) {
	tests := []struct {
		name             string
		dir              string
		refusedAsPath    bool
		refusedAsRemoval bool
	}{
		{name: "names none", dir: "", refusedAsPath: true, refusedAsRemoval: false},
		{name: "the root itself", dir: ".", refusedAsPath: true, refusedAsRemoval: true},
		{name: "the parent of the root", dir: "..", refusedAsPath: true, refusedAsRemoval: true},
		{name: "a join back to the root", dir: "x/..", refusedAsPath: true, refusedAsRemoval: true},
		{name: "a nested path", dir: "sub/dir", refusedAsPath: true, refusedAsRemoval: true},
		{name: "an absolute path", dir: "/etc", refusedAsPath: true, refusedAsRemoval: true},
		{name: "one directory", dir: "property_title_searchable", refusedAsPath: false, refusedAsRemoval: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newBucketDirsFixture(t)
			f.mkdirs(helpers.ObjectsBucketLSM, "property_title_searchable")

			path, err := f.dirs.Path(tt.dir, "a recorded directory")
			if tt.refusedAsPath {
				require.Error(t, err)
				require.Empty(t, path)
			} else {
				require.NoError(t, err)
				require.Equal(t, filepath.Join(f.lsmPath, tt.dir), path)
			}

			err = f.dirs.Discard(tt.dir, "a recorded directory")
			if tt.refusedAsRemoval {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			require.DirExists(t, f.lsmPath, "the shard's LSM directory")
			require.True(t, f.exists(helpers.ObjectsBucketLSM), "the shard's object store")
			require.Equal(t, tt.refusedAsPath, f.exists("property_title_searchable"),
				"only a handle that names one directory removes one")
		})
	}
}

// The name a promotion renames onto is as capable of escaping the root as the
// one it renames, so both are guarded.
func TestPromoteGuardsBothHandles(t *testing.T) {
	tests := []struct {
		name    string
		from    string
		to      string
		refused bool
	}{
		{name: "the directory to promote escapes", from: "..", to: "property_title_searchable", refused: true},
		{name: "the name to promote onto escapes", from: "property_title__g42_ingest", to: "..", refused: true},
		{name: "neither names a directory", from: "", to: "", refused: true},
		// The canonical name is free by the time a promotion runs; clearing it is
		// the caller's step, not this one's.
		{name: "both name one directory", from: "property_title__g42_ingest", to: "property_title_rangeable"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newBucketDirsFixture(t)
			f.mkdirs("property_title__g42_ingest", "property_title_searchable")

			err := f.dirs.Promote(tt.from, tt.to)

			require.DirExists(t, f.lsmPath, "the shard's LSM directory")
			if tt.refused {
				require.Error(t, err)
				require.True(t, f.exists("property_title__g42_ingest"), "a refused promotion renames nothing")
				require.True(t, f.exists("property_title_searchable"))
				return
			}
			require.NoError(t, err)
			require.False(t, f.exists("property_title__g42_ingest"))
			data, err := os.ReadFile(filepath.Join(f.lsmPath, tt.to, "segment-1.db"))
			require.NoError(t, err)
			require.Equal(t, "property_title__g42_ingest", string(data),
				"the canonical name holds the staged data")
		})
	}
}

// The probe every recorded directory is resolved through. A stat that could
// not answer must not read as an absent directory: the promotion probe would
// take "cannot see it" as proof the rename already ran.
func TestDirExistsSeparatesAbsentFromUnreadable(t *testing.T) {
	tests := []struct {
		name     string
		dir      string
		sealRoot bool
		want     bool
		wantErr  bool
	}{
		{name: "a directory that is there", dir: "property_title_searchable", want: true},
		{name: "nothing at that name", dir: "property_gone_searchable"},
		{name: "a regular file is not a directory", dir: "afile"},
		{name: "a handle naming none reads as absent, not as an error", dir: ""},
		{name: "a handle that does not name one directory under the shard", dir: "sub/dir", wantErr: true},
		{
			name: "a directory the process may not stat is not an absent one",
			dir:  "property_title_searchable", sealRoot: true, wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newBucketDirsFixture(t)
			f.mkdirs("property_title_searchable")
			require.NoError(t, os.WriteFile(filepath.Join(f.lsmPath, "afile"), []byte("x"), 0o600))
			if tt.sealRoot {
				if os.Geteuid() == 0 {
					t.Skip("root traverses a 0o000 directory, so the permission error cannot arise")
				}
				require.NoError(t, os.Chmod(f.lsmPath, 0o000))
				t.Cleanup(func() { os.Chmod(f.lsmPath, 0o700) })
			}

			there, err := f.dirs.Exists(tt.dir)
			if tt.wantErr {
				require.Error(t, err)
				require.False(t, there, "a probe that could not answer must not report a directory")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, there)
		})
	}
}

func TestTrackerDirsRootOneLevelDownAndTakeTheSameGuard(t *testing.T) {
	f := newBucketDirsFixture(t)
	f.mkdirs("property_title_searchable")
	trackers := f.dirs.Trackers()
	require.NoError(t, os.MkdirAll(filepath.Join(trackers.root, "searchable_retokenize_1"), 0o777))

	require.Equal(t, filepath.Join(f.lsmPath, migrationsDir), trackers.root)

	there, err := trackers.Exists("searchable_retokenize_1")
	require.NoError(t, err)
	require.True(t, there)

	require.Error(t, trackers.Discard("..", "a tracker directory"),
		"the parent of the tracker root is the shard's LSM directory")
	require.True(t, f.exists("property_title_searchable"))
}

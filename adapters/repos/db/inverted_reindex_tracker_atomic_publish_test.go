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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Once it has returned, a publish that overwrote properties.mig in place looks
// exactly like one that replaced it: same content, no temp file left over. The
// difference is the window in between, where the target holds neither the old
// bytes nor the new ones — and a crash in that window leaves the torn file that
// retires a shard's reindex for good.
//
// A hard link taken before the publish makes that window observable without a
// crash. A rename leaves the file the link names untouched; a write through the
// target reaches it. The last row is a plain in-place write, so the discriminator
// is pinned as one that actually separates the two.
func TestPropsSidecarIsPublishedByReplacingTheFile(t *testing.T) {
	tests := []struct {
		name    string
		before  []byte
		after   []byte
		publish func(rt *fileReindexTracker, target string, content []byte) error
		// wantReplaced is whether publish is required to leave the old file alone.
		wantReplaced bool
	}{
		{
			name:         "a longer property list",
			before:       []byte("cat"),
			after:        []byte("cat,dog,emu"),
			publish:      publishAtomic,
			wantReplaced: true,
		},
		{
			name:         "a shorter property list",
			before:       []byte("cat,dog,emu"),
			after:        []byte("cat"),
			publish:      publishAtomic,
			wantReplaced: true,
		},
		{
			name:         "a list of the same length",
			before:       []byte("cat,dog"),
			after:        []byte("emu,fox"),
			publish:      publishAtomic,
			wantReplaced: true,
		},
		{
			name:   "the writer the shard actually uses",
			before: []byte("cat"),
			after:  []byte("cat,dog"),
			publish: func(rt *fileReindexTracker, _ string, content []byte) error {
				return rt.saveProps(strings.Split(string(content), ","))
			},
			wantReplaced: true,
		},
		{
			name:   "a plain in-place write, which is what this test exists to catch",
			before: []byte("cat"),
			after:  []byte("cat,dog"),
			publish: func(_ *fileReindexTracker, target string, content []byte) error {
				return os.WriteFile(target, content, 0o644)
			},
			wantReplaced: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lsm := t.TempDir()
			rt := NewFileReindexTracker(lsm, "publish_1", &UuidKeyParser{})
			require.NoError(t, rt.init())

			target := rt.filepath(rt.config.filenameProperties)
			require.NoError(t, os.WriteFile(target, tc.before, 0o644))

			// A second name for the file the target points at right now. Outside
			// the migration dir, so no listing over that dir has to skip it.
			witness := filepath.Join(lsm, "witness")
			require.NoError(t, os.Link(target, witness))
			witnessInfo, err := os.Stat(witness)
			require.NoError(t, err)

			require.NoError(t, tc.publish(rt, target, tc.after))

			published, err := os.ReadFile(target)
			require.NoError(t, err)
			require.Equal(t, tc.after, published, "the publish has to land the new content")

			kept, err := os.ReadFile(witness)
			require.NoError(t, err)
			targetInfo, err := os.Stat(target)
			require.NoError(t, err)

			if !tc.wantReplaced {
				require.Equal(t, tc.after, kept,
					"an in-place write reaches the old file, or the check below proves nothing")
				require.True(t, os.SameFile(witnessInfo, targetInfo))
				return
			}
			require.Equal(t, tc.before, kept,
				"the publish wrote through the old file, so a crash mid-write leaves it torn")
			require.False(t, os.SameFile(witnessInfo, targetInfo),
				"the target has to be a different file than the one it replaced")
		})
	}
}

func publishAtomic(rt *fileReindexTracker, _ string, content []byte) error {
	return rt.createFileAtomic(rt.config.filenameProperties, content)
}

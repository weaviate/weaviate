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

package file

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getIno(t *testing.T, path string) uint64 {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err)
	return info.Sys().(*syscall.Stat_t).Ino
}

func TestSafeStagingDirName(t *testing.T) {
	t.Run("deterministic", func(t *testing.T) {
		a := SafeStagingDirName(".backup-staging-", "backup1", "myclass")
		b := SafeStagingDirName(".backup-staging-", "backup1", "myclass")
		assert.Equal(t, a, b)
	})

	t.Run("golden value — byte-identical to the former db.safeSnapshotName", func(t *testing.T) {
		// Locks in the exact on-disk name so the extraction can't silently
		// change staging directory names across versions.
		got := SafeStagingDirName(".backup-staging-", "backup1", "myclass")
		assert.Equal(t, ".backup-staging-backup1-myclass-c55686e328f1", got)
	})

	t.Run("prefix prepended verbatim", func(t *testing.T) {
		name := SafeStagingDirName(".replica-staging-", "op-123", "MyClass")
		assert.True(t, strings.HasPrefix(name, ".replica-staging-"))
	})

	t.Run("long label truncated to SnapshotNameMaxLabel", func(t *testing.T) {
		longPart := strings.Repeat("x", 100)
		name := SafeStagingDirName(".p-", longPart)
		label := strings.TrimPrefix(name, ".p-")
		// strip the "-<12 hex>" suffix to isolate the truncated label
		truncated := label[:len(label)-13]
		assert.Len(t, truncated, SnapshotNameMaxLabel)
	})

	t.Run("short label left intact", func(t *testing.T) {
		name := SafeStagingDirName(".p-", "short")
		assert.True(t, strings.HasPrefix(name, ".p-short-"))
	})

	t.Run("unique suffix even when truncated labels collide", func(t *testing.T) {
		shared := strings.Repeat("a", SnapshotNameMaxLabel)
		a := SafeStagingDirName(".p-", shared+"-one")
		b := SafeStagingDirName(".p-", shared+"-two")
		assert.NotEqual(t, a, b, "distinct inputs must yield distinct names despite identical truncated labels")
	})

	t.Run("total length well within filesystem limits", func(t *testing.T) {
		name := SafeStagingDirName(".backup-staging-", strings.Repeat("x", 500), strings.Repeat("y", 500))
		assert.Less(t, len(name), 255)
	})
}

func TestProbeHardlinkSupport(t *testing.T) {
	t.Run("supported on a normal temp dir", func(t *testing.T) {
		dir := t.TempDir()
		assert.True(t, ProbeHardlinkSupport(dir))
	})

	t.Run("false for a non-existent dir", func(t *testing.T) {
		assert.False(t, ProbeHardlinkSupport(filepath.Join(t.TempDir(), "does-not-exist")))
	})

	t.Run("leaves no probe files behind", func(t *testing.T) {
		dir := t.TempDir()
		require.True(t, ProbeHardlinkSupport(dir))
		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		assert.Empty(t, entries, "probe must clean up its temp files")
	})
}

func TestHardlinkFiles(t *testing.T) {
	t.Run("hardlink shares the source inode", func(t *testing.T) {
		dir := t.TempDir()
		src := filepath.Join(dir, "src.db")
		require.NoError(t, os.WriteFile(src, []byte("segment data"), 0o644))
		dst := filepath.Join(dir, "staging", "src.db")

		require.NoError(t, HardlinkFiles([]HardlinkPair{{Src: src, Dst: dst}}))

		assert.Equal(t, getIno(t, src), getIno(t, dst), "hard-linked file must share the source inode")
		got, err := os.ReadFile(dst)
		require.NoError(t, err)
		assert.Equal(t, []byte("segment data"), got)
	})

	t.Run("creates missing nested parent dirs", func(t *testing.T) {
		dir := t.TempDir()
		src := filepath.Join(dir, "src")
		require.NoError(t, os.WriteFile(src, []byte("x"), 0o644))
		dst := filepath.Join(dir, "a", "b", "c", "file")

		require.NoError(t, HardlinkFiles([]HardlinkPair{{Src: src, Dst: dst}}))
		assert.FileExists(t, dst)
	})

	t.Run("links every pair in one call", func(t *testing.T) {
		dir := t.TempDir()
		var pairs []HardlinkPair
		for _, n := range []string{"one", "two", "three"} {
			src := filepath.Join(dir, n)
			require.NoError(t, os.WriteFile(src, []byte(n), 0o644))
			pairs = append(pairs, HardlinkPair{Src: src, Dst: filepath.Join(dir, "staging", n)})
		}
		require.NoError(t, HardlinkFiles(pairs))
		for _, p := range pairs {
			assert.Equal(t, getIno(t, p.Src), getIno(t, p.Dst))
		}
	})

	t.Run("source mutation is visible via the hardlink", func(t *testing.T) {
		dir := t.TempDir()
		src := filepath.Join(dir, "src")
		require.NoError(t, os.WriteFile(src, []byte("before"), 0o644))
		dst := filepath.Join(dir, "dst")
		require.NoError(t, HardlinkFiles([]HardlinkPair{{Src: src, Dst: dst}}))

		require.NoError(t, os.WriteFile(src, []byte("after!"), 0o644))
		got, err := os.ReadFile(dst)
		require.NoError(t, err)
		assert.Equal(t, []byte("after!"), got, "shared inode means src writes are visible via dst")
	})

	t.Run("empty slice is a no-op", func(t *testing.T) {
		assert.NoError(t, HardlinkFiles(nil))
	})

	t.Run("non-existent source errors, wrapped with dst", func(t *testing.T) {
		dir := t.TempDir()
		dst := filepath.Join(dir, "staging", "missing")
		err := HardlinkFiles([]HardlinkPair{{Src: filepath.Join(dir, "missing"), Dst: dst}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), dst)
	})
}

func TestCopyFile(t *testing.T) {
	t.Run("copy has an independent inode and equal content", func(t *testing.T) {
		dir := t.TempDir()
		src := filepath.Join(dir, "src")
		require.NoError(t, os.WriteFile(src, []byte("mutable data"), 0o644))
		dst := filepath.Join(dir, "dst")

		require.NoError(t, CopyFile(src, dst))

		assert.NotEqual(t, getIno(t, src), getIno(t, dst), "a copy must not share the source inode")
		got, err := os.ReadFile(dst)
		require.NoError(t, err)
		assert.Equal(t, []byte("mutable data"), got)
	})

	t.Run("source mutation does not affect the copy", func(t *testing.T) {
		dir := t.TempDir()
		src := filepath.Join(dir, "src")
		require.NoError(t, os.WriteFile(src, []byte("before"), 0o644))
		dst := filepath.Join(dir, "dst")
		require.NoError(t, CopyFile(src, dst))

		require.NoError(t, os.WriteFile(src, []byte("after!"), 0o644))
		got, err := os.ReadFile(dst)
		require.NoError(t, err)
		assert.Equal(t, []byte("before"), got, "the copy is independent of later src writes")
	})

	t.Run("non-existent source errors", func(t *testing.T) {
		dir := t.TempDir()
		err := CopyFile(filepath.Join(dir, "missing"), filepath.Join(dir, "dst"))
		require.Error(t, err)
	})

	t.Run("missing destination parent dir errors", func(t *testing.T) {
		dir := t.TempDir()
		src := filepath.Join(dir, "src")
		require.NoError(t, os.WriteFile(src, []byte("x"), 0o644))
		err := CopyFile(src, filepath.Join(dir, "no-such-dir", "dst"))
		require.Error(t, err, "CopyFile does not create dst's parent dir")
	})
}

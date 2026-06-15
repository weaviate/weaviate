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

package hnsw

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCorruptCommitLogFixer_Do(t *testing.T) {
	t.Run("keeps normal files without condensed counterpart", func(t *testing.T) {
		tmp := t.TempDir()
		f1 := filepath.Join(tmp, "commit1.log")
		require.Nil(t, os.WriteFile(f1, []byte("test"), 0o644))

		fixer := NewCorruptedCommitLogFixer()
		files, err := fixer.Do([]string{f1})
		require.Nil(t, err)
		require.Equal(t, []string{f1}, files)
	})

	t.Run("keeps .condensed files", func(t *testing.T) {
		tmp := t.TempDir()
		f := filepath.Join(tmp, "commit3.log.condensed")
		require.Nil(t, os.WriteFile(f, []byte("condensed"), 0o644))

		fixer := NewCorruptedCommitLogFixer()
		files, err := fixer.Do([]string{f})
		require.Nil(t, err)
		require.Equal(t, []string{f}, files)
	})

	t.Run("removes uncondensed file if condensed exists", func(t *testing.T) {
		tmp := t.TempDir()
		f := filepath.Join(tmp, "commit4.log")
		cf := filepath.Join(tmp, "commit4.log.condensed")
		require.Nil(t, os.WriteFile(f, []byte("uncondensed"), 0o644))
		require.Nil(t, os.WriteFile(cf, []byte("condensed"), 0o644))

		fixer := NewCorruptedCommitLogFixer()
		files, err := fixer.Do([]string{f, cf})
		require.Nil(t, err)
		require.Equal(t, []string{f}, files)
	})

	t.Run("keeps surviving files when a delete fails", func(t *testing.T) {
		tmp := t.TempDir()
		// commit5.log + commit5.log.condensed make the .condensed file corrupt,
		// so the fixer will try to delete it. Making it a non-empty directory
		// makes os.Remove fail, exercising the error path.
		twin := filepath.Join(tmp, "commit5.log")
		corrupt := filepath.Join(tmp, "commit5.log.condensed")
		tail := filepath.Join(tmp, "commit6.log")
		require.Nil(t, os.WriteFile(twin, []byte("twin"), 0o644))
		require.Nil(t, os.MkdirAll(corrupt, 0o755))
		require.Nil(t, os.WriteFile(filepath.Join(corrupt, "blocker"), []byte("x"), 0o644))
		require.Nil(t, os.WriteFile(tail, []byte("tail"), 0o644))

		fixer := NewCorruptedCommitLogFixer()
		files, err := fixer.Do([]string{twin, corrupt, tail})
		// the failed delete is reported...
		require.Error(t, err)
		// ...but the corrupt file is still excluded and the valid tail file
		// after it survives instead of being truncated away
		require.Equal(t, []string{twin, tail}, files)
	})
}

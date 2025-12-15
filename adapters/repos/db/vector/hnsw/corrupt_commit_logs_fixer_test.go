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
}

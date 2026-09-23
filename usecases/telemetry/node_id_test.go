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

package telemetry

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadOrCreateNodeID_FirstBootMintsAndPersists(t *testing.T) {
	dir := t.TempDir()

	id, err := ReadOrCreateNodeID(dir)
	require.NoError(t, err)
	assert.NotEmpty(t, id)

	path := filepath.Join(dir, nodeIDFileName)
	info, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())

	b, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, id, string(b))

	_, err = os.Stat(path + ".tmp")
	assert.True(t, os.IsNotExist(err), "the .tmp file must not survive a successful write")
}

func TestReadOrCreateNodeID_RestartStability(t *testing.T) {
	dir := t.TempDir()

	first, err := ReadOrCreateNodeID(dir)
	require.NoError(t, err)

	// Simulate N process restarts against the same data volume: every call
	// must return the id minted on the first boot, never a fresh one.
	for i := 0; i < 3; i++ {
		again, err := ReadOrCreateNodeID(dir)
		require.NoError(t, err)
		assert.Equal(t, first, again, "restart %d must read back the same nodeId", i)
	}
}

func TestReadOrCreateNodeID_DistinctDataDirsGetDistinctIDs(t *testing.T) {
	dirA, dirB := t.TempDir(), t.TempDir()

	idA, err := ReadOrCreateNodeID(dirA)
	require.NoError(t, err)
	idB, err := ReadOrCreateNodeID(dirB)
	require.NoError(t, err)

	assert.NotEqual(t, idA, idB)
}

func TestReadOrCreateNodeID_UnwritableDirReturnsError(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("requires non-root; root bypasses directory permission bits")
	}
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o555))
	defer os.Chmod(dir, 0o755) //nolint:errcheck

	_, err := ReadOrCreateNodeID(dir)
	assert.Error(t, err, "caller relies on this error to trigger its ephemeral-id fallback")
}

func TestReadOrCreateNodeID_ExistingFileContentVariants(t *testing.T) {
	tests := []struct {
		name        string
		fileContent string
		wantMinted  bool // true: content is treated as absent, a fresh UUID is minted
	}{
		{name: "plain uuid, no trailing whitespace", fileContent: "0195b6c0-0000-7000-8000-000000000001", wantMinted: false},
		{name: "uuid with trailing newline", fileContent: "0195b6c0-0000-7000-8000-000000000002\n", wantMinted: false},
		{name: "empty file", fileContent: "", wantMinted: true},
		{name: "whitespace-only file", fileContent: "   \n", wantMinted: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, nodeIDFileName)
			require.NoError(t, os.WriteFile(path, []byte(tt.fileContent), 0o600))

			id, err := ReadOrCreateNodeID(dir)
			require.NoError(t, err)
			assert.NotEmpty(t, id)

			if tt.wantMinted {
				assert.NotEqual(t, tt.fileContent, id)
			} else {
				assert.Equal(t, strings.TrimSpace(tt.fileContent), id)
			}
		})
	}
}

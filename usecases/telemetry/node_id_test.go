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
	"sync"
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

// TestReadOrCreateNodeID_ConcurrentFirstBoot pins the concurrent first-boot race:
// N goroutines call ReadOrCreateNodeID against one fresh, shared data dir at the
// same instant (the embedded-mode shape, where multiple instances default to one
// PERSISTENCE_DATA_PATH). Before the os.Link + read-back fix, this reproduced a
// ~75% caller-error rate (rename racing on a fixed tmp name) plus callers that
// succeeded but disagreed with the file another caller's rename had already
// overwritten. This test catches both failure modes because it asserts on the
// actual returned values from every goroutine against the actual on-disk content,
// not just that ReadOrCreateNodeID returns without error.
func TestReadOrCreateNodeID_ConcurrentFirstBoot(t *testing.T) {
	tests := []struct {
		name     string
		callers  int
		attempts int
	}{
		{name: "4 concurrent callers", callers: 4, attempts: 150},
		{name: "8 concurrent callers", callers: 8, attempts: 50},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for a := 0; a < tt.attempts; a++ {
				dir := t.TempDir()

				var wg sync.WaitGroup
				ids := make([]string, tt.callers)
				errs := make([]error, tt.callers)
				start := make(chan struct{})
				for i := 0; i < tt.callers; i++ {
					wg.Add(1)
					go func(i int) {
						defer wg.Done()
						<-start
						ids[i], errs[i] = ReadOrCreateNodeID(dir)
					}(i)
				}
				close(start)
				wg.Wait()

				for i, err := range errs {
					require.NoErrorf(t, err, "attempt %d caller %d: unexpected error", a, i)
				}

				onDiskBytes, err := os.ReadFile(filepath.Join(dir, nodeIDFileName))
				require.NoErrorf(t, err, "attempt %d: node-id file must exist after all callers return", a)
				onDisk := strings.TrimSpace(string(onDiskBytes))
				require.NotEmptyf(t, onDisk, "attempt %d: on-disk id must not be empty", a)

				for i, id := range ids {
					assert.Equalf(t, onDisk, id, "attempt %d caller %d: returned id must equal the on-disk id, never a locally-minted value that lost the race", a, i)
				}

				entries, err := os.ReadDir(dir)
				require.NoError(t, err)
				for _, e := range entries {
					assert.NotContainsf(t, e.Name(), ".tmp", "attempt %d: no leftover tmp file after all callers finish, got %q", a, e.Name())
				}
			}
		})
	}
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

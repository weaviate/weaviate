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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func TestNextCommitLogFileName(t *testing.T) {
	now := time.Now().Unix()

	tests := []struct {
		name        string
		current     string
		wantDerived bool
	}{
		{name: "log from an earlier second", current: fmt.Sprintf("%d", now-3600), wantDerived: true},
		{name: "log from the current second", current: fmt.Sprintf("%d", now), wantDerived: true},
		{name: "log timestamped ahead of the clock", current: fmt.Sprintf("%d", now+3600), wantDerived: true},
		{name: "malformed name", current: "not-a-number"},
		{name: "name carrying a suffix", current: fmt.Sprintf("%d.sorted", now)},
		{name: "empty name", current: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			nextTS, derived := nextCommitLogFileName(tc.current)
			require.Equal(t, tc.wantDerived, derived)

			if !derived {
				// nothing to advance past, but the name still has to be usable and
				// must not reopen the file that was just rotated out
				assert.NotEqual(t, tc.current, strconv.FormatInt(nextTS, 10))
				return
			}

			currentTS, err := strconv.ParseInt(tc.current, 10, 64)
			require.NoError(t, err)
			// a name that does not sort after the previous one reopens the log that
			// was just rotated out, and a backup only copies rotated-out logs
			assert.Greater(t, nextTS, currentTS)
		})
	}
}

// Several backups of the same shard starting at once switch the log several
// times inside one second. Each switch has to leave a file of its own behind:
// a switch that reused the name of the file it just rotated out would keep
// appending to it, and since ListFiles excludes the active file, everything
// that file already held would drop out of a backup taken right after.
func TestSwitchCommitLogsBurst(t *testing.T) {
	ctx := context.Background()
	rootDir := t.TempDir()
	id := "burst-switch"

	cl, err := NewCommitLogger(rootDir, id, logrus.New(), cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)

	const switches = 5
	for i := range switches {
		require.NoError(t, cl.AddNode(uint64(i), 0))
		require.NoError(t, cl.Flush())

		switched, err := cl.switchCommitLogs(true)
		require.NoError(t, err)
		require.True(t, switched)
	}
	require.NoError(t, cl.Shutdown(ctx))

	// one file per switch, plus the log the last switch opened; a switch that
	// reused the previous name would leave one file fewer
	entries, err := listRawCommitLogFiles(commitLogDirectory(rootDir, id))
	require.NoError(t, err)
	require.Len(t, entries, switches+1)

	for _, entry := range entries[:switches] {
		info, err := entry.Info()
		require.NoError(t, err)
		assert.NotZero(t, info.Size(), "%s: rotated-out log lost its writes", entry.Name())
	}
}

// A rotation must never append to a log that already exists under the name it
// picks: that log may end in a torn commit, and the rotated-in writes would be
// read misaligned behind it.
func TestSwitchCommitLogs_SkipsExistingNames(t *testing.T) {
	ctx := context.Background()
	rootDir := t.TempDir()
	id := "switch-existing"

	cl, err := NewCommitLogger(rootDir, id, logrus.New(), cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	require.NoError(t, cl.AddNode(1, 0))
	require.NoError(t, cl.Flush())

	dir := commitLogDirectory(rootDir, id)
	current, err := strconv.ParseInt(filepath.Base(cl.currentFileName), 10, 64)
	require.NoError(t, err)
	// occupy every name the rotation could pick, whether it follows the clock
	// or the current log
	existing := map[string]struct{}{}
	for _, base := range []int64{current, time.Now().Unix()} {
		for i := int64(1); i <= 3; i++ {
			existing[strconv.FormatInt(base+i, 10)] = struct{}{}
		}
	}
	existing[strconv.FormatInt(time.Now().Unix(), 10)] = struct{}{}
	delete(existing, filepath.Base(cl.currentFileName))
	for name := range existing {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("existing"), 0o644))
	}

	switched, err := cl.switchCommitLogs(true)
	require.NoError(t, err)
	require.True(t, switched)
	require.NotContains(t, existing, filepath.Base(cl.currentFileName))

	require.NoError(t, cl.AddNode(2, 0))
	require.NoError(t, cl.Flush())
	require.NoError(t, cl.Shutdown(ctx))

	for name := range existing {
		content, err := os.ReadFile(filepath.Join(dir, name))
		require.NoError(t, err)
		assert.Equal(t, "existing", string(content), "%s was written to", name)
	}
}

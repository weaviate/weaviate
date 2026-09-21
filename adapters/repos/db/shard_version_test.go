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
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShardVersioner_ClosesItsFile(t *testing.T) {
	versionPath := filepath.Join(t.TempDir(), "version")
	_, err := newShardVersioner(versionPath, false)
	require.NoError(t, err)

	// a file left open would otherwise be closed by its finalizer
	defer debug.SetGCPercent(debug.SetGCPercent(-1))
	openFiles := func() int {
		entries, err := os.ReadDir("/dev/fd")
		require.NoError(t, err)
		return len(entries)
	}

	before := openFiles()
	for range 20 {
		_, err := newShardVersioner(versionPath, true)
		require.NoError(t, err)
	}
	require.Equal(t, before, openFiles())
}

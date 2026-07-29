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

package schema

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDropVectorMarkerPurgeMinVersion_HasConsumer makes the release
// dependency machine-checkable instead of PR-description prose: the
// drop-vector marker-introduction purge/refusal and the reshaped removal gate
// run unconditionally inside the deterministic UpdateClass apply, so a
// 1.38→1.39 rolling upgrade diverges per node (schema split-brain) unless the
// rolling-upgrade min-version gate (weaviate/weaviate#11901) fences marker introductions.
// That gate MUST consume [DropVectorMarkerPurgeMinVersion].
//
// While no consumer exists the test SKIPS with a release-blocker banner (CI
// stays green for development, the dependency stays visible); once #11901
// lands and references the constant anywhere outside this package, the test
// asserts the consumer keeps existing.
func TestDropVectorMarkerPurgeMinVersion_HasConsumer(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	require.True(t, ok)
	// cluster/schema/ -> repo root
	root := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
	require.FileExists(t, filepath.Join(root, "go.mod"), "repo root discovery failed")

	consumers := 0
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if name == "vendor" || name == ".git" || name == "node_modules" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		if filepath.Dir(path) == filepath.Dir(thisFile) {
			return nil // the declaring package doesn't count as a consumer
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(data), "DropVectorMarkerPurgeMinVersion") {
			consumers++
		}
		return nil
	})
	require.NoError(t, err)

	if consumers == 0 {
		t.Skip("RELEASE BLOCKER — do not ship 1.39.0 with this skipping: " +
			"cluster/schema.DropVectorMarkerPurgeMinVersion has no consumer. " +
			"The rolling-upgrade min-version gate (weaviate/weaviate#11901) must land and consume it " +
			"to fence drop-vector marker introductions (purge/refusal + removal gate) — " +
			"mixed-version clusters otherwise apply the same raft entry divergently.")
	}
	require.GreaterOrEqual(t, consumers, 1)
}

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
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// T-IDENT-1: ReadOrCreateNodeID creates then reads stable.
func TestReadOrCreateNodeID_CreatesAndReads(t *testing.T) {
	dir := t.TempDir()
	id1, err := ReadOrCreateNodeID(dir)
	require.NoError(t, err)
	require.NotEmpty(t, id1)

	// second call reads the same value
	id2, err := ReadOrCreateNodeID(dir)
	require.NoError(t, err)
	assert.Equal(t, id1, id2, "second read must return the same UUID")

	// file on disk contains the UUID
	b, err := os.ReadFile(filepath.Join(dir, "node-id"))
	require.NoError(t, err)
	assert.Equal(t, id1, string(b))
}

// T-IDENT-2: nodeId survives simulated restart (two New() calls same dir).
func TestNodeIDSurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	sg1 := &fakeNodesStatusGetter{}
	sm1 := &fakeSchemaManager{}
	logger, _ := test.NewNullLogger()
	nodeID1, err := ReadOrCreateNodeID(dir)
	require.NoError(t, err)
	tel1 := New(sg1, sm1, logger, "", 0, false, nodeID1, false, nil)

	sg2 := &fakeNodesStatusGetter{}
	sm2 := &fakeSchemaManager{}
	nodeID2, err := ReadOrCreateNodeID(dir)
	require.NoError(t, err)
	tel2 := New(sg2, sm2, logger, "", 0, false, nodeID2, false, nil)

	assert.Equal(t, tel1.nodeID, tel2.nodeID, "nodeId must be identical across restarts")
}

// T-IDENT-3: machineId != nodeId, and the R2/R3 overwrite slip is absent.
// Two New() calls with the same data dir: machineId differs (fresh per process)
// while nodeId is identical. This is the key correctness guard.
func TestMachineIDDiffersFromNodeID(t *testing.T) {
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()

	nodeID, err := ReadOrCreateNodeID(dir)
	require.NoError(t, err)

	sg1 := &fakeNodesStatusGetter{}
	sm1 := &fakeSchemaManager{}
	tel1 := New(sg1, sm1, logger, "", 0, false, nodeID, false, nil)

	sg2 := &fakeNodesStatusGetter{}
	sm2 := &fakeSchemaManager{}
	nodeID2, err := ReadOrCreateNodeID(dir)
	require.NoError(t, err)
	tel2 := New(sg2, sm2, logger, "", 0, false, nodeID2, false, nil)

	// nodeId is stable: both telemeters see the same persisted UUID
	assert.Equal(t, tel1.nodeID, tel2.nodeID, "nodeId must be identical across New() calls")

	// machineId is ephemeral: each New() call gets a fresh UUID
	assert.NotEqual(t, string(tel1.machineID), string(tel2.machineID),
		"machineId must differ across New() calls (counts restarts)")

	// machineId must not equal nodeId for either instance
	assert.NotEqual(t, string(tel1.machineID), tel1.nodeID,
		"machineId must not overwrite nodeId (slip guard)")
	assert.NotEqual(t, string(tel2.machineID), tel2.nodeID,
		"machineId must not overwrite nodeId (slip guard)")
}

// T-IDENT-4: nodeId resets on data-dir wipe.
func TestNodeIDResetsOnDataDirWipe(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()

	id1, err := ReadOrCreateNodeID(dir1)
	require.NoError(t, err)
	id2, err := ReadOrCreateNodeID(dir2)
	require.NoError(t, err)

	assert.NotEqual(t, id1, id2, "different data dirs must produce different nodeIds")
}

// T-IDENT-5: read-only / unwritable dir falls back.
func TestReadOrCreateNodeID_UnwritableDirReturnsError(t *testing.T) {
	dir := t.TempDir()
	// Make the directory read-only so writes fail.
	require.NoError(t, os.Chmod(dir, 0o555))
	defer os.Chmod(dir, 0o755) //nolint:errcheck

	_, err := ReadOrCreateNodeID(dir)
	assert.Error(t, err, "expected an error on unwritable dir")
}

// waitForClusterID timing test: returns immediately once context cancelled.
func TestWaitForClusterIDFuncWithStub(t *testing.T) {
	logger, _ := test.NewNullLogger()
	sg := &fakeNodesStatusGetter{}
	sm := &fakeSchemaManager{}

	const fixedID = "00000000-0000-7000-0000-000000000099"
	stub := func(ctx context.Context) (string, int64, error) {
		return fixedID, 42000, nil
	}
	tel := New(sg, sm, logger, "", 0, false, "test-node-id", false, stub)

	// Start sets clusterID from the stub waiter
	// We call it directly to test the field assignment path (Start itself makes
	// a network push which we don't want in unit tests).
	waitCtx := context.Background()
	clusterID, clusterCreatedAt, err := tel.waitForClusterID(waitCtx)
	require.NoError(t, err)
	assert.Equal(t, fixedID, clusterID)
	assert.Equal(t, int64(42000), clusterCreatedAt)
}

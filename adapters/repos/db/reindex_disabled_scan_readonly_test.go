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
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

// migrationState is one on-disk stop-phase of a runtime reindex, named by
// the sentinels present in its tracker dir.
type migrationState struct {
	dirName   string
	sentinels []string
	// discoverable records whether the recovery scan is expected to
	// reconstruct a task from this state.
	discoverable bool
}

// migrationStopPhases covers the four ways a migration can be sitting on
// disk when a node boots. merged-but-not-tidied is called out separately
// because that is the state whose finalize path was writable.
func migrationStopPhases() []migrationState {
	return []migrationState{
		{
			dirName:   "filterable_enable_text_1",
			sentinels: []string{"started.mig"},
		},
		{
			dirName:      "filterable_enable_body_2",
			sentinels:    []string{"started.mig", "reindexed.mig", "merged.mig"},
			discoverable: true,
		},
		{
			dirName:   "filterable_enable_title_3",
			sentinels: []string{"started.mig", "reindexed.mig", "merged.mig", "swapped.mig", "tidied.mig"},
		},
		{
			dirName:   "filterable_enable_abstract_4",
			sentinels: []string{"started.mig", "cancelled.mig"},
		},
	}
}

// plantRecoveryLayout writes every stop phase from [migrationStopPhases]
// into one shard, plus the ingest dir each would swap in. Returns the data
// root the scan is pointed at.
func plantRecoveryLayout(t *testing.T, collection, shardName string) string {
	t.Helper()

	rootPath := t.TempDir()
	lsmPath := filepath.Join(rootPath, strings.ToLower(collection), shardName, "lsm")
	migrationsDir := filepath.Join(lsmPath, ".migrations")

	for _, phase := range migrationStopPhases() {
		migDir := filepath.Join(migrationsDir, phase.dirName)
		require.NoError(t, os.MkdirAll(migDir, 0o755))
		for _, s := range phase.sentinels {
			require.NoError(t, os.WriteFile(filepath.Join(migDir, s), nil, 0o644))
		}

		rec := reindexRecoveryRecord{
			TaskID:      "task-" + phase.dirName,
			TaskVersion: 7,
			UnitID:      shardName + "__node1",
			Payload: ReindexTaskPayload{
				MigrationType: ReindexTypeEnableFilterable,
				Collection:    collection,
				Properties:    []string{"body"},
				UnitToShard:   map[string]string{shardName + "__node1": shardName},
				UnitToNode:    map[string]string{shardName + "__node1": "node1"},
			},
		}
		blob, err := json.Marshal(rec)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(
			filepath.Join(migDir, reindexRecoveryPayloadFile), blob, 0o644))

		ingestDir := filepath.Join(lsmPath, "property_body__enable_filterable_ingest_1")
		require.NoError(t, os.MkdirAll(ingestDir, 0o755))
		require.NoError(t, os.WriteFile(
			filepath.Join(ingestDir, "segment.db"), []byte("ingest-"+phase.dirName), 0o644))
	}

	return rootPath
}

// TestDiscoverInFlightReindexTasks_IsReadOnly pins the property that makes
// the flag-off startup WARN safe: the scan behind it looks at every
// migration stop phase on disk, including merged-but-not-tidied, and
// writes nothing.
//
// With runtime reindex off this scan is the ONLY thing that touches
// migration state at boot, so "it only reads" is load-bearing rather than
// incidental — the finalize path in the same area was writable until this
// PR gated it.
//
// The discovery assertion keeps the byte-identity assertion honest: a scan
// that silently found nothing would trivially write nothing.
func TestDiscoverInFlightReindexTasks_IsReadOnly(t *testing.T) {
	const collection, shardName = "ResidueClass", "shard1"

	rootPath := plantRecoveryLayout(t, collection, shardName)
	before := snapshotTree(t, rootPath)

	logger, _ := test.NewNullLogger()
	recovered, err := DiscoverInFlightReindexTasks(rootPath, logger, nil)
	require.NoError(t, err)

	var wantDiscovered int
	for _, phase := range migrationStopPhases() {
		if phase.discoverable {
			wantDiscovered++
		}
	}
	require.Len(t, recovered, wantDiscovered,
		"the scan must actually walk the layout, otherwise byte-identity proves nothing")
	require.Equal(t, collection, recovered[0].Collection)
	require.Equal(t, shardName, recovered[0].ShardName)

	require.Equal(t, before, snapshotTree(t, rootPath),
		"the flag-off startup scan must not write, rename, or delete anything")
}

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
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestLegacyMarkerMigrationSurvivesTheSweep covers the upgrade path: a node
// running the release before the migration records marked a completed
// migration with a file in its tracker dir, and its staged directory is the
// property's only copy. This build writes records instead, so without the
// marker in the preserve predicate the first sweep deletes that copy.
//
// The Warn is the other half. This build cannot promote marker-era state, and
// the schema flip it belongs to already committed cluster-wide, so the
// property answers from an empty bucket and nothing else says so.
func TestLegacyMarkerMigrationSurvivesTheSweep(t *testing.T) {
	tests := []struct {
		name string
		// propName is the property the marker-era migration covers. Only
		// classProp has a canonical bucket dir on disk.
		propName string
		marker   string
		wantDirs bool
		wantWarn bool
	}{
		{
			name:     "the canonical bucket is gone, so the staged copy is the only one",
			propName: "gone",
			marker:   "merged.mig",
			wantDirs: true,
			wantWarn: true,
		},
		{
			name:     "the other marker name counts too",
			propName: "gone",
			marker:   "tidied.mig",
			wantDirs: true,
			wantWarn: true,
		},
		{
			// Preserved on the same evidence, but the property still serves
			// its own bucket, so there is nothing for an operator to act on.
			name:     "the canonical bucket is still there",
			propName: "category",
			marker:   "merged.mig",
			wantDirs: true,
			wantWarn: false,
		},
		{
			name:     "the same tracker without a marker is stale state",
			propName: "gone",
			wantDirs: false,
			wantWarn: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "LegacyMarker_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{"category"})
			shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(ctx)
			lsm := shard.pathLSM()

			tracker := "enable_filterable_" + tc.propName + "_1"
			ingest := "property_" + tc.propName + "__enable_filterable_ingest_1"
			reindex := "property_" + tc.propName + "__enable_filterable_reindex_1"

			mkTrackerDir(t, lsm, tracker)
			mkRecoveryPayload(t, lsm, tracker, tc.propName)
			mkSidecarDir(t, lsm, ingest)
			mkSidecarDir(t, lsm, reindex)
			if tc.marker != "" {
				require.NoError(t, os.WriteFile(
					filepath.Join(lsm, migrationsDir, tracker, tc.marker), nil, 0o600))
			}

			logger, ok := shard.index.logger.(*logrus.Logger)
			require.True(t, ok, "the shard's logger must be hookable for the Warn assertion")
			hook := test.NewLocal(logger)

			// The load path is what has to surface this: reconciliation is
			// record-driven and says nothing about a directory no record names.
			shard.reconcileMigrationRecords(ctx, class)
			require.Equal(t, tc.wantWarn, legacyMarkerWarned(hook, tc.propName),
				"a warning naming %q on the shard load path", tc.propName)

			cleanSweep(t, ctx, shard, tc.propName, "filterable")

			require.Equal(t, tc.wantDirs, dirExistsAt(t, filepath.Join(lsm, migrationsDir), tracker),
				"tracker dir %s", tracker)
			require.Equal(t, tc.wantDirs, dirExistsAt(t, lsm, ingest),
				"ingest sidecar %s, which holds the data", ingest)
			require.Equal(t, tc.wantDirs, dirExistsAt(t, lsm, reindex),
				"reindex sidecar %s", reindex)
		})
	}
}

func legacyMarkerWarned(hook *test.Hook, propName string) bool {
	for _, entry := range hook.AllEntries() {
		if entry.Level != logrus.WarnLevel || !strings.Contains(entry.Message, "serve empty") {
			continue
		}
		props, ok := entry.Data["properties"].([]string)
		if ok && len(props) == 1 && props[0] == propName {
			return true
		}
	}
	return false
}

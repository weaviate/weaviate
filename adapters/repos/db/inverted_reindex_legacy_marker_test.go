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
//
// The payload is the only thing that names those directories, so a payload
// that cannot be read is the same loss with nothing left to name: the sweep
// has to withhold on the whole shard instead.
func TestLegacyMarkerMigrationSurvivesTheSweep(t *testing.T) {
	tests := []struct {
		name string
		// propName is the property the marker-era migration covers. Only
		// classProp has a canonical bucket dir on disk.
		propName string
		marker   string
		// rawPayload replaces the well-formed payload.mig this tracker would
		// otherwise carry.
		rawPayload string
		// unlistableMigrations takes the read bit off .migrations while
		// leaving it traversable, so the records directory underneath still
		// answers and only the marker-era scan fails.
		unlistableMigrations bool
		wantDirs             bool
		wantWarn             bool
		wantUnreadableWarn   bool
		wantWithholdWarn     bool
		wantSweepErr         bool
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
		{
			// Nothing names the sidecars, so preserving the tracker alone
			// would hand the only copy of the data to the reclaimers.
			name:               "a payload that cannot be parsed withholds the whole shard",
			propName:           "gone",
			marker:             "merged.mig",
			rawPayload:         `{"payload": {"properties": [`,
			wantDirs:           true,
			wantUnreadableWarn: true,
		},
		{
			// A property name is joined onto the shard root and the result is
			// handed to a recursive delete, and unlike a record's names these
			// never passed the decoder's validation.
			name:               "a payload naming a property that escapes the shard is not read as a property list",
			propName:           "gone",
			marker:             "merged.mig",
			rawPayload:         `{"payload": {"properties": ["../../../etc"]}}`,
			wantDirs:           true,
			wantUnreadableWarn: true,
		},
		{
			// The scan that finds marker-era trackers reads .migrations; the
			// sidecar sweep reads the LSM root and succeeds on the same
			// fault. A listing nobody could read must therefore withhold, or
			// the sweep takes the only copy on the evidence of an empty set
			// it never built.
			name:                 "a migration directory that cannot be listed withholds the whole shard",
			propName:             "gone",
			marker:               "merged.mig",
			unlistableMigrations: true,
			wantDirs:             true,
			wantWithholdWarn:     true,
			wantSweepErr:         true,
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
			if tc.rawPayload != "" {
				require.NoError(t, os.WriteFile(
					filepath.Join(lsm, migrationsDir, tracker, reindexRecoveryPayloadFile),
					[]byte(tc.rawPayload), 0o644))
			} else {
				mkRecoveryPayload(t, lsm, tracker, tc.propName)
			}
			mkSidecarDir(t, lsm, ingest)
			mkSidecarDir(t, lsm, reindex)
			if tc.marker != "" {
				require.NoError(t, os.WriteFile(
					filepath.Join(lsm, migrationsDir, tracker, tc.marker), nil, 0o600))
			}

			migrations := filepath.Join(lsm, migrationsDir)
			if tc.unlistableMigrations {
				// Traversable but not readable, so .migrations/records still
				// answers and the record set stays clean. That is the state
				// an upgrading shard is in: it has marker-era trackers and no
				// records directory at all.
				require.NoError(t, os.Chmod(migrations, 0o111))
				t.Cleanup(func() { os.Chmod(migrations, 0o755) })
				if _, err := os.ReadDir(migrations); err == nil {
					t.Skip("this user can list an unreadable directory, so the failure cannot be staged")
				}
			}

			logger, ok := shard.index.logger.(*logrus.Logger)
			require.True(t, ok, "the shard's logger must be hookable for the Warn assertion")
			hook := test.NewLocal(logger)

			// The load path is what has to surface this: reconciliation is
			// record-driven and says nothing about a directory no record names.
			shard.reconcileMigrationRecords(ctx, class)
			require.Equal(t, tc.wantWarn, legacyMarkerWarned(hook, tc.propName),
				"a warning naming %q on the shard load path", tc.propName)
			require.Equal(t, tc.wantUnreadableWarn, warnedContaining(hook, "cannot read"),
				"a warning that the tracker's properties could not be read")
			require.Equal(t, tc.wantWithholdWarn, warnedContaining(hook, "could not be listed"),
				"a warning that the migration directory could not be listed")

			if tc.wantSweepErr {
				_, err := shard.CleanStalePartialReindexState(ctx, tc.propName, "filterable")
				require.Error(t, err, "a sweep that could not read the directory must not report success")
				require.NoError(t, os.Chmod(migrations, 0o755))
			} else {
				cleanSweep(t, ctx, shard, tc.propName, "filterable")
			}

			require.Equal(t, tc.wantDirs, dirExistsAt(t, migrations, tracker),
				"tracker dir %s", tracker)
			require.Equal(t, tc.wantDirs, dirExistsAt(t, lsm, ingest),
				"ingest sidecar %s, which holds the data", ingest)
			require.Equal(t, tc.wantDirs, dirExistsAt(t, lsm, reindex),
				"reindex sidecar %s", reindex)
		})
	}
}

// warnedContaining reports a Warn whose message carries want.
func warnedContaining(hook *test.Hook, want string) bool {
	for _, entry := range hook.AllEntries() {
		if entry.Level == logrus.WarnLevel && strings.Contains(entry.Message, want) {
			return true
		}
	}
	return false
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

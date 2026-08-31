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
	"strconv"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

func TestBuildReindexTasksGenerationAllocation(t *testing.T) {
	const propName = "title"

	tests := []struct {
		name        string
		dirs        []string
		recordGen   int
		undecodable bool
		unlistable  bool
		rehydrate   bool

		wantErr bool
		wantDir string
	}{
		{
			name:    "a fresh task on an empty shard claims the first generation",
			wantDir: "enable_filterable_title_1",
		},
		{
			name:    "a fresh task steps over the highest directory",
			dirs:    []string{"enable_filterable_title_2"},
			wantDir: "enable_filterable_title_3",
		},
		{
			name:      "a fresh task steps over a generation only a record still claims",
			dirs:      []string{"enable_filterable_title_2"},
			recordGen: 5,
			wantDir:   "enable_filterable_title_6",
		},
		{
			name:      "a rehydrate re-adopts the generation a record claims, not an older directory's",
			dirs:      []string{"enable_filterable_title_2"},
			recordGen: 5,
			rehydrate: true,
			wantDir:   "enable_filterable_title_5",
		},
		{
			name:      "a rehydrate re-adopts the highest directory when no record claims more",
			dirs:      []string{"enable_filterable_title_2"},
			rehydrate: true,
			wantDir:   "enable_filterable_title_2",
		},
		{
			name:      "a rehydrate with nothing claimed instantiates nothing",
			rehydrate: true,
		},
		{
			name:        "a record nobody can read refuses a fresh allocation",
			dirs:        []string{"enable_filterable_title_2"},
			undecodable: true,
			wantErr:     true,
		},
		{
			name:        "a record nobody can read refuses a rehydrate too",
			dirs:        []string{"enable_filterable_title_2"},
			undecodable: true,
			rehydrate:   true,
			wantErr:     true,
		},
		{
			name:       "a tracker directory nobody can list refuses a fresh allocation",
			dirs:       []string{"enable_filterable_title_2"},
			unlistable: true,
			wantErr:    true,
		},
		{
			name:       "a tracker directory nobody can list refuses a rehydrate too",
			dirs:       []string{"enable_filterable_title_2"},
			unlistable: true,
			rehydrate:  true,
			wantErr:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lsmPath := fakeMigrationsDir(t, tc.dirs)
			logger, _ := test.NewNullLogger()

			if tc.recordGen > 0 {
				subject := testMigrationSubject(42, StrategyCodeEnableFilterable, propName)
				subject.TrackerDir = "enable_filterable_" + propName + "_" + strconv.Itoa(tc.recordGen)
				require.NoError(t, NewMigrationRecordStore(lsmPath, logger).
					Put(NewMigrationRecordMerged(subject)))
			}
			if tc.undecodable {
				recordsDir := filepath.Join(lsmPath, migrationsDir, migrationRecordsDirName)
				require.NoError(t, os.MkdirAll(recordsDir, 0o755))
				require.NoError(t, os.WriteFile(
					filepath.Join(recordsDir, "99_enable_filterable.json"), []byte("{"), 0o600))
			}

			if tc.unlistable {
				makeMigrationsUnlistable(t, lsmPath)
			}

			p := &ReindexProvider{logger: logger}
			tasks, err := p.buildReindexTasks(&ReindexTaskPayload{
				MigrationType: ReindexTypeEnableFilterable,
				Collection:    "Docs",
				Properties:    []string{propName},
			}, lsmPath, tc.rehydrate)

			if tc.wantErr {
				require.Error(t, err, "a shard whose migration state is partly invisible must not be allocated from")
				return
			}
			require.NoError(t, err)
			if tc.wantDir == "" {
				require.Empty(t, tasks, "nothing is claimed, so nothing may be instantiated")
				return
			}
			require.Len(t, tasks, 1)
			require.Equal(t, tc.wantDir, tasks[0].strategy.MigrationDirName())
		})
	}
}

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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// entriesAbout returns every log entry whose message carries needle.
func entriesAbout(hook *logrustest.Hook, needle string) []*logrus.Entry {
	var out []*logrus.Entry
	for _, entry := range hook.AllEntries() {
		if strings.Contains(entry.Message, needle) {
			out = append(out, entry)
		}
	}
	return out
}

// linesOf renders entries for a failure message: logrus entries are pointers,
// so asserting on them prints addresses instead of the lines.
func linesOf(entries []*logrus.Entry) []string {
	out := make([]string, len(entries))
	for i, entry := range entries {
		out[i] = fmt.Sprintf("%s %v", entry.Message, entry.Data)
	}
	return out
}

// One line per fault kind for the whole walk, with its count, its reason and
// capped names: a line per shard or record follows the tenant count at every boot.
func TestRecoveryWalkReportsEachFaultKindOnce(t *testing.T) {
	const shards = 12
	fixtureLogger, _ := logrustest.NewNullLogger()

	tests := []struct {
		name string
		seed func(t *testing.T, i int, lsm string)
		// about picks the walk's line; names is the field carrying its capped names.
		about, names    string
		wantText        []string
		unreadableLines int
	}{
		{
			name: "a record recovery cannot build a task from",
			seed: func(t *testing.T, i int, lsm string) {
				require.NoError(t, os.MkdirAll(lsm, 0o777))
				subject := testMigrationSubject(uint64(i+1), StrategyCodeEnableSearchable, "title")
				subject.MigrationType, subject.TargetTokenization = ReindexTypeEnableSearchable, ""
				require.NoError(t, NewMigrationRecordStore(lsm, fixtureLogger).Put(NewMigrationRecordMerged(subject)))
			},
			about: "builds no reindex task", names: "records",
			wantText: []string{fmt.Sprintf("%d migration(s)", shards)},
		},
		{
			name: "a record set that cannot be read",
			seed: func(t *testing.T, _ int, lsm string) {
				require.NoError(t, os.MkdirAll(filepath.Join(lsm, migrationsDir), 0o777))
				require.NoError(t, os.WriteFile(filepath.Join(lsm, migrationsDir, migrationRecordsDirName), nil, 0o600))
			},
			about: "the migration records of", names: "shards",
			wantText: []string{
				fmt.Sprintf("%d shard(s)", shards), "read migration records dir",
				fmt.Sprintf("(and %d more)", shards-maxReportedErrors),
			},
			unreadableLines: 1,
		},
		{
			name: "one record that cannot be read",
			seed: func(t *testing.T, _ int, lsm string) {
				recordsDir := filepath.Join(lsm, migrationsDir, migrationRecordsDirName)
				require.NoError(t, os.MkdirAll(recordsDir, 0o777))
				require.NoError(t, os.WriteFile(filepath.Join(recordsDir, "searchable_retokenize_title_1.json"),
					[]byte("not json"), 0o600))
			},
			about: "some migration records of", names: "shards",
			wantText:        []string{fmt.Sprintf("%d shard(s)", shards)},
			unreadableLines: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			for i := 0; i < shards; i++ {
				tt.seed(t, i, filepath.Join(root, "books_abc", fmt.Sprintf("tenant-%02d", i), "lsm"))
			}

			logger, hook := logrustest.NewNullLogger()
			recovered, err := DiscoverInFlightReindexTasks(root, true, logger, nil)
			require.NoError(t, err)
			require.Empty(t, recovered)

			about := entriesAbout(hook, tt.about)
			require.Len(t, about, 1, "one line for the whole walk: %v", linesOf(about))
			require.Equal(t, logrus.WarnLevel, about[0].Level)
			for _, want := range tt.wantText {
				require.Contains(t, about[0].Message, want)
			}
			names, ok := about[0].Data[tt.names].([]string)
			require.True(t, ok, "the line carries the names it counted")
			require.Len(t, names, maxReportedErrors+1,
				"the capped names plus the one entry that says how many are unaccounted for")
			require.Contains(t, names[len(names)-1], fmt.Sprintf("and %d more", shards-maxReportedErrors))

			var unreadable []*logrus.Entry
			for _, entry := range entriesAbout(hook, "reindex recovery:") {
				if strings.Contains(entry.Message, "could not be read") {
					unreadable = append(unreadable, entry)
				}
			}
			require.Len(t, unreadable, tt.unreadableLines,
				"a shard is reported as wholly or partly unreadable, never both: %v", linesOf(unreadable))
		})
	}
}

// The apply's sweep summary must print: record_set_reads is where a
// once-per-shard regression shows up.
func TestUpdatePropertySummaryCountsRecordSetReads(t *testing.T) {
	ctx := testCtx()
	className := "SweepSummaryRecordReads_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{"title"})

	logger, hook := logrustest.NewNullLogger()
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false, func(i *Index) { i.logger = logger })
	shard := shd.(*Shard)
	defer shard.Shutdown(context.Background())

	prop := class.Properties[0]
	off := false
	prop.IndexFilterable = &off
	prop.IndexSearchable = &off
	prop.IndexRangeFilters = &off

	hook.Reset()
	require.NoError(t, idx.updateProperty(ctx, prop))

	about := entriesAbout(hook, "partial-reindex cleanup: migration dirs swept for disabled index types")
	require.Len(t, about, 1, "one summary line for the apply: %v", linesOf(about))
	require.GreaterOrEqual(t, about[0].Data["record_set_reads"], int64(1),
		"the summary reports the record-set read the sweep still paid")
}

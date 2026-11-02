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

// One line per fault kind per shard, not per record.
func TestRecoveryReportsEachFaultKindOnce(t *testing.T) {
	const records = 12
	fixtureLogger, _ := logrustest.NewNullLogger()

	tests := []struct {
		name            string
		seed            func(t *testing.T, lsm string)
		about           string
		wantText        []string
		unreadableLines int
	}{
		{
			name: "records recovery cannot build a task from",
			seed: func(t *testing.T, lsm string) {
				require.NoError(t, os.MkdirAll(lsm, 0o777))
				for i := 0; i < records; i++ {
					subject := testMigrationSubject(uint64(i+1), StrategyCodeEnableSearchable, "title")
					subject.MigrationType, subject.TargetTokenization = ReindexTypeEnableSearchable, ""
					require.NoError(t, NewMigrationRecordStore(lsm, fixtureLogger).Put(NewMigrationRecordMerged(subject)))
				}
			},
			about:    "builds no reindex task",
			wantText: []string{fmt.Sprintf("%d migration(s)", records), fmt.Sprintf("and %d more", records-maxReportedErrors)},
		},
		{
			name: "a record set that cannot be read",
			seed: func(t *testing.T, lsm string) {
				require.NoError(t, os.MkdirAll(filepath.Join(lsm, migrationsDir), 0o777))
				require.NoError(t, os.WriteFile(filepath.Join(lsm, migrationsDir, migrationRecordsDirName), nil, 0o600))
			},
			about:           "the migration records could not be read",
			wantText:        []string{"read migration records dir"},
			unreadableLines: 1,
		},
		{
			name: "one record that cannot be read",
			seed: func(t *testing.T, lsm string) {
				recordsDir := filepath.Join(lsm, migrationsDir, migrationRecordsDirName)
				require.NoError(t, os.MkdirAll(recordsDir, 0o777))
				require.NoError(t, os.WriteFile(filepath.Join(recordsDir, "searchable_retokenize_title_1.json"),
					[]byte("not json"), 0o600))
			},
			about:           "some migration records could not be read",
			unreadableLines: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lsm := filepath.Join(t.TempDir(), "books_abc", "tenant-00", "lsm")
			tt.seed(t, lsm)

			logger, hook := logrustest.NewNullLogger()
			recovered := NewMigrationRecoveryReindexer(true, logger, nil).
				recoverFromStore(NewMigrationRecordStore(lsm, logger), "tenant-00")
			require.Empty(t, recovered)

			about := entriesAbout(hook, tt.about)
			require.Len(t, about, 1, "one line for the shard: %v", linesOf(about))
			require.Equal(t, logrus.WarnLevel, about[0].Level)
			for _, want := range tt.wantText {
				require.Contains(t, about[0].Message, want)
			}

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

// A fault every tenant shares, such as a failing disk, must not log once per shard load.
func TestRecoveryCapsFaultLinesAcrossShards(t *testing.T) {
	const shards = maxReportedErrors + 5
	logger, hook := logrustest.NewNullLogger()
	recovery := NewMigrationRecoveryReindexer(true, logger, nil)
	for i := 0; i < shards; i++ {
		lsm := filepath.Join(t.TempDir(), "books_abc", fmt.Sprintf("tenant-%02d", i), "lsm")
		require.NoError(t, os.MkdirAll(filepath.Join(lsm, migrationsDir), 0o777))
		require.NoError(t, os.WriteFile(filepath.Join(lsm, migrationsDir, migrationRecordsDirName), nil, 0o600))
		recovery.recoverFromStore(NewMigrationRecordStore(lsm, logger), fmt.Sprintf("tenant-%02d", i))
	}
	about := entriesAbout(hook, "the migration records could not be read")
	require.Len(t, about, maxReportedErrors, "%v", linesOf(about))
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

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
	"time"

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
		name     string
		seed     func(t *testing.T, lsm string)
		about    string
		wantText []string
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
			name: "one record that cannot be read",
			seed: func(t *testing.T, lsm string) {
				plantUnreadableRecord(t, filepath.Join(lsm, migrationsDir, migrationRecordsDirName))
			},
			about: "some migration records could not be read",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lsm := filepath.Join(t.TempDir(), "books_abc", "tenant-00", "lsm")
			tt.seed(t, lsm)

			logger, hook := logrustest.NewNullLogger()
			store := NewMigrationRecordStore(lsm, logger)
			require.NoError(t, store.Load())
			recovered := NewMigrationRecoveryReindexer(true, logger, nil, nil).recoverFromStore(store, "tenant-00")
			require.Empty(t, recovered)

			about := entriesAbout(hook, tt.about)
			require.Len(t, about, 1, "one line for the shard: %v", linesOf(about))
			require.Equal(t, logrus.WarnLevel, about[0].Level)
			for _, want := range tt.wantText {
				require.Contains(t, about[0].Message, want)
			}
		})
	}
}

// A fault every tenant shares, such as a failing disk, must not log once per
// shard load, must not hide another kind, and must say how much it dropped.
func TestRecoveryCapsFaultLinesAcrossShards(t *testing.T) {
	const dropped = 5
	logger, hook := logrustest.NewNullLogger()
	recovery := NewMigrationRecoveryReindexer(true, logger, nil, nil)
	recoverShard := func(seed func(lsm string)) {
		lsm := filepath.Join(t.TempDir(), "books_abc", "tenant", "lsm")
		seed(lsm)
		store := NewMigrationRecordStore(lsm, logger)
		require.NoError(t, store.Load())
		recovery.recoverFromStore(store, "tenant")
	}
	unreadable := func(lsm string) {
		plantUnreadableRecord(t, filepath.Join(lsm, migrationsDir, migrationRecordsDirName))
	}
	for i := 0; i < maxReportedErrors+dropped; i++ {
		recoverShard(unreadable)
	}
	about := entriesAbout(hook, "some migration records could not be read")
	require.Len(t, about, maxReportedErrors, "%v", linesOf(about))

	recoverShard(func(lsm string) {
		subject := testMigrationSubject(1, StrategyCodeEnableSearchable, "title")
		subject.MigrationType, subject.TargetTokenization = ReindexTypeEnableSearchable, ""
		require.NoError(t, os.MkdirAll(lsm, 0o777))
		require.NoError(t, NewMigrationRecordStore(lsm, logger).Put(NewMigrationRecordMerged(subject)))
	})
	require.Len(t, entriesAbout(hook, "builds no reindex task"), 1, "another kind still reports")

	recovery.unreadableFaults.windowStart = time.Time{}
	hook.Reset()
	recoverShard(unreadable)
	about = entriesAbout(hook, "some migration records could not be read")
	require.Len(t, about, 1, "%v", linesOf(about))
	require.Equal(t, dropped, about[0].Data["dropped_lines"])
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

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

// Recovery faults are sampled across shard loads: a line per loading shard
// would follow the tenant count at every boot.
func TestRecoveryFaultsAreSampledAcrossShardLoads(t *testing.T) {
	const shards = 3 * maxReportedErrors
	fixtureLogger, _ := logrustest.NewNullLogger()

	for _, tt := range []struct {
		name  string
		seed  func(t *testing.T, i int, lsm string)
		about string
	}{
		{
			name: "a record recovery cannot build a task from",
			seed: func(t *testing.T, i int, lsm string) {
				subject := testMigrationSubject(uint64(i+1), StrategyCodeEnableSearchable, "title")
				subject.MigrationType, subject.TargetTokenization = ReindexTypeEnableSearchable, ""
				require.NoError(t, NewMigrationRecordStore(lsm, fixtureLogger).Put(NewMigrationRecordMerged(subject)))
			},
			about: "builds no reindex task",
		},
		{
			name: "one record that cannot be read",
			seed: func(t *testing.T, _ int, lsm string) {
				recordsDir := filepath.Join(lsm, migrationsDir, migrationRecordsDirName)
				require.NoError(t, os.MkdirAll(recordsDir, 0o777))
				require.NoError(t, os.WriteFile(filepath.Join(recordsDir, "searchable_retokenize_title_1.json"),
					[]byte("not json"), 0o600))
			},
			about: "could not be read",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			logger, hook := logrustest.NewNullLogger()
			recovery := NewReindexRecoveryHook(true, logger, nil)
			for i := 0; i < shards; i++ {
				lsm := filepath.Join(t.TempDir(), "lsm")
				require.NoError(t, os.MkdirAll(lsm, 0o777))
				tt.seed(t, i, lsm)
				store := NewMigrationRecordStore(lsm, logger)
				require.NoError(t, store.Load())
				require.Empty(t, recovery.recoverableTasks(store, fmt.Sprintf("tenant-%02d", i)))
			}
			about := entriesAbout(hook, tt.about)
			require.NotEmpty(t, about)
			require.LessOrEqual(t, len(about), maxReportedErrors, "sampled, not one per shard: %v", linesOf(about))
			require.Equal(t, logrus.WarnLevel, about[0].Level)
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

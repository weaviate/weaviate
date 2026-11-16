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
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/additional"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/searchparams"
)

const tokWord = models.PropertyTokenizationWord

func TestALoadFinishesAHalfWrittenSwapDecision(t *testing.T) {
	for decided := range 2 {
		t.Run(halfName(decided)+" half decided", func(t *testing.T) {
			f := newTokenizationChange(t)
			f.decide(t, f.tasks[decided])
			hook := f.captureLogs()

			f.reload(t)
			assert.Equal(t, tokWord, f.tokenization())
			assert.Equal(t, allFound, f.counts(t), "before any swap tick")
			assert.Equal(t, []MigrationState{MigrationStatePromoted, MigrationStatePromoted}, f.states(t))
			assert.Empty(t, errorLines(hook), "a load that finishes the unit reports nothing wrong")
		})
	}
}

// After a recorded flip decision every load serves each remaining index's new data, or the shard does not load.
func TestALoadServesEveryDecidedIndex(t *testing.T) {
	type load struct {
		before  func(t *testing.T, f *tokenizationChange, half int) // runs while the shard is down
		wantErr string
		found   int
	}
	tests := []struct {
		name     string
		finished []bool
		prepare  func(t *testing.T, f *tokenizationChange, half int)
		loads    []load
	}{
		{
			name: "an index is deleted after the task finished",
			prepare: func(t *testing.T, f *tokenizationChange, half int) {
				f.swapAndWrite(t)
				f.class.Properties[0].Tokenization = tokWord
				f.dropIndex(t, half)
			},
			loads: []load{{found: 4}, {found: 4}},
		},
		{
			name: "an index is removed while the shard is unloaded",
			prepare: func(t *testing.T, f *tokenizationChange, half int) {
				f.decideBoth(t)
				f.class.Properties[0].Tokenization = tokWord
				indexOff(f.class.Properties[0], half)
			},
			loads: []load{{found: 3}, {found: 3}},
		},
		{
			name: "a rename fails on every load", finished: []bool{false, true},
			prepare: func(t *testing.T, f *tokenizationChange, half int) {
				f.decideBoth(t)
				blockCanonicalClear(t, f, f.tasks[half])
			},
			loads: []load{{found: 3}, {found: 3}, {found: 3}},
		},
		{
			name: "a rename fails and the staged data opens only on the retry", finished: []bool{false, true},
			prepare: func(t *testing.T, f *tokenizationChange, half int) {
				f.decideBoth(t)
				blockCanonicalClear(t, f, f.tasks[half])
			},
			loads: []load{
				{before: chmodStaged(0), wantErr: "the shard does not load"},
				{before: chmodStaged(0o700), found: 3},
				{found: 3},
			},
		},
		{
			name: "a record turns unreadable while a rename is owed",
			prepare: func(t *testing.T, f *tokenizationChange, half int) {
				f.decideBoth(t)
				blockCanonicalClear(t, f, f.tasks[half])
			},
			loads: []load{
				{found: 3},
				{before: writeRecordFile("zz-junk.json", "{not json"), wantErr: "zz-junk.json"},
				{before: removeRecordFile("zz-junk.json"), found: 3},
			},
		},
		{
			name: "a half missed writes after its swap",
			prepare: func(t *testing.T, f *tokenizationChange, half int) {
				f.swapAndWrite(t)
				stampUnmirrored(t, f, f.tasks[half])
			},
			loads: []load{{found: 4}, {found: 4}},
		},
		{
			name: "a decided half missed writes",
			prepare: func(t *testing.T, f *tokenizationChange, half int) {
				f.decideBoth(t)
				stampUnmirrored(t, f, f.tasks[half])
			},
			loads: []load{{wantErr: "no double-write mirror"}, {wantErr: "no double-write mirror"}},
		},
		{
			name:    "a swapped half's staged data is deleted by hand",
			prepare: func(t *testing.T, f *tokenizationChange, _ int) { f.swapAndWrite(t) },
			loads:   []load{{before: removeStagedDir, wantErr: "is gone"}, {wantErr: "is gone"}},
		},
		{
			name:    "a decided half's staged data is deleted by hand",
			prepare: func(t *testing.T, f *tokenizationChange, _ int) { f.decideBoth(t) },
			loads:   []load{{before: removeStagedDir, wantErr: "is gone"}, {wantErr: "is gone"}},
		},
		{
			name: "a half-written decision whose other half lost its staged data",
			prepare: func(t *testing.T, f *tokenizationChange, half int) {
				f.decide(t, f.tasks[1-half])
			},
			loads: []load{{before: removeStagedDir, wantErr: "could not follow"}, {wantErr: "could not follow"}},
		},
		{
			name: "a half-written decision that cannot be recorded",
			prepare: func(t *testing.T, f *tokenizationChange, half int) {
				f.decide(t, f.tasks[1-half])
			},
			loads: []load{
				{before: denyRecordWrites, wantErr: "could not follow"},
				{before: allowRecordWrites, found: 3},
				{found: 3},
			},
		},
		{
			name: "a unit run holds the seal during the load",
			prepare: func(t *testing.T, f *tokenizationChange, _ int) {
				f.decideBoth(t)
				f.idx.db.SetReindexUnitSeal(func() ReindexUnitSeal {
					return func(distributedtask.TaskDescriptor, string) (func(), bool) { return nil, false }
				})
			},
			loads: []load{{found: 3}, {found: 3}},
		},
	}
	for _, tc := range tests {
		if tc.finished == nil {
			tc.finished = []bool{false}
		}
		for _, finished := range tc.finished {
			for half := range 2 {
				t.Run(fmt.Sprintf("%s/%s/task finished=%t", tc.name, halfName(half), finished), func(t *testing.T) {
					f := newTokenizationChange(t)
					if finished {
						f.class.Properties[0].Tokenization = tokWord
					}
					tc.prepare(t, f, half)
					for i, l := range tc.loads {
						err := f.load(t, func() {
							if l.before != nil {
								l.before(t, f, half)
							}
						})
						if l.wantErr != "" {
							require.ErrorContains(t, err, l.wantErr, "load %d", i+1)
							continue
						}
						require.NoError(t, err, "load %d", i+1)
						assert.Equal(t, tokWord, queryTokenization(f.shard, tokChangeProp,
							f.class.Properties[0].Tokenization), "load %d", i+1)
						f.assertFound(t, l.found, i+1)
					}
				})
			}
		}
	}
}

func halfName(half int) string {
	return []string{"searchable", "filterable"}[half]
}

func (f *tokenizationChange) decideBoth(t *testing.T) {
	f.decide(t, f.tasks[0])
	f.decide(t, f.tasks[1])
}

// swapAndWrite runs the live swap, then writes a 4th object that only the new data holds.
func (f *tokenizationChange) swapAndWrite(t *testing.T) {
	require.Empty(t, f.swap(f.tasks).Errs)
	require.NoError(t, f.shard.PutObject(f.ctx, createTestObjectWithText(f.class.Class, tokChangeValue)))
}

func indexOff(prop *models.Property, half int) {
	off := false
	if half == 0 {
		prop.IndexSearchable = &off
	} else {
		prop.IndexFilterable = &off
	}
}

func indexOn(flag *bool) bool { return flag == nil || *flag }

func (f *tokenizationChange) dropIndex(t *testing.T, half int) {
	indexOff(f.class.Properties[0], half)
	eg := enterrors.NewErrorGroupWrapper(f.idx.logger)
	var counts migrationSweepCounts
	f.shard.updatePropertyBuckets(f.ctx, eg, f.class.Properties[0], &counts)
	require.NoError(t, eg.Wait())
}

// load restarts the shard, running before while it is down.
func (f *tokenizationChange) load(t *testing.T, before func()) error {
	t.Helper()
	name, lsm := f.shard.Name(), f.shard.pathLSM()
	if !f.down {
		require.NoError(t, f.shard.Shutdown(f.ctx))
		f.down = true
	}
	before()
	simulateProcessRestartBucketCleanup(t, lsm)
	loaded, err := f.idx.initShard(f.ctx, name, f.class, nil, true, true)
	if err != nil {
		return err
	}
	f.idx.shards.Store(name, loaded)
	f.shard, f.down = loaded.(*Shard), false
	return nil
}

// assertFound checks every entry point the property's remaining indexes serve.
func (f *tokenizationChange) assertFound(t *testing.T, want, load int) {
	t.Helper()
	prop := f.class.Properties[0]
	if indexOn(prop.IndexSearchable) && indexOn(prop.IndexFilterable) {
		got := f.counts(t)
		for i := range got {
			assert.Equal(t, want, got[i], "load %d, entry point %d of %v", load, i, got)
		}
		return
	}
	var filter *filters.LocalFilter
	var keyword *searchparams.KeywordRanking
	if indexOn(prop.IndexFilterable) {
		filter = propEqualsFilter(f.class.Class, tokChangeProp, tokChangeValue)
	} else {
		keyword = &searchparams.KeywordRanking{Type: "bm25", Properties: []string{tokChangeProp}, Query: tokChangeValue}
	}
	found, _, err := f.shard.ObjectSearch(f.ctx, 10, filter, keyword, nil, nil,
		additional.Properties{}, []string{tokChangeProp})
	require.NoError(t, err)
	assert.Len(t, found, want, "load %d", load)
}

func chmodStaged(mode os.FileMode) func(*testing.T, *tokenizationChange, int) {
	return func(t *testing.T, f *tokenizationChange, half int) {
		staged := filepath.Join(f.shard.pathLSM(), f.tasks[half].ingestBucketName(tokChangeProp))
		require.NoError(t, os.Chmod(staged, mode))
		t.Cleanup(func() { os.Chmod(staged, 0o700) })
	}
}

func removeStagedDir(t *testing.T, f *tokenizationChange, half int) {
	require.NoError(t, os.RemoveAll(filepath.Join(f.shard.pathLSM(), f.tasks[half].ingestBucketName(tokChangeProp))))
}

func writeRecordFile(name, content string) func(*testing.T, *tokenizationChange, int) {
	return func(t *testing.T, f *tokenizationChange, _ int) {
		require.NoError(t, os.WriteFile(filepath.Join(f.shard.migrationRecords.Dir(), name), []byte(content), 0o600))
	}
}

func removeRecordFile(name string) func(*testing.T, *tokenizationChange, int) {
	return func(t *testing.T, f *tokenizationChange, _ int) {
		require.NoError(t, os.Remove(filepath.Join(f.shard.migrationRecords.Dir(), name)))
	}
}

func denyRecordWrites(t *testing.T, f *tokenizationChange, _ int) {
	denyDirectoryWrites(t, f.shard.migrationRecords.Dir())
}

func allowRecordWrites(t *testing.T, f *tokenizationChange, _ int) {
	require.NoError(t, os.Chmod(f.shard.migrationRecords.Dir(), 0o700))
}

func stampUnmirrored(t *testing.T, f *tokenizationChange, half *ShardReindexTaskGeneric) {
	rec, ok := half.migrationRecord(f.shard)
	require.True(t, ok)
	stamped, ok := migrationRecordStampedUnmirrored(rec)
	require.True(t, ok)
	require.NoError(t, f.shard.migrationRecords.Put(stamped))
}

// blockCanonicalClear leaves a subdirectory the load cannot empty in half's canonical directory.
func blockCanonicalClear(t *testing.T, f *tokenizationChange, half *ShardReindexTaskGeneric) {
	rec, ok := half.migrationRecord(f.shard)
	require.True(t, ok)
	blocker := filepath.Join(f.shard.pathLSM(), rec.Subject().Props[tokChangeProp].Canonical, "blocker")
	require.NoError(t, os.MkdirAll(blocker, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(blocker, "pinned"), nil, 0o600))
	denyDirectoryWrites(t, blocker)
}

func (f *tokenizationChange) captureLogs() *logrustest.Hook {
	logger, hook := logrustest.NewNullLogger()
	f.idx.logger = logger
	return hook
}

func errorLines(hook *logrustest.Hook) []string {
	var out []string
	for _, entry := range hook.AllEntries() {
		if entry.Level <= logrus.ErrorLevel {
			out = append(out, entry.Message)
		}
	}
	return out
}

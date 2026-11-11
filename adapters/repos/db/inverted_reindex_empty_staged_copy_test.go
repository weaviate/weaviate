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
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// A shard copy or backup carries files only, so a staged directory holding none
// does not reach the target; that must not read as lost data on the next load.
func TestShardCopyOfAnEmptyStagedBucketDoesNotWedgeTheMigration(t *testing.T) {
	const (
		propName  = "title"
		otherProp = "body"
		lateToken = "zulu"
	)
	tests := []struct {
		name            string
		withoutProperty int
		writeAfterSwap  bool
		wantLateHits    int
	}{
		{
			name: "an empty shard",
		},
		{
			name:            "objects that never set the property",
			withoutProperty: 5,
		},
		{
			name:           "a write lands in the staged bucket after the swap",
			writeAfterSwap: true,
			wantLateHits:   1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "EmptyStagedCopy_" + uuid.NewString()[:8]
			class := newEnableFilterableTestClass(className, propName, otherProp)
			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, true, false)
			shard := shd.(*Shard)
			for i := 0; i < tc.withoutProperty; i++ {
				require.NoError(t, shard.PutObject(ctx, objectWithOnly(className, otherProp, "unrelated")))
			}

			task, _ := newEnableFilterableTask(t, idx, className, shard.migrationUnit(), propName)
			require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
			require.NoError(t, task.RunPrepareOnShard(ctx, shard))
			require.NoError(t, task.RunSwapOnShard(ctx, shard))

			rec, ok := task.migrationRecord(shard)
			require.True(t, ok)
			swapped, ok := rec.(MigrationRecordSwapped)
			require.True(t, ok, "fixture: the swap leaves a swapped record")
			require.True(t, swapped.StagedWasEmpty(propName), "fixture: nothing reached the staged bucket before the swap")
			key := rec.Subject().Key

			// FINISHED: the task flips the schema cluster-wide after every shard swapped.
			class.Properties[0].IndexFilterable = boolPtr(true)
			require.NoError(t, idx.updateProperty(ctx, class.Properties[0]))

			if tc.writeAfterSwap {
				require.NoError(t, shard.PutObject(ctx, createTestObjectWithText(className, lateToken)))
			}

			require.NoError(t, shard.HaltForTransfer(ctx, false))
			files, err := shard.ListBackupFiles(ctx, &backup.ShardDescriptor{})
			require.NoError(t, err)
			require.NoError(t, shard.resumeMaintenanceCycles(ctx))
			name, lsm := shard.Name(), shard.pathLSM()
			require.NoError(t, shard.Shutdown(ctx))
			simulateProcessRestartBucketCleanup(t, lsm)
			keepOnlyListedFiles(t, idx.Config.RootPath, lsm, files)

			loaded := openShardFromDisk(t, ctx, idx, class, name)
			defer loaded.Shutdown(context.Background())

			assert.False(t, loaded.migrationRecords.Wedged(key), "the copied record must not be wedged")
			assert.NotContains(t, migrationRecordStates(t, lsm), MigrationStateSwapped,
				"the load must finish a migration whose copy carried every file it had")
			assert.Equal(t, tc.wantLateHits, countFilterHits(t, ctx, loaded, className, propName, lateToken),
				"a write that reached the staged bucket after the swap must survive the copy")

			require.NoError(t, loaded.PutObject(ctx, createTestObjectWithText(className, "yankee")))
			assert.Equal(t, 1, countFilterHits(t, ctx, loaded, className, propName, "yankee"),
				"the promoted bucket must take new writes")
		})
	}
}

func objectWithOnly(className, prop, value string) *storobj.Object {
	return &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:         strfmt.UUID(uuid.NewString()),
			Class:      className,
			Properties: map[string]interface{}{prop: value},
		},
	}
}

// Leaves the shard's LSM tree as a copy lands it: listed files only, so no empty directory.
func keepOnlyListedFiles(t *testing.T, root, lsm string, files []string) {
	t.Helper()
	var dirs []string
	require.NoError(t, filepath.WalkDir(lsm, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			dirs = append(dirs, path)
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if slices.Contains(files, rel) {
			return nil
		}
		return os.Remove(path)
	}))
	slices.Reverse(dirs)
	for _, dir := range dirs[:len(dirs)-1] {
		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		if len(entries) == 0 {
			require.NoError(t, os.Remove(dir))
		}
	}
}

func countFilterHits(t *testing.T, ctx context.Context, s *Shard, className, prop, token string) int {
	t.Helper()
	res, _, err := s.ObjectSearch(ctx, 100, &filters.LocalFilter{Root: &filters.Clause{
		Operator: filters.OperatorEqual,
		On:       &filters.Path{Class: schema.ClassName(className), Property: schema.PropertyName(prop)},
		Value:    &filters.Value{Value: token, Type: schema.DataTypeText},
	}}, nil, nil, nil, additional.Properties{}, nil)
	require.NoError(t, err)
	return len(res)
}

// A flip committed at load can wait on the schema with its staged directory
// served live, so a copy in that wait drops an empty one the same way.
func TestLoadTimeFlipTellsAnEmptyStagedDirectoryFromALostOne(t *testing.T) {
	off, on := false, true
	tests := []struct {
		name          string
		stagedHasData bool
		wantPromoted  bool
	}{
		{name: "the staged directory held nothing", wantPromoted: true},
		{name: "the staged directory held data", stagedHasData: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newReconcileFixture(t)
			f.class = promotionGateClass(fieldFilterable, &off)
			subject := testMigrationSubject(42, StrategyCodeEnableFilterable, promotionGateProp)
			staged := subject.Props[promotionGateProp].Staged
			if tc.stagedHasData {
				f.mkdirs(staged)
			} else {
				require.NoError(t, os.MkdirAll(filepath.Join(f.lsmPath, staged), 0o777))
			}
			f.tasks = []*distributedtask.Task{testTask(subject.TaskID, 42, distributedtask.TaskStatusFinished)}
			f.put(NewMigrationRecordMerged(subject))

			f.reconcile()
			require.Equal(t, !tc.stagedHasData, f.swapped(t, subject.Key).StagedWasEmpty(promotionGateProp))

			require.NoError(t, os.RemoveAll(filepath.Join(f.lsmPath, staged)))
			f.class = promotionGateClass(fieldFilterable, &on)
			r := f.reconcile()

			state, present := f.state(subject.Key)
			require.True(t, present)
			if tc.wantPromoted {
				assert.Zero(t, r.WedgedCount())
				assert.Equal(t, MigrationStatePromoted, state)
				assert.True(t, f.exists(subject.Props[promotionGateProp].Canonical))
				return
			}
			assert.Equal(t, 1, r.WedgedCount(), "a staged directory lost with data in it is data loss")
			assert.Equal(t, MigrationStateSwapped, state)
		})
	}
}

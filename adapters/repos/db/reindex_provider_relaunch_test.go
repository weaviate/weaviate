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
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// A relaunch (handle done, unit still non-terminal) must not reopen the staged directory as a second bucket.
func TestRelaunchedUnitAfterSwapOpensNoSecondBucket(t *testing.T) {
	for _, tc := range []struct {
		name          string
		migrationType ReindexMigrationType
		prop          string
		newClass      func(className string) *models.Class
		newObjects    func(t *testing.T, className string) []*storobj.Object
		// Only text bucket keys hold the raw token a segment check looks for.
		text bool
	}{
		{
			name: "repair filterable", migrationType: ReindexTypeRepairFilterable, prop: "title",
			newClass: func(className string) *models.Class {
				return newTestClassWithProps(className, []string{"title"})
			},
			newObjects: func(t *testing.T, className string) []*storobj.Object {
				return makeConvergenceTestObjects(t, 25, className)
			},
			text: true,
		},
		{
			name: "rebuild searchable", migrationType: ReindexTypeRebuildSearchable, prop: "title",
			newClass: func(className string) *models.Class {
				return newRebuildSearchableTestClass(className, []string{"title"})
			},
			newObjects: func(t *testing.T, className string) []*storobj.Object {
				return makeConvergenceTestObjects(t, 25, className)
			},
			text: true,
		},
		{
			name: "repair rangeable", migrationType: ReindexTypeRepairRangeable, prop: filterableToRangeablePropName,
			newClass: func(className string) *models.Class {
				class := newFilterableToRangeableTestClass(className)
				class.Properties[0].IndexRangeFilters = boolPtr(true)
				return class
			},
			newObjects: func(t *testing.T, className string) []*storobj.Object {
				return makeFilterableToRangeableTestObjects(t, 25, className)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "Relaunch_" + uuid.NewString()[:8]
			class := tc.newClass(className)
			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, true, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(context.Background())
			registerIndex(idx, className)
			for _, obj := range tc.newObjects(t, className) {
				require.NoError(t, shard.PutObject(ctx, obj))
			}

			logger, _ := logrustest.NewNullLogger()
			p := NewReindexProvider(idx.db, nil, nil, logger, "node1", func() int { return 1 }, ctx)
			unitID := shard.migrationUnit()
			payload := &ReindexTaskPayload{
				Collection: className, MigrationType: tc.migrationType, Properties: []string{tc.prop},
				UnitToShard: map[string]string{unitID: shard.Name()},
				UnitToNode:  map[string]string{unitID: "node1"},
			}
			task := &distributedtask.Task{
				Namespace:      ReindexNamespace,
				TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_relaunch", Version: 1},
				Status:         distributedtask.TaskStatusStarted,
			}
			built, err := p.createReindexTasks(task.TaskDescriptor, unitID, payload)
			require.NoError(t, err)
			require.Len(t, built, 1)
			ingestName := built[0].ingestBucketName(tc.prop)
			canonical := built[0].strategy.SourceBucketName(tc.prop)

			put := func(token string) {
				require.NoError(t, shard.PutObject(ctx, &storobj.Object{
					MarshallerVersion: 1,
					Object: models.Object{
						ID: strfmt.UUID(uuid.NewString()), Class: className,
						Properties: map[string]any{tc.prop: token},
					},
				}))
			}
			for run := 1; run <= 2; run++ {
				rec := newFakeRecorder()
				p.processOneUnit(ctx, task, payload, idx, unitID, rec, &keyDocColumnDrops{})
				require.Emptyf(t, rec.failed, "run %d", run)
				require.Equalf(t, []string{unitID}, rec.completed, "run %d", run)
				assert.Truef(t, shard.store.Bucket(ingestName) == nil,
					"run %d: the staged directory is open under its own name next to the canonical bucket serving it", run)
				if run == 1 && tc.text {
					put("xmarker")
				}
			}

			live := shard.store.Bucket(canonical)
			require.NotNil(t, live)
			require.Equal(t, ingestName, filepath.Base(live.GetDir()))
			if !tc.text {
				return
			}
			put("ymarker")
			// The order the dirty timers produce: the live memtable went dirty first.
			require.NoError(t, live.FlushAndSwitch())
			if second := shard.store.Bucket(ingestName); second != nil {
				require.NoError(t, second.FlushAndSwitch())
			}
			assert.True(t, dirHoldsBytes(t, live.GetDir(), []byte("ymarker")),
				"a flushed, acknowledged write must stay in a segment of the live directory")
		})
	}
}

func dirHoldsBytes(t *testing.T, dir string, needle []byte) bool {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range entries {
		if !e.Type().IsRegular() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		require.NoError(t, err)
		if bytes.Contains(data, needle) {
			return true
		}
	}
	return false
}

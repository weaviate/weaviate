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
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// A change-tokenization unit runs two halves in sequence and writes a tracker
// directory for each before either starts. Recovery rebuilds a task only where
// the tracker also carries a migration record, so a restart between the two
// halves recovers one task. The consumer reads any non-empty set as the whole
// unit, runs the searchable half alone and reports the unit finished, and the
// schema flips for a filterable index nothing rebuilt.
func TestRecoveryReportsTheHalvesItCouldNotSeed(t *testing.T) {
	const (
		taskID   = "Docs:change-tokenization:title:ab12"
		unitID   = "shard-1__node-0"
		propName = "title"

		searchableTracker = "searchable_retokenize_title_1"
		filterableTracker = "filterable_retokenize_title_1"
	)

	payload := `{"taskID":"` + taskID + `","taskVersion":42,"unitID":"` + unitID + `",` +
		`"payload":{"migrationType":"change-tokenization","collection":"Docs",` +
		`"properties":["` + propName + `"],"targetTokenization":"field",` +
		`"bucketStrategy":"inverted"}}`

	tests := []struct {
		name           string
		recordedHalves []string
		wantRecovered  int
		wantMissing    []string
	}{
		{
			name:           "both halves recorded",
			recordedHalves: []string{searchableTracker, filterableTracker},
			wantRecovered:  2,
		},
		{
			name:           "the restart landed between the halves",
			recordedHalves: []string{searchableTracker},
			wantRecovered:  1,
			wantMissing:    []string{"filterable_retokenize_title"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			lsm := filepath.Join(root, "docs_abc", "shard-1", "lsm")
			logger, _ := logrustest.NewNullLogger()

			for _, tracker := range []string{searchableTracker, filterableTracker} {
				dir := filepath.Join(lsm, migrationsDir, tracker)
				require.NoError(t, os.MkdirAll(dir, 0o777))
				require.NoError(t, os.WriteFile(
					filepath.Join(dir, reindexRecoveryPayloadFile), []byte(payload), 0o600))
			}

			store := NewMigrationRecordStore(lsm, logger)
			require.NoError(t, store.Load())
			for _, tracker := range tt.recordedHalves {
				code := StrategyCodeSearchableRetokenize
				if tracker == filterableTracker {
					code = StrategyCodeFilterableRetokenize
				}
				subject := testMigrationSubject(42, code, propName)
				subject.TaskID = taskID
				subject.TrackerDir = tracker
				subject.MigrationType = ReindexTypeChangeTokenization
				subject.TargetTokenization = models.PropertyTokenizationField
				// Each half writes into its own directories, the way the two
				// production tasks do; the loader refuses two records that
				// claim one directory.
				subject.Props[propName] = MigrationPropertyDirs{
					Staged:    "property_" + propName + "__" + tracker + "_ingest",
					Canonical: subject.Props[propName].Canonical,
					Sidecar:   "property_" + propName + "__" + tracker + "_reindex",
				}
				require.NoError(t, store.Put(NewMigrationRecordIterated(subject)))
			}

			recovered, err := DiscoverInFlightReindexTasks(root, logger, nil)
			require.NoError(t, err)

			var tasks []*ShardReindexTaskGeneric
			for _, rr := range recovered {
				tasks = append(tasks, rr.Tasks...)
			}
			require.Len(t, tasks, tt.wantRecovered)

			desc := distributedtask.TaskDescriptor{ID: taskID, Version: 42}
			require.Equal(t, tt.wantMissing,
				migrationHalvesMissingFromCache(lsm, desc, unitID, tasks))
		})
	}
}

// A tracker another unit wrote is not one this unit owes.
func TestAnotherUnitsTrackerIsNotAMissingHalf(t *testing.T) {
	root := t.TempDir()
	lsm := filepath.Join(root, "lsm")
	dir := filepath.Join(lsm, migrationsDir, "filterable_retokenize_title_1")
	require.NoError(t, os.MkdirAll(dir, 0o777))
	require.NoError(t, os.WriteFile(filepath.Join(dir, reindexRecoveryPayloadFile),
		[]byte(`{"taskID":"t","taskVersion":42,"unitID":"shard-2__node-0",`+
			`"payload":{"migrationType":"change-tokenization","collection":"Docs",`+
			`"properties":["title"],"targetTokenization":"field","bucketStrategy":"inverted"}}`),
		0o600))

	require.Empty(t, migrationHalvesMissingFromCache(lsm,
		distributedtask.TaskDescriptor{ID: "t", Version: 42}, "shard-1__node-0", nil))
}

// Running only the seeded half reports a finished unit and the schema flips
// for both halves, so the never-started half is rebuilt from its payload and
// the unit runs whole.
func TestAPartiallySeededUnitRebuildsItsNeverStartedHalf(t *testing.T) {
	ctx := testCtx()
	className := "PartialSeed" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{"title"})
	hot, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	defer hot.Shutdown(context.Background())
	registerIndex(idx, className)

	logger, _ := logrustest.NewNullLogger()
	p := NewReindexProvider(idx.db, nil, nil, logger, "node1", nil, ctx)

	payload := &ReindexTaskPayload{
		Collection: className, MigrationType: ReindexTypeChangeTokenization,
		Properties: []string{"title"}, TargetTokenization: "field", BucketStrategy: lsmkv.StrategyMapCollection,
		UnitToShard: map[string]string{"u1": hot.Name()},
		UnitToNode:  map[string]string{"u1": "node1"},
	}
	desc := distributedtask.TaskDescriptor{ID: "T_partial", Version: 1}

	concrete, err := unwrapShard(ctx, hot)
	require.NoError(t, err)
	tasks, err := p.createReindexTasks(desc, "u1", payload, concrete.pathLSM(), false)
	require.NoError(t, err)
	require.Len(t, tasks, 2, "change-tokenization on a property with both indexes runs two halves")

	// persistRecoveryRecord writes one of these per generated task before
	// either half starts, which is what makes both halves visible on disk.
	for _, task := range tasks {
		dir := task.migrationPath(concrete.pathLSM())
		require.NoError(t, os.MkdirAll(dir, 0o777))
		require.NoError(t, os.WriteFile(filepath.Join(dir, reindexRecoveryPayloadFile),
			[]byte(`{"taskID":"T_partial","taskVersion":1,"unitID":"u1","payload":`+
				`{"migrationType":"change-tokenization","collection":"`+className+`",`+
				`"properties":["title"],"targetTokenization":"field","bucketStrategy":"`+lsmkv.StrategyMapCollection+`"}}`),
			0o600))
	}

	// Only the searchable half carried a record when the node came back.
	p.SeedReindexTaskCache(map[distributedtask.TaskDescriptor]map[string][]*ShardReindexTaskGeneric{
		desc: {"u1": tasks[:1]},
	})

	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	rec := newFakeRecorder()
	p.processOneUnit(ctx, &distributedtask.Task{
		Namespace: ReindexNamespace, TaskDescriptor: desc,
		Status: distributedtask.TaskStatusStarted, Payload: raw,
	}, payload, idx, "u1", rec)

	require.Empty(t, rec.failed,
		"a never-started half rebuilds from its payload instead of failing the unit")
	require.Contains(t, rec.completed, "u1")
	require.Len(t, p.cachedReindexTasks(desc, "u1"), 2,
		"the cache holds both halves, so the swap phase covers them both")
}

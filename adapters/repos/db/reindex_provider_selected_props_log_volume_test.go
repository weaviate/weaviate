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
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// The selected-props write is best-effort: persistRecoveryRecord logs and
// carries on so one unwritable shard does not fail the task. The cause is
// usually the disk, which fails the write on every unit alike, and
// persistRecoveryRecord runs once per unit — one shard per tenant on a
// multi-tenant collection. Warning per unit therefore puts one line per
// tenant in the log. Count the failures and warn once for the task.
func TestPersistRecoveryRecordDoesNotWarnPerUnit(t *testing.T) {
	ctx := testCtx()
	className := "PropsWarnVolume" + uuid.NewString()[:8]
	props := []string{"cat", "dog"}

	shd, _ := testShardWithSettings(t, ctx,
		newTestClassWithProps(className, props),
		enthnsw.UserConfig{Skip: true}, false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)
	lsm := shard.pathLSM()

	p, hook := newTestProvider(t)
	payload := &ReindexTaskPayload{
		MigrationType: ReindexTypeEnableFilterable,
		Collection:    className,
		Properties:    props,
		UnitToShard:   map[string]string{"unit-1": shard.Name()},
	}
	tasks, err := p.createReindexTasks(payload, lsm, false)
	require.NoError(t, err)
	require.NotEmpty(t, tasks)

	dtmTask := &distributedtask.Task{
		Namespace:      ReindexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "task-1", Version: 1},
	}

	failures := &selectedPropsFailures{}

	// The first call creates the migration dirs and writes the sidecars.
	require.NoError(t, p.persistRecoveryRecord(dtmTask, payload, "unit-1", shard, tasks, failures))

	// Take the sidecar away and make its directory unwritable, so every later
	// call fails the write the way a full or read-only disk does. The recovery
	// payload beside it is already on disk with identical content, so that
	// half still succeeds and the failure is isolated to the props write.
	for _, task := range tasks {
		migDir := filepath.Join(lsm, ".migrations", task.MigrationDirName())
		require.FileExists(t, filepath.Join(migDir, "properties.mig"),
			"first call must write the sidecar, or the arm below proves nothing")
		require.NoError(t, os.Remove(filepath.Join(migDir, "properties.mig")))
		require.NoError(t, os.Chmod(migDir, 0o555))
		t.Cleanup(func() { _ = os.Chmod(migDir, 0o777) })
	}

	hook.Reset()

	// One call per unit, as processUnits makes them.
	const units = 50
	for range units {
		require.NoError(t, p.persistRecoveryRecord(dtmTask, payload, "unit-1", shard, tasks, failures))
	}

	var perUnitWarnings int
	for _, e := range hook.AllEntries() {
		if strings.Contains(e.Message, "failed to record task properties") {
			perUnitWarnings++
		}
	}
	require.Zero(t, perUnitWarnings,
		"a failure that repeats on every unit must not log once per unit")

	require.EqualValues(t, units*len(tasks), failures.count(),
		"every failed write must still be counted")

	// The aggregate the task reports once, after its units have joined.
	failures.report(p.logger, dtmTask.ID)

	var aggregate int
	for _, e := range hook.AllEntries() {
		if strings.Contains(e.Message, "failed to record task properties") {
			aggregate++
		}
	}
	require.Equal(t, 1, aggregate, "exactly one line for the whole task")
}

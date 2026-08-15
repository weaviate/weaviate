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
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/weaviate/weaviate/cluster/distributedtask"
)

// One fixture, both halves of the feature: the task a submission leaves behind
// when it loses the race to a backup and cancels itself. It gave way so the
// capture could finish, so it must not then take that capture down with it.
// The second row is the other half — the waiver is unit state, not amnesty.
func TestSubmitRollbackResidue(t *testing.T) {
	captureStarted := time.Date(2026, 8, 15, 10, 0, 0, 0, time.UTC)
	cancelledAt := captureStarted.Add(time.Minute)

	tests := []struct {
		name                string
		units               []distributedtask.UnitStatus
		wantBackstopRefuses bool
	}{
		{
			name:  "the rollback landed before any worker claimed a unit",
			units: []distributedtask.UnitStatus{distributedtask.UnitStatusPending, distributedtask.UnitStatusPending},
		},
		{
			name:                "a worker had claimed a unit before the cancel landed",
			units:               []distributedtask.UnitStatus{distributedtask.UnitStatusPending, distributedtask.UnitStatusInProgress},
			wantBackstopRefuses: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := reindexTask("Movies:change-tokenization:title:ab3f",
				distributedtask.TaskStatusCancelled, payloadFor("Movies"))
			task.FinishedAt, task.Units = cancelledAt, units(tt.units...)
			tasks := []*distributedtask.Task{task}

			// Gate non-closure, on both gates a capture in flight can meet.
			assert.False(t, NewShardReindexActivityLookup(tasks, logrus.New())("Movies", "s1"),
				"the per-shard gate must stay open over a task the rollback already cancelled")
			_, restoreBlocked := NewAnyReindexActivityLookup(tasks)([]string{"Movies"})
			assert.False(t, restoreBlocked,
				"the cluster-wide gate must stay open over a task the rollback already cancelled")

			// The commit-time check is the only one left with an opinion.
			verdict := NewReindexOverlapLookup(tasks, 24*time.Hour, noLocalWorker,
				func() time.Time { return cancelledAt })([]string{"Movies"}, captureStarted)
			assert.Equal(t, tt.wantBackstopRefuses, !verdict.allowsBackup())
		})
	}
}

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

package rest

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
)

// TestLogPausedReindexes pins the operator signal for a node that boots
// with runtime reindex off while migration state is still on disk. Without
// it the operator sees a frozen task, a refused submit and a silent
// startup, with nothing connecting the three.
func TestLogPausedReindexes(t *testing.T) {
	t.Run("names every paused migration and both exits", func(t *testing.T) {
		logger, hook := test.NewNullLogger()

		logPausedReindexes(logger, []db.RecoveredReindex{
			{
				Descriptor: distributedtask.TaskDescriptor{ID: "task-a", Version: 3},
				Collection: "Books",
				ShardName:  "shard1",
			},
			{
				Descriptor: distributedtask.TaskDescriptor{ID: "task-b", Version: 4},
				Collection: "Movies",
				ShardName:  "shard2",
			},
		})

		require.Len(t, hook.Entries, 2, "one line per affected shard")
		for _, e := range hook.Entries {
			require.Equal(t, logrus.WarnLevel, e.Level,
				"a paused migration is a WARN: it is a state the operator has to resolve")
			require.Contains(t, e.Message, "will NOT resume")
			require.Contains(t, e.Message, "untouched")
			require.Contains(t, e.Message, "RUNTIME_REINDEX_ENABLED=true", "must name the resume exit")
			require.Contains(t, e.Message, "cancel", "must name the cancel exit")
		}

		first := hook.Entries[0]
		require.Equal(t, "Books", first.Data["collection"])
		require.Equal(t, "shard1", first.Data["shard"])
		require.Equal(t, "task-a", first.Data["task_id"])
	})

	t.Run("stays quiet when nothing is paused", func(t *testing.T) {
		logger, hook := test.NewNullLogger()
		logPausedReindexes(logger, nil)
		require.Empty(t, hook.Entries,
			"a node with no migration state must not emit a scary line")
	})
}

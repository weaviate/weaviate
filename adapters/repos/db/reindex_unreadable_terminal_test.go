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
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
)

// A terminal task whose payload cannot be read never tears its sidecars down:
// the teardown is addressed by the shards the payload names. Nothing else says
// so — the cleanup gate is keyed on the same payload, so none closes either —
// and the only remaining sweep is the next restart's orphan audit. The
// commit-time backstop keeps refusing meanwhile, which is what makes the line
// actionable rather than noise.
func TestTerminalTaskWithAnUnreadablePayloadReportsThatNothingToreItDown(t *testing.T) {
	tests := []struct {
		name    string
		status  distributedtask.TaskStatus
		payload []byte
		wantLog bool
	}{
		{
			name:    "failed, nothing decodes",
			status:  distributedtask.TaskStatusFailed,
			payload: []byte("{not json"),
			wantLog: true,
		},
		{
			name:    "cancelled, nothing decodes",
			status:  distributedtask.TaskStatusCancelled,
			payload: []byte("{not json"),
			wantLog: true,
		},
		{
			// Decodes without error, names no collection: the shape a newer
			// node renaming the field produces. It tears down exactly as
			// little as the one that will not decode at all.
			name:    "cancelled, the collection field was renamed",
			status:  distributedtask.TaskStatusCancelled,
			payload: []byte(`{"collektion":"Movies","unitToShard":{"u1":"shard1"}}`),
			wantLog: true,
		},
		{
			name:    "cancelled, readable",
			status:  distributedtask.TaskStatusCancelled,
			payload: []byte(`{"collection":"Movies","migrationType":"repair-filterable","unitToShard":{"u1":"shard1"}}`),
			wantLog: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logger, hook := logrustest.NewNullLogger()
			logger.SetLevel(logrus.DebugLevel)
			provider := NewReindexProvider(nil, nil, logger, "node1",
				func() int { return 1 }, context.Background())

			require.NoError(t, provider.OnTaskCompleted(&distributedtask.Task{
				TaskDescriptor: distributedtask.TaskDescriptor{ID: "t1", Version: 1},
				Namespace:      ReindexNamespace,
				Status:         test.status,
				Payload:        test.payload,
			}))

			var found bool
			for _, entry := range hook.AllEntries() {
				if strings.Contains(entry.Message, "no sidecar teardown can run for it") {
					found = true
					require.Equal(t, logrus.WarnLevel, entry.Level)
				}
			}
			require.Equal(t, test.wantLog, found,
				"an operator only learns about un-torn-down state from this line")
		})
	}
}

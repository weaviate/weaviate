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
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
)

// TestBackupRequestsTotal_LogErrorIsBounded pins the second place a
// refusal reaches the log: a participant refusal comes back as a server
// error, and this logger printed the whole body — 7 MB on a node with
// 20,000 shards.
func TestBackupRequestsTotal_LogErrorIsBounded(t *testing.T) {
	lines := make([]string, 20000)
	for i := range lines {
		lines[i] = fmt.Sprintf("node1/Cls: %v: shard %q (collection \"Cls\") has an active runtime-reindex task in DTM",
			backup.ErrBackupBlockedByInFlightReindex, fmt.Sprintf("shard-%d", i))
	}

	tests := []struct {
		name string
		err  error
	}{
		{name: "genuine reindex blocking every shard", err: errors.New(strings.Join(lines, "\n"))},
		{
			name: "cluster leader unreachable",
			err: fmt.Errorf("%w: the cluster leader could not be reached, so runtime-reindex state is unknown",
				backup.ErrBackupBlockedByInFlightReindex),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, hook := test.NewNullLogger()
			e := newBackupRequestsTotal(nil, logger)

			e.logError("Cls", tt.err)

			for _, entry := range hook.AllEntries() {
				logged := entry.Message
				if loggedErr, ok := entry.Data["error"]; ok {
					logged += fmt.Sprint(loggedErr)
				}
				require.LessOrEqual(t, len(logged), 8<<10,
					"log line must not grow with the number of refused shards")
			}
		})
	}
}

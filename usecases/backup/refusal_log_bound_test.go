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

package backup

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
)

// logBoundBytes is the largest log line a refusal may produce. Well
// above what five shard refusals need, well below what a per-shard
// listing produces on a node with thousands of them.
const logBoundBytes = 8 << 10

func massRefusal(n int) []string {
	lines := make([]string, n)
	for i := range lines {
		lines[i] = fmt.Sprintf(
			"node1/Cls: %v: shard %q (collection \"Cls\") has an active runtime-reindex task in DTM; "+
				"retry after the migration finishes (poll GET /v1/schema/<class>/indexes until all indexes "+
				"report status=\"ready\") or cancel it via PUT /v1/schema/<class>/indexes/<prop>",
			backup.ErrBackupBlockedByInFlightReindex, fmt.Sprintf("shard-%d", i))
	}
	return lines
}

// TestBackupRefusalLogIsBounded pins that a refused backup writes a
// bounded log line (not the 7 MB a 20,000-shard refusal used to produce),
// while the caller still receives every line in the response.
func TestBackupRefusalLogIsBounded(t *testing.T) {
	tests := []struct {
		name string
		// refusal is what the storage layer hands back to the scheduler.
		refusal error
		// mustSurviveInResponse is text the caller must still get.
		mustSurviveInResponse string
	}{
		{
			name: "genuine reindex blocking every shard",
			// Assembled in-process by DB.Backupable: one joined error
			// per blocked shard.
			refusal:               errors.New(strings.Join(massRefusal(20000), "\n")),
			mustSurviveInResponse: "shard-19999",
		},
		{
			name: "refusal flattened by the participant RPC",
			// The canCommit path stringifies the participant's joined
			// error before the coordinator ever sees it, so the bound
			// cannot rely on unwrapping a joined error.
			refusal: fmt.Errorf("node %q: %w: %s", "node2",
				backup.ErrBackupBlockedByInFlightReindex,
				strings.Join(massRefusal(20000), "\n")),
			mustSurviveInResponse: "shard-19999",
		},
		{
			name: "cluster leader unreachable",
			refusal: fmt.Errorf("%w: the cluster leader could not be reached, so runtime-reindex "+
				"state is unknown for every shard on this node", backup.ErrBackupBlockedByInFlightReindex),
			mustSurviveInResponse: "cluster leader could not be reached",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			const cls = "Cls"

			logger, hook := test.NewNullLogger()
			fs := newFakeScheduler(nil)
			fs.log = logger
			fs.selector.On("ListClasses", ctx).Return([]string{cls})
			fs.selector.On("Backupable", ctx, []string{cls}).Return(tt.refusal)

			_, err := fs.scheduler().Backup(ctx, nil, &BackupRequest{
				Backend: "s3",
				ID:      "1234",
				Include: []string{cls},
			})

			require.Error(t, err)
			require.Contains(t, err.Error(), tt.mustSurviveInResponse,
				"the response goes to a caller who asked for the list; it stays complete")

			entries := hook.AllEntries()
			require.NotEmpty(t, entries, "a refused backup must be logged at all")
			for _, entry := range entries {
				require.LessOrEqual(t, len(entry.Message), logBoundBytes,
					"log line must not grow with the number of refused shards")
			}
		})
	}
}

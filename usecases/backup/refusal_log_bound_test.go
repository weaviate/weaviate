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
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
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
// bounded log line however many lines the refusal carries, while the
// caller still receives every one of them in the response.
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

// TestCoordinatorFailureLogIsBounded pins the bound on the line the
// coordinator writes when a participant fails: the participant's refusal
// reaches it already flattened to a string, so nothing downstream can shrink
// it again.
func TestCoordinatorFailureLogIsBounded(t *testing.T) {
	var (
		ctx          = context.Background()
		any          = mock.Anything
		backendName  = "s3"
		backupID     = "1"
		nodes        = []string{"N1", "N2"}
		classes      = []string{"Class-A"}
		nodeResolver = newFakeNodeResolver(nodes)
		cresp        = &CanCommitResponse{Method: OpCreate, ID: backupID, Timeout: 1}
		sReq         = &StatusRequest{OpCreate, backupID, backendName, "", "", ""}
		sresp        = &StatusResponse{Status: backup.Success, ID: backupID, Method: OpCreate}
		abortReq     = &AbortRequest{OpCreate, backupID, backendName, "", "", ""}
	)

	logger, hook := test.NewNullLogger()
	fc := newFakeCoordinator(nodeResolver)
	fc.log = logger
	coordinator := *fc.coordinator()
	coordinator.timeoutNodeDown = 0

	wide := errors.New(strings.Join(massRefusal(20000), "\n"))
	fc.selector.On("Shards", ctx, classes[0]).Return(nodes, nil)
	fc.client.On("CanCommit", any, nodes[0], any).Return(cresp, nil)
	fc.client.On("CanCommit", any, nodes[1], any).Return(cresp, nil)
	fc.client.On("Commit", any, nodes[0], sReq).Return(wide)
	fc.client.On("Commit", any, nodes[1], sReq).Return(nil)
	fc.client.On("Status", any, nodes[1], sReq).Return(sresp, nil)
	fc.client.On("Abort", any, nodes[0], abortReq).Return(nil)
	fc.client.On("Abort", any, nodes[1], abortReq).Return(nil)
	fc.backend.On("HomeDir", any, any, backupID).Return("bucket/" + backupID)
	fc.backend.On("PutObject", any, backupID, GlobalBackupFile, any).Return(nil).Twice()

	req := newReq(classes, backendName, backupID)
	store := coordStore{objectStore: objectStore{fc.backend, req.ID, "", "", ""}}
	require.NoError(t, coordinator.Backup(ctx, store, &req))
	<-fc.backend.doneChan

	require.Contains(t, fc.backend.glMeta.Error, "shard-19999",
		"the stored descriptor keeps the whole refusal for the status API")

	// Two sites log this failure: the per-participant line inside commitAll and
	// the summary the backup goroutine writes once it is done. The second lands
	// after the meta write the wait above observes, so both are polled for.
	require.Eventually(t, func() bool {
		var sawParticipant, sawSummary bool
		for _, entry := range hook.AllEntries() {
			if strings.Contains(entry.Message, "shard-0") {
				sawParticipant = true
			}
			if strings.Contains(entry.Message, "coordinator: ") {
				sawSummary = true
			}
		}
		return sawParticipant && sawSummary
	}, 5*time.Second, 10*time.Millisecond, "both failure lines have to be logged at all")

	for _, entry := range hook.AllEntries() {
		require.LessOrEqual(t, len(entry.Message), logBoundBytes,
			"log line must not grow with the size of the participant's refusal")
	}
}

// TestParticipantFailureLogIsBounded pins the bound on the line a participant
// writes when its own capture pass fails. The caller reads the full failure
// from the status endpoint; the log keeps a summary.
func TestParticipantFailureLogIsBounded(t *testing.T) {
	const cls = "Cls"
	wide := errors.New(strings.Join(massRefusal(20000), "\n"))

	_, _, errMsg, hook := runParticipantBackupWithMetaWriteErr(t, &fakeSourcer{}, newFakeBackend(),
		[]string{cls}, t.TempDir(), nil, backup.ClassDescriptor{Name: cls, Error: wide})

	require.Contains(t, errMsg, "shard-19999",
		"the stored failure meta keeps the whole refusal for the status API")
	var sawFailure bool
	for _, entry := range hook.AllEntries() {
		require.LessOrEqual(t, len(entry.Message), logBoundBytes,
			"log line must not grow with the size of the refusal")
		if strings.Contains(entry.Message, "shard-0") {
			sawFailure = true
		}
	}
	require.True(t, sawFailure, "the failure has to be logged at all")
}
